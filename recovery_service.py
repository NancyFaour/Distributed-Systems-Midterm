"""Background missed-heartbeat repair and periodic full-fragment integrity scrubbing."""

import asyncio
from contextlib import asynccontextmanager
import logging
import os
import threading
import time

from fastapi import FastAPI

from metadata_db import connect_db
from redundant_store import RedundantStore

LOGGER = logging.getLogger("uvicorn.error")
logging.getLogger("pika").setLevel(logging.WARNING)
STOP = threading.Event()
PAUSED = threading.Event()
CYCLE_LOCK = threading.Lock()
STATE = {"last_cycle": None, "last_error": None, "last_events": []}
STORE = RedundantStore()


def run_cycle(scrub):
    with CYCLE_LOCK, connect_db() as lease:
        # Only one recovery coordinator scans at a time, including manual runs.
        acquired = lease.execute(
            "SELECT pg_try_advisory_lock(734983015) AS ok"
        ).fetchone()["ok"]
        if not acquired:
            return []
        try:
            events = STORE.maintain(scrub=scrub)
            STATE.update(last_cycle=time.time(), last_error=None, last_events=events)
            for event in events:
                LOGGER.info("Recovery event: %s", event)
            return events
        finally:
            lease.execute("SELECT pg_advisory_unlock(734983015)")


def worker():
    last_scrub = 0
    scrub_interval = float(os.getenv("SCRUB_INTERVAL_SECONDS", "20"))
    while not STOP.is_set():
        if not PAUSED.is_set():
            try:
                scrub = time.monotonic() - last_scrub >= scrub_interval
                if not PAUSED.is_set():
                    if os.getenv("JOB_QUEUE_ENABLED") == "true":
                        from job_queue import enqueue

                        enqueue("scrub" if scrub else "repair", automatic=True)
                    else:
                        run_cycle(scrub)
                    if scrub:
                        last_scrub = time.monotonic()
            except Exception as error:
                STATE["last_error"] = str(error)
                LOGGER.exception("Recovery scan failed; will retry")
        if os.getenv("JOB_QUEUE_ENABLED") == "true":
            try:
                from job_queue import enqueue, publish_pending

                enqueue("search", automatic=True)
                publish_pending()
                STATE["last_cycle"] = time.time()
                STATE["last_error"] = None
            except Exception as error:
                STATE["last_error"] = str(error)
                LOGGER.exception("Job publisher failed; durable outbox retained")
        STOP.wait(2)


@asynccontextmanager
async def lifespan(application):
    STOP.clear()
    if os.getenv("JOB_QUEUE_ENABLED") == "true":
        with connect_db() as connection:
            paused = connection.execute(
                "SELECT paused FROM maintenance_control WHERE id=1"
            ).fetchone()["paused"]
        PAUSED.set() if paused else PAUSED.clear()
    application.state.worker = threading.Thread(target=worker, daemon=True)
    application.state.worker.start()
    yield
    STOP.set()
    await asyncio.to_thread(application.state.worker.join, 5)


app = FastAPI(title="Background Recovery and Scrubbing", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "healthy" if app.state.worker.is_alive() else "failed"}


@app.get("/status")
def status():
    return {
        "worker_alive": app.state.worker.is_alive(),
        "paused": PAUSED.is_set(),
        "scrub_interval_seconds": float(os.getenv("SCRUB_INTERVAL_SECONDS", "20")),
        "failure_detection": "heartbeat registry timeout (15 seconds)",
        **STATE,
    }


@app.get("/events")
def events():
    with connect_db() as connection:
        return connection.execute(
            "SELECT * FROM recovery_events ORDER BY event_id DESC LIMIT 100"
        ).fetchall()


@app.post("/pause")
def pause():
    PAUSED.set()
    if os.getenv("JOB_QUEUE_ENABLED") == "true":
        with connect_db() as connection:
            connection.execute("UPDATE maintenance_control SET paused=TRUE WHERE id=1")
        with connect_db() as lease:
            lease.execute("SELECT pg_advisory_lock(734983015)")
            lease.execute("SELECT pg_advisory_unlock(734983015)")
    with CYCLE_LOCK:
        pass
    return {"paused": True}


@app.post("/resume")
def resume():
    PAUSED.clear()
    if os.getenv("JOB_QUEUE_ENABLED") == "true":
        with connect_db() as connection:
            connection.execute("UPDATE maintenance_control SET paused=FALSE WHERE id=1")
    return {"paused": False}


@app.post("/run")
def manual_cycle(scrub: bool = True):
    return run_cycle(scrub)
