"""Durable DB outbox -> confirmed RabbitMQ messages -> acknowledged workers."""

import hashlib
import json
import logging
import os
import socket
import time
import uuid

import pika
from psycopg.types.json import Jsonb
from metadata_db import connect_db
from settings import setting

QUEUE = setting("MAINTENANCE_QUEUE", "storage-maintenance-v1")
LOGGER = logging.getLogger(__name__)


def broker():
    credentials = pika.PlainCredentials(
        setting("RABBITMQ_USER", "midterm"), setting("RABBITMQ_PASSWORD", "")
    )
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=setting("RABBITMQ_HOST", "127.0.0.1"),
            credentials=credentials,
            heartbeat=0,
            socket_timeout=10,
            blocked_connection_timeout=30,
        )
    )


def enqueue(kind, automatic=False):
    with connect_db() as connection:
        row = connection.execute(
            """INSERT INTO maintenance_jobs(kind,automatic) VALUES (%s,%s)
            ON CONFLICT(kind) WHERE status IN ('pending','queued','running') DO NOTHING RETURNING *""",
            (kind, automatic),
        ).fetchone()
        if row is None:
            row = connection.execute(
                "SELECT * FROM maintenance_jobs WHERE kind=%s AND status IN ('pending','queued','running')",
                (kind,),
            ).fetchone()
    return row


def publish_pending():
    with connect_db() as connection:
        rows = connection.execute(
            "SELECT * FROM maintenance_jobs WHERE status='pending' ORDER BY created_at LIMIT 20 FOR UPDATE SKIP LOCKED"
        ).fetchall()
        if not rows:
            return 0
        session = broker()
        try:
            channel = session.channel()
            channel.queue_declare(queue=QUEUE, durable=True)
            channel.confirm_delivery()
            for job in rows:
                channel.basic_publish(
                    exchange="",
                    routing_key=QUEUE,
                    body=str(job["job_id"]).encode(),
                    mandatory=True,
                    properties=pika.BasicProperties(
                        delivery_mode=2, content_type="text/plain"
                    ),
                )
                connection.execute(
                    "UPDATE maintenance_jobs SET status='queued',updated_at=now() WHERE job_id=%s",
                    (job["job_id"],),
                )
        finally:
            session.close()
    return len(rows)


def execute(job_id, worker_id):
    # Session advisory locks make duplicate RabbitMQ delivery safe, including a
    # worker crash after committing results but before acknowledging the message.
    lock = int.from_bytes(
        hashlib.sha256(str(job_id).encode()).digest()[:8], "big", signed=True
    )
    with connect_db() as lease:
        if not lease.execute(
            "SELECT pg_try_advisory_lock(%s) AS ok", (lock,)
        ).fetchone()["ok"]:
            return False
        try:
            with connect_db() as connection:
                job = connection.execute(
                    "SELECT * FROM maintenance_jobs WHERE job_id=%s", (job_id,)
                ).fetchone()
                if not job or job["status"] in ("done", "failed"):
                    return True
                paused = connection.execute(
                    "SELECT paused FROM maintenance_control WHERE id=1"
                ).fetchone()["paused"]
                if paused and job["automatic"] and job["kind"] != "search":
                    connection.execute(
                        "UPDATE maintenance_jobs SET status='done',result=%s,updated_at=now() WHERE job_id=%s",
                        (Jsonb({"skipped": "maintenance paused"}), job_id),
                    )
                    return True
                connection.execute(
                    "UPDATE maintenance_jobs SET status='running',worker_id=%s,attempts=attempts+1,updated_at=now() WHERE job_id=%s",
                    (worker_id, job_id),
                )
            from redundant_store import RedundantStore

            store = RedundantStore()
            try:
                if job["kind"] == "search":
                    from search_index import sync_index

                    result = sync_index()
                else:
                    lease.execute("SELECT pg_advisory_lock(734983015)")
                    try:
                        # Check the shared pause flag again after waiting for the scan lease.
                        with connect_db() as connection:
                            paused = connection.execute(
                                "SELECT paused FROM maintenance_control WHERE id=1"
                            ).fetchone()["paused"]
                        if paused and job["automatic"]:
                            result = {"skipped": "maintenance paused"}
                        elif job["kind"] == "rebalance":
                            from rebalancing import rebalance

                            result = rebalance(store)
                        else:
                            result = {
                                "events": store.maintain(scrub=job["kind"] == "scrub")
                            }
                    finally:
                        lease.execute("SELECT pg_advisory_unlock(734983015)")
                with connect_db() as connection:
                    connection.execute(
                        "UPDATE maintenance_jobs SET status='done',result=%s,error=NULL,updated_at=now() WHERE job_id=%s",
                        (Jsonb(result), job_id),
                    )
                LOGGER.info("Completed %s job %s on %s", job["kind"], job_id, worker_id)
                return True
            except Exception as error:
                with connect_db() as connection:
                    connection.execute(
                        """UPDATE maintenance_jobs SET status=CASE WHEN attempts<3 THEN 'pending' ELSE 'failed' END,
                        error=%s,updated_at=now() WHERE job_id=%s""",
                        (str(error), job_id),
                    )
                LOGGER.exception("Job failed; bounded retry scheduled")
                return True  # DB outbox publishes a new delivery if retry is needed.
        finally:
            lease.execute("SELECT pg_advisory_unlock(%s)", (lock,))


def consume():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARNING)
    worker_id = os.getenv("HOSTNAME", socket.gethostname()) + "-" + uuid.uuid4().hex[:6]
    while True:
        try:
            session = broker()
            channel = session.channel()
            channel.queue_declare(queue=QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)

            def on_message(ch, method, properties, body):
                try:
                    job_id = uuid.UUID(body.decode())
                except (ValueError, UnicodeError):
                    ch.basic_reject(method.delivery_tag, requeue=False)
                    return
                if execute(job_id, worker_id):
                    ch.basic_ack(method.delivery_tag)
                else:
                    time.sleep(1)
                    ch.basic_nack(method.delivery_tag, requeue=True)

            channel.basic_consume(queue=QUEUE, on_message_callback=on_message)
            channel.start_consuming()
        except Exception:
            LOGGER.exception("Worker connection interrupted; reconnecting")
            time.sleep(3)


if __name__ == "__main__":
    consume()
