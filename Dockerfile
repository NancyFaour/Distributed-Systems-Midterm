FROM python:3.13.17-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY storage_node.py metadata_service.py ./
COPY object_store.py metadata_db.py chunking.py consistent_hashing.py ./
COPY redundancy.py redundant_store.py recovery_service.py ./
COPY gateway.py settings.py cluster_status.py search_index.py job_queue.py rebalancing.py ./
COPY scripts/migrate.py ./scripts/migrate.py
COPY infra/postgres ./infra/postgres

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "storage_node:app", "--host", "0.0.0.0", "--port", "8000"]
