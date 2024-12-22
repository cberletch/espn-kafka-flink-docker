FROM apache/airflow:2.0.2
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev && \
    rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir duckdb-engine duckdb
USER airflow