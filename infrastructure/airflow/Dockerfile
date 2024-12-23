FROM apache/airflow:2.10.4

USER root
# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    libssl-dev \
    cmake && \
    rm -rf /var/lib/apt/lists/*

USER airflow
# Install Python packages
RUN pip install --no-cache-dir \
    'duckdb>=0.8.1' \
    'duckdb-engine>=0.6.4' \
    'pyarrow>=6.0.1'