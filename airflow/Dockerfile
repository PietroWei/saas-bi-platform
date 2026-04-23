FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
 && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# DAGs are mounted at runtime via compose volume; nothing else to copy.
