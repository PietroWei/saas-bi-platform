FROM python:3.11-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
        dbt-core==1.8.4 \
        dbt-postgres==1.8.2

ENV DBT_PROFILES_DIR=/usr/app/dbt

WORKDIR /usr/app/dbt
