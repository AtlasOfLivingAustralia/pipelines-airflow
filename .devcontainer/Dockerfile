FROM apache/airflow:2.10.1

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk file \
    && rm -rf /var/lib/apt/lists/*
USER airflow