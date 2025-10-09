FROM apache/airflow:2.10.5-python3.11

USER root


RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk file git-all python3-venv python3-pip python-is-python3 \
    && rm -rf /var/lib/apt/lists/*

# Ensure the 'airflow' group exists and set /opt/airflow as the home directory
# RUN groupadd -f airflow && \
#     usermod -d /opt/airflow airflow && \
#     chown -R airflow:airflow /opt/airflow
    
USER airflow
