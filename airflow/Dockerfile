
FROM apache/airflow:3.4.0-python3.12

ARG AIRFLOW_VERSION=3.4.0

COPY airflow/requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements.txt
