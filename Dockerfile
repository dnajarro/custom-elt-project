FROM apache/airflow:latest

USER airflow

RUN pip install apache-airflow-providers-docker \
    && pip install apache-airflow-providers-http \
    && pip install apache-airflow-providers-airbyte

USER root