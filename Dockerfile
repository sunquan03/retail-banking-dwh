FROM apache/airflow:3.1.7

USER root

RUN pip install "apache-airflow[auth-fab]"

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt