FROM apache/airflow:3.1.7

USER airflow

RUN pip install --no-cache-dir "apache-airflow[auth-fab]"

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt