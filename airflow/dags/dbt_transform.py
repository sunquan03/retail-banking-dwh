from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import timedelta
import pendulum
from utils.loader import read_sql_file
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="dbt_transform",
    default_args=default_args,
    schedule="@daily",
    start_date=pendulum.datetime(2026, 2, 24),
    catchup=False,
) as dag:

    drop_dbt = SQLExecuteQueryOperator(
        task_id="clean_old_dbt_data",
        conn_id="dwh_postgres",
        sql=read_sql_file("/opt/airflow/sql/drop_dbt.sql"),
    )


    dbt_run = DockerOperator(
        task_id="dbt_run",
        image="ghcr.io/dbt-labs/dbt-postgres:1.7.5",
        command="run --profiles-dir /usr/app --project-dir /usr/app",
        working_dir="/usr/app",
        mounts=[
            Mount(source=os.environ.get("DBT_HOST_PATH"), target="/usr/app", type="bind")
        ],
        environment={
            "POSTGRES_HOST": "dwh_postgres",
            "POSTGRES_USER": os.environ.get("POSTGRES_USER"),
            "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD"),
            "POSTGRES_DB": os.environ.get("POSTGRES_DB"),
        },
        network_mode="retail-banking-dwh_default",
        mount_tmp_dir=False,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
    )


    drop_dbt >> dbt_run