from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from docker.types import Mount
from datetime import datetime
from datetime import timedelta
import pendulum
from pathlib import Path
from utils.loader import run_ddl, load_csv_postgres, read_sql_file
import os


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

def get_dwh_db_conn_params():
    conn = BaseHook.get_connection("dwh_postgres")

    return {
        "host": conn.host,
        "port": conn.port,
        "dbname": conn.schema,
        "user": conn.login,
        "password": conn.password,
    }


def init_schemas():
    conn_params = get_dwh_db_conn_params()
    run_ddl(filepath="/opt/airflow/sql/init_schemas.sql", conn_params=conn_params)


def init_raw_table():
    conn_params = get_dwh_db_conn_params()
    run_ddl(filepath="/opt/airflow/sql/transactions_ddl.sql", conn_params=conn_params)


def load_raw_transactions():
    conn_params = get_dwh_db_conn_params()
    load_csv_postgres(filepath="/opt/airflow/data/PS_20174392719_1491204439457_log.csv", conn_params=conn_params)


with DAG(
    dag_id="upload_transform_transactions",
    start_date=pendulum.datetime(2025, 2, 22),
    schedule="@daily",
    catchup=False,
    tags=["ingestion"]
) as dag:
    
    task_init_schemas = PythonOperator(
        task_id = "task_init_schemas",
        python_callable = init_schemas
    )

    task_init_raw_table = PythonOperator(
        task_id="task_init_raw_table",
        python_callable = init_raw_table
    )

    task_load_transactions = PythonOperator(
        task_id = "task_load_transactions",
        python_callable = load_raw_transactions
    )

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

    task_init_schemas >> task_init_raw_table >> task_load_transactions >> drop_dbt >> dbt_run