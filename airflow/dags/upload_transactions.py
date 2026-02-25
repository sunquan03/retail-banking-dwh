from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
from pathlib import Path
import pendulum
from utils.loader import run_ddl, load_csv_postgres


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
    dag_id="upload_transactions",
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

    task_init_schemas >> task_init_raw_table >> task_load_transactions