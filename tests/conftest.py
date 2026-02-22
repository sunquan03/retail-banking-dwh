import os
import psycopg2
import pytest
from dotenv import load_dotenv


load_dotenv()

@pytest.fixture(scope="session")
def conn_params():
    params = {
        "host": os.getenv("TEST_DB_HOST"),
        "port": os.getenv("TEST_DB_PORT"),
        "dbname": os.getenv("TEST_DB_NAME"),
        "user": os.getenv("TEST_DB_USER"),
        "password": os.getenv("TEST_DB_PASSWORD"),
    }
    #print(params)
    return params


@pytest.fixture(scope="session")
def pg_conn(conn_params):
    conn = psycopg2(**conn)
    try:
        yield conn
    finally:
        conn.close()

def _exec(conn, sql:str):
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql)

