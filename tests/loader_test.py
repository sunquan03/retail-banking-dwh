from pathlib import Path
from utils.loader import run_ddl, load_csv_postgres


def test_run_ddl(conn_params):
    schemas_file = Path("airflow/sql/init_schemas.sql")
    assert schemas_file.exists(), f"Missing file {schemas_file}"

    run_ddl(schemas_file, conn_params)

    ddl_file = Path("airflow/sql/transactions_ddl.sql")
    assert ddl_file.exists(), f"Missing file {ddl_file}"

    run_ddl(ddl_file, conn_params)

    import psycopg2

    conn = psycopg2.connect(**conn_params)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select exists(
                        select 1 from information_schema.tables
                        where table_schema='raw' and table_name='transactions'
                    )
                    """
                )
                assert cur.fetchone() == (True,)
    finally:
        conn.close()

def test_load_csv_postgres(conn_params):
    rows_actual = 6362620
    
    filepath = "data/PS_20174392719_1491204439457_log.csv"
    load_csv_postgres(filepath, conn_params)

    import psycopg2

    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute("select count(*) from raw.transactions")
            rows_cnt = cur.fetchone()[0]
            assert rows_cnt == rows_actual, f"Number of rows inserted doesn't match: {rows_cnt}"
            