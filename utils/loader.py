import psycopg2
import pandas as pd
from io import StringIO

def init_ddl(filepath:str, conn_params:dict):
    with open(filepath, "r", encoding="utf-8") as f:
        sql = f.read()
        conn = psycopg2.connect(**conn_params)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
        finally:
            conn.close()
            


def load_csv_postgres(filepath:str, conn_params:dict):
    df = pd.read_csv(filepath)
    
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    buf = StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)

    cur.copy_expert("""
                    COPY raw.transactions (
                        step,
                        type,
                        amount,
                        nameOrig,
                        oldbalanceOrg,
                        newbalanceOrig,
                        nameDest,
                        oldbalanceDest,
                        newbalanceDest,
                        isFraud,
                        isFlaggedFraud
                    )
                    FROM STDIN
                    WITH (FORMAT csv, HEADER true)
                    """,
                    buf)

    conn.commit()
    cur.close()
    conn.close()
    