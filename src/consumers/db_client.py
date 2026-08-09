import os

import psycopg2


def get_connection():
    conn = psycopg2.connect(
        host=os.environ["TIMESCALEDB_HOST"],
        port=os.environ["TIMESCALEDB_PORT"],
        dbname=os.environ["TIMESCALEDB_DB"],
        user=os.environ["TIMESCALEDB_USER"],
        password=os.environ["TIMESCALEDB_PASSWORD"],
    )
    conn.autocommit = True
    return conn
