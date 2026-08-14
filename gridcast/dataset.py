import pandas as pd

from gridcast.config import get_connection, setup_logging

setup_logging()


def dataset():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM analytics.features;")
    columns = [desc[0] for desc in cur.description]
    data = pd.DataFrame(cur.fetchall(), columns=columns)

    return data


def latest_features(zone: str | None = None) -> pd.DataFrame:
    """Most recent analytics.features row per zone, for inference."""
    conn = get_connection()
    cur = conn.cursor()
    query = "SELECT DISTINCT ON (zone) * FROM analytics.features"
    params = ()
    if zone is not None:
        query += " WHERE zone = %s"
        params = (zone,)
    query += " ORDER BY zone, time DESC;"
    cur.execute(query, params)
    columns = [desc[0] for desc in cur.description]
    data = pd.DataFrame(cur.fetchall(), columns=columns)

    return data
