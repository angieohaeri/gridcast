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


def features_window(hours: int, zone: str | None = None) -> pd.DataFrame:
    """analytics.features rows from the trailing `hours` hours, for /history."""
    conn = get_connection()
    cur = conn.cursor()
    query = "SELECT * FROM analytics.features WHERE time >= now() - make_interval(hours => %s)"
    params = (hours,)
    if zone is not None:
        query += " AND zone = %s"
        params = (hours, zone)
    query += " ORDER BY zone, time;"
    cur.execute(query, params)
    columns = [desc[0] for desc in cur.description]
    data = pd.DataFrame(cur.fetchall(), columns=columns)

    return data


def latest_inst_load_time() -> pd.Timestamp | None:
    """Most recent reading in analytics.stg_inst_load.

    Used for the dashboard's freshness indicator: inst_load carries no settlement
    lag (unlike demand_mw's ~2-3 days), so it reflects whether the pipeline is
    actually live rather than whether the settled feed is caught up.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT max(time) FROM analytics.stg_inst_load;")
    (result,) = cur.fetchone()
    return result


def recent_peak(days: int = 30, zone: str | None = None) -> pd.DataFrame:
    """Per-zone max demand_mw over the trailing `days` days, for map normalization."""
    conn = get_connection()
    cur = conn.cursor()
    query = (
        "SELECT zone, max(demand_mw) AS peak_mw FROM analytics.features "
        "WHERE time >= now() - make_interval(days => %s)"
    )
    params = (days,)
    if zone is not None:
        query += " AND zone = %s"
        params = (days, zone)
    query += " GROUP BY zone;"
    cur.execute(query, params)
    columns = [desc[0] for desc in cur.description]
    data = pd.DataFrame(cur.fetchall(), columns=columns)

    return data
