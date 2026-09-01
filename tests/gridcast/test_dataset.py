from unittest.mock import MagicMock

import pandas as pd
import pytest

from gridcast import dataset


def make_cursor(columns, rows=(), fetchone_result=None):
    cur = MagicMock()
    cur.description = [(col,) for col in columns]
    cur.fetchall.return_value = rows
    cur.fetchone.return_value = fetchone_result
    return cur


@pytest.fixture
def mock_get_connection(monkeypatch):
    """Patches gridcast.dataset.get_connection; returns the fake cursor for assertions."""
    def _patch(cur):
        conn = MagicMock()
        conn.cursor.return_value = cur
        monkeypatch.setattr(dataset, "get_connection", lambda: conn)
    return _patch


def test_dataset(mock_get_connection):
    cur = make_cursor(["zone", "time", "demand_mw"], rows=[("AEP", "2026-01-01", 100.0)])
    mock_get_connection(cur)

    result = dataset.dataset()

    cur.execute.assert_called_once_with("SELECT * FROM analytics.features;")
    assert list(result.columns) == ["zone", "time", "demand_mw"]
    assert result.iloc[0].tolist() == ["AEP", "2026-01-01", 100.0]


def test_latest_features_no_zone(mock_get_connection):
    cur = make_cursor(["zone", "time"], rows=[("AEP", "2026-01-01")])
    mock_get_connection(cur)

    dataset.latest_features()

    cur.execute.assert_called_once_with(
        "SELECT DISTINCT ON (zone) * FROM analytics.features ORDER BY zone, time DESC;",
        (),
    )


def test_latest_features_with_zone(mock_get_connection):
    cur = make_cursor(["zone", "time"], rows=[("AEP", "2026-01-01")])
    mock_get_connection(cur)

    dataset.latest_features(zone="AEP")

    cur.execute.assert_called_once_with(
        "SELECT DISTINCT ON (zone) * FROM analytics.features WHERE zone = %s ORDER BY zone, time DESC;",
        ("AEP",),
    )


def test_features_window_no_zone(mock_get_connection):
    cur = make_cursor(["zone", "time"], rows=[("AEP", "2026-01-01")])
    mock_get_connection(cur)

    dataset.features_window(24)

    cur.execute.assert_called_once_with(
        "SELECT * FROM analytics.features WHERE time >= now() - make_interval(hours => %s) "
        "ORDER BY zone, time;",
        (24,),
    )


def test_features_window_with_zone(mock_get_connection):
    cur = make_cursor(["zone", "time"], rows=[("AEP", "2026-01-01")])
    mock_get_connection(cur)

    dataset.features_window(24, zone="AEP")

    cur.execute.assert_called_once_with(
        "SELECT * FROM analytics.features WHERE time >= now() - make_interval(hours => %s) "
        "AND zone = %s ORDER BY zone, time;",
        (24, "AEP"),
    )


def test_latest_inst_load_time(mock_get_connection):
    expected = pd.Timestamp("2026-08-16 12:00", tz="UTC")
    cur = make_cursor(["time"], fetchone_result=(expected,))
    mock_get_connection(cur)

    result = dataset.latest_inst_load_time()

    cur.execute.assert_called_once_with("SELECT max(time) FROM public.instantaneous_load;")
    assert result == expected


def test_recent_peak_no_zone(mock_get_connection):
    cur = make_cursor(["zone", "peak_mw"], rows=[("AEP", 500.0)])
    mock_get_connection(cur)

    dataset.recent_peak()

    cur.execute.assert_called_once_with(
        "SELECT zone, max(demand_mw) AS peak_mw FROM analytics.features "
        "WHERE time >= now() - make_interval(days => %s) GROUP BY zone;",
        (30,),
    )


def test_recent_peak_with_zone(mock_get_connection):
    cur = make_cursor(["zone", "peak_mw"], rows=[("AEP", 500.0)])
    mock_get_connection(cur)

    dataset.recent_peak(days=7, zone="AEP")

    cur.execute.assert_called_once_with(
        "SELECT zone, max(demand_mw) AS peak_mw FROM analytics.features "
        "WHERE time >= now() - make_interval(days => %s) AND zone = %s GROUP BY zone;",
        (7, "AEP"),
    )
