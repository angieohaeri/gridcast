import importlib.util
from pathlib import Path
from unittest.mock import MagicMock

from fastapi.testclient import TestClient
import pandas as pd
import pytest

API_MAIN_PATH = Path(__file__).resolve().parents[3] / "src" / "api" / "main.py"
spec = importlib.util.spec_from_file_location("api_main", API_MAIN_PATH)
api_main = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api_main)

HORIZONS = (1, 24, 72)


def fake_predict(df, models):
    """Stands in for gridcast.modeling.predict.predict - deterministic per horizon,
    ignores actual feature values, since that logic is covered by test_predict.py."""
    out = df[["time", "zone"]].copy()
    for h in models:
        out[f"y_{h}h"] = 100.0 + h
    return out


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(api_main, "load_models", lambda: {h: MagicMock() for h in HORIZONS})
    monkeypatch.setattr(api_main, "predict", fake_predict)
    with TestClient(api_main.app) as c:
        yield c


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "horizons": list(HORIZONS)}


def test_predict_merges_weather_and_nulls_missing(client, monkeypatch):
    df = pd.DataFrame({
        "time": pd.to_datetime(["2026-01-01 00:00"], utc=True),
        "zone": ["AEP"],
        "temperature": [10.0],
        "precipitation": [float("nan")],
        "wind_speed": [5.0],
        "cloud_cover": [80.0],
    })
    monkeypatch.setattr(api_main, "latest_features", lambda zone=None: df)

    response = client.get("/predict", params={"zone": "AEP"})

    assert response.status_code == 200
    [row] = response.json()
    assert row["zone"] == "AEP"
    assert row["y_1h"] == 101.0
    assert row["y_24h"] == 124.0
    assert row["y_72h"] == 172.0
    assert row["temperature"] == 10.0
    assert row["precipitation"] is None
    assert row["wind_speed"] == 5.0


def test_predict_404_when_no_data(client, monkeypatch):
    monkeypatch.setattr(api_main, "latest_features", lambda zone=None: pd.DataFrame())

    response = client.get("/predict", params={"zone": "NOPE"})

    assert response.status_code == 404


def test_history_aligns_predictions_to_future_actuals(client, monkeypatch):
    df = pd.DataFrame({
        "time": pd.to_datetime(["2026-01-01 00:00", "2026-01-01 01:00"], utc=True),
        "zone": ["AEP", "AEP"],
        "demand_mw": [500.0, 600.0],
    })
    monkeypatch.setattr(api_main, "features_window", lambda hours=48, zone=None: df)

    response = client.get("/history", params={"zone": "AEP", "hours": 48})

    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 6  # 2 timestamps x 3 horizons

    one_hour_rows = [r for r in rows if r["horizon_h"] == 1]
    # predicted at 00:00 for the 1h horizon lands on the actual at 01:00 (600.0)
    aligned = next(r for r in one_hour_rows if pd.Timestamp(r["time"]) == pd.Timestamp("2026-01-01 01:00", tz="UTC"))
    assert aligned["actual_mw"] == 600.0
    assert aligned["predicted_mw"] == 101.0

    # predicted at 01:00 for the 1h horizon lands on 02:00, which has no actual
    unaligned = next(r for r in one_hour_rows if pd.Timestamp(r["time"]) == pd.Timestamp("2026-01-01 02:00", tz="UTC"))
    assert unaligned["actual_mw"] is None


def test_history_404_when_no_data(client, monkeypatch):
    monkeypatch.setattr(api_main, "features_window", lambda hours=48, zone=None: pd.DataFrame())

    response = client.get("/history", params={"zone": "NOPE"})

    assert response.status_code == 404


def test_peak(client, monkeypatch):
    df = pd.DataFrame({"zone": ["AEP", "COMED"], "peak_mw": [1000.0, 2000.0]})
    monkeypatch.setattr(api_main, "recent_peak", lambda days=30, zone=None: df)

    response = client.get("/peak")

    assert response.status_code == 200
    assert response.json() == [
        {"zone": "AEP", "peak_mw": 1000.0},
        {"zone": "COMED", "peak_mw": 2000.0},
    ]


def test_peak_404_when_no_data(client, monkeypatch):
    monkeypatch.setattr(api_main, "recent_peak", lambda days=30, zone=None: pd.DataFrame())

    response = client.get("/peak", params={"zone": "NOPE"})

    assert response.status_code == 404


def test_freshness(client, monkeypatch):
    ts = pd.Timestamp("2026-08-16 12:00", tz="UTC")
    monkeypatch.setattr(api_main, "latest_load_time", lambda: ts)

    response = client.get("/freshness")

    assert response.status_code == 200
    assert pd.Timestamp(response.json()["latest_load_time"]) == ts


def test_freshness_none(client, monkeypatch):
    monkeypatch.setattr(api_main, "latest_load_time", lambda: None)

    response = client.get("/freshness")

    assert response.status_code == 200
    assert response.json()["latest_load_time"] is None
