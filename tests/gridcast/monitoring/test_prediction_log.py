from unittest.mock import MagicMock

import pandas as pd
import pytest

from gridcast.monitoring.prediction_log import prediction_rows, production_model_meta

META = {
    h: {
        "model_name": f"gridcast-lgbm-{h}h",
        "model_version": str(h),
        "model_run_id": f"run-{h}",
        "featureset": "v2",
    }
    for h in (1, 24, 72)
}


def test_prediction_rows_expands_zones_by_horizons():
    preds = pd.DataFrame(
        {
            "time": pd.to_datetime(["2026-09-01T00:00Z", "2026-09-01T00:00Z"]),
            "zone": ["AE", "AEP"],
            "y_1h": [100.0, 200.0],
            "y_24h": [110.0, 210.0],
            "y_72h": [120.0, 220.0],
        }
    )
    rows = prediction_rows(preds, META)
    assert len(rows) == 6  # 2 zones x 3 horizons


def test_prediction_rows_target_time_and_value_per_horizon():
    preds = pd.DataFrame(
        {"time": pd.to_datetime(["2026-09-01T00:00Z"]), "zone": ["AE"],
         "y_1h": [100.0], "y_24h": [110.0], "y_72h": [120.0]}
    )
    by_h = {r["horizon_h"]: r for r in prediction_rows(preds, META)}

    assert by_h[1]["predicted_mw"] == 100.0
    assert by_h[1]["target_time"] == pd.Timestamp("2026-09-01T01:00Z")
    assert by_h[72]["predicted_mw"] == 120.0
    assert by_h[72]["target_time"] == pd.Timestamp("2026-09-04T00:00Z")
    assert by_h[24]["feature_time"] == pd.Timestamp("2026-09-01T00:00Z")


def test_prediction_rows_stamps_model_metadata():
    preds = pd.DataFrame(
        {"time": pd.to_datetime(["2026-09-01T00:00Z"]), "zone": ["AE"],
         "y_1h": [1.0], "y_24h": [2.0], "y_72h": [3.0]}
    )
    by_h = {r["horizon_h"]: r for r in prediction_rows(preds, META)}
    assert by_h[24]["model_name"] == "gridcast-lgbm-24h"
    assert by_h[24]["model_run_id"] == "run-24"
    assert by_h[72]["featureset"] == "v2"


def test_production_model_meta_reads_version_and_featureset(monkeypatch):
    client = MagicMock()
    client.get_latest_versions.side_effect = lambda name, stages: [
        MagicMock(version="7", run_id=f"{name}-run")
    ]
    client.get_run.return_value = MagicMock(data=MagicMock(tags={"featureset": "v2"}))
    monkeypatch.setattr("gridcast.monitoring.prediction_log.MlflowClient", lambda: client)

    meta = production_model_meta()

    assert meta[1]["model_version"] == "7"
    assert meta[24]["model_name"] == "gridcast-lgbm-24h"
    assert meta[72]["model_run_id"] == "gridcast-lgbm-72h-run"
    assert meta[72]["featureset"] == "v2"


def test_production_model_meta_missing_featureset_tag(monkeypatch):
    client = MagicMock()
    client.get_latest_versions.side_effect = lambda name, stages: [
        MagicMock(version="1", run_id="r")
    ]
    client.get_run.return_value = MagicMock(data=MagicMock(tags={}))
    monkeypatch.setattr("gridcast.monitoring.prediction_log.MlflowClient", lambda: client)

    assert production_model_meta()[1]["featureset"] == ""


@pytest.mark.parametrize("h", [1, 24, 72])
def test_prediction_rows_covers_every_horizon(h):
    preds = pd.DataFrame(
        {"time": pd.to_datetime(["2026-09-01T00:00Z"]), "zone": ["AE"],
         "y_1h": [1.0], "y_24h": [2.0], "y_72h": [3.0]}
    )
    assert any(r["horizon_h"] == h for r in prediction_rows(preds, META))
