from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from sklearn.metrics import r2_score

from gridcast.modeling.train import evaluate, per_zone_metrics, promote_if_better, split_by_date


def test_split_by_date_start_and_end():
    df = pd.DataFrame({"time": pd.to_datetime(
        ["2026-01-01", "2026-02-01", "2026-03-01", "2026-04-01"]
    )})
    result = split_by_date(df, start=pd.Timestamp("2026-02-01"), end=pd.Timestamp("2026-04-01"))
    assert result["time"].tolist() == list(pd.to_datetime(["2026-02-01", "2026-03-01"]))


def test_split_by_date_open_ended():
    df = pd.DataFrame({"time": pd.to_datetime(
        ["2026-01-01", "2026-02-01", "2026-03-01"]
    )})
    assert split_by_date(df, start=None, end=pd.Timestamp("2026-02-01"))["time"].tolist() == [
        pd.Timestamp("2026-01-01")
    ]
    assert split_by_date(df, start=pd.Timestamp("2026-02-01"), end=None)["time"].tolist() == [
        pd.Timestamp("2026-02-01"), pd.Timestamp("2026-03-01")
    ]
    assert split_by_date(df, start=None, end=None)["time"].tolist() == list(df["time"])


def test_evaluate_drops_missing_labels_and_scores():
    df = pd.DataFrame({
        "y_1h": [100.0, 200.0, np.nan, 300.0],
        "feat1": [1, 2, 3, 4],
    })
    model = MagicMock()
    model.predict.return_value = np.array([110.0, 190.0, 310.0])

    result = evaluate(model, df, feature_cols=["feat1"], h=1)

    # only the 3 non-null rows should reach the model
    assert model.predict.call_args[0][0].shape[0] == 3

    y_true = np.array([100.0, 200.0, 300.0])
    y_pred = np.array([110.0, 190.0, 310.0])
    error = y_pred - y_true
    assert result["rmse"] == pytest.approx(np.sqrt(np.mean(error**2)))
    assert result["mae"] == pytest.approx(np.mean(np.abs(error)))
    assert result["r2"] == pytest.approx(r2_score(y_true, y_pred))


def test_per_zone_metrics_grouped_by_zone():
    df = pd.DataFrame({
        "zone": ["A", "A", "B", "B"],
        "y_1h": [100.0, 200.0, 50.0, 150.0],
        "feat1": [1, 2, 3, 4],
    })
    model = MagicMock()
    model.predict.return_value = np.array([110.0, 190.0, 60.0, 140.0])

    result = per_zone_metrics(model, df, feature_cols=["feat1"], h=1).set_index("zone")

    assert result.loc["A", "mae"] == pytest.approx(10.0)
    assert result.loc["A", "mape"] == pytest.approx(np.mean([10 / 100, 10 / 200]) * 100)
    assert result.loc["B", "mae"] == pytest.approx(10.0)
    assert result.loc["B", "mape"] == pytest.approx(np.mean([10 / 50, 10 / 150]) * 100)


def make_client(current_versions, current_rmse=None):
    client = MagicMock()
    client.get_latest_versions.return_value = current_versions
    if current_versions:
        client.get_run.return_value = MagicMock(data=MagicMock(metrics={"test_rmse": current_rmse}))
    return client


def test_promote_if_better_no_current_production():
    client = make_client(current_versions=[])

    promote_if_better(client, "gridcast-lgbm-1h", "3", new_rmse=100.0)

    client.transition_model_version_stage.assert_called_once_with(
        name="gridcast-lgbm-1h", version="3", stage="Production", archive_existing_versions=True
    )


def test_promote_if_better_new_is_better():
    current = MagicMock(run_id="run1", version="2")
    client = make_client(current_versions=[current], current_rmse=150.0)

    promote_if_better(client, "gridcast-lgbm-1h", "3", new_rmse=100.0)

    client.transition_model_version_stage.assert_called_once_with(
        name="gridcast-lgbm-1h", version="3", stage="Production", archive_existing_versions=True
    )


def test_promote_if_better_new_is_worse():
    current = MagicMock(run_id="run1", version="2")
    client = make_client(current_versions=[current], current_rmse=50.0)

    promote_if_better(client, "gridcast-lgbm-1h", "3", new_rmse=100.0)

    client.transition_model_version_stage.assert_not_called()


def test_promote_if_better_tie_does_not_promote():
    current = MagicMock(run_id="run1", version="2")
    client = make_client(current_versions=[current], current_rmse=100.0)

    promote_if_better(client, "gridcast-lgbm-1h", "3", new_rmse=100.0)

    client.transition_model_version_stage.assert_not_called()
