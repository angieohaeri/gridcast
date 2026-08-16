import numpy as np
import pandas as pd
import pytest

from gridcast.features import (
    build_labels,
    cyclical_features,
    degree_day_features,
    peak_hour_flag,
    weekend_flag,
)


def test_weekend_flag():
    # 2026-08-15 is a Saturday, 2026-08-17 is a Monday (US/Eastern)
    df = pd.DataFrame({
        "time": pd.to_datetime(
            ["2026-08-15 12:00", "2026-08-17 12:00"], utc=True
        ),
    })
    result = weekend_flag(df)
    assert result["is_weekend"].tolist() == [True, False]


def test_degree_day_features():
    df = pd.DataFrame({"temperature": [0.0, 18.33, 30.0]})
    result = degree_day_features(df)
    assert result["hdd"].tolist() == [18.33, 0.0, 0.0]
    assert result["cdd"].tolist() == [0.0, 0.0, 30.0 - 18.33]


def test_peak_hour_flag():
    # hours 14:00-18:00 US/Eastern should be flagged
    df = pd.DataFrame({
        "time": pd.to_datetime(
            ["2026-08-17 18:00", "2026-08-17 22:00", "2026-08-18 02:00"], utc=True
        ),
    })
    result = peak_hour_flag(df)
    assert result["is_peak_hour"].tolist() == [True, True, False]


def test_cyclical_features():
    # 2026-01-01 05:00 UTC -> 2026-01-01 00:00 US/Eastern (EST, UTC-5)
    # 2026-04-01 16:00 UTC -> 2026-04-01 12:00 US/Eastern (EDT, UTC-4)
    df = pd.DataFrame({
        "time": pd.to_datetime(
            ["2026-01-01 05:00", "2026-04-01 16:00"], utc=True
        ),
    })
    result = cyclical_features(df)

    assert result["year"].tolist() == [2026, 2026]
    assert result["hour_sin"].tolist() == pytest.approx([0.0, 0.0], abs=1e-9)
    assert result["hour_cos"].tolist() == pytest.approx([1.0, -1.0], abs=1e-9)
    assert result["month_sin"].tolist() == pytest.approx([0.0, 1.0], abs=1e-9)
    assert result["month_cos"].tolist() == pytest.approx([1.0, 0.0], abs=1e-9)


def test_build_labels_shifts_within_zone():
    df = pd.DataFrame({
        "zone": ["A", "A", "A", "B", "B", "B"],
        "time": pd.to_datetime(
            ["2026-01-01 00:00", "2026-01-01 01:00", "2026-01-01 02:00"] * 2,
            utc=True,
        ),
        "demand_mw": [10.0, 20.0, 30.0, 100.0, 200.0, 300.0],
    })
    result = build_labels(df)

    zone_a = result[result["zone"] == "A"].sort_values("time")
    zone_b = result[result["zone"] == "B"].sort_values("time")

    # y_1h is demand_mw one hour ahead, within the same zone
    assert zone_a["y_1h"].tolist()[:2] == [20.0, 30.0]
    assert np.isnan(zone_a["y_1h"].tolist()[2])

    assert zone_b["y_1h"].tolist()[:2] == [200.0, 300.0]
    assert np.isnan(zone_b["y_1h"].tolist()[2])

    # horizons longer than the group size are entirely unfilled (no cross-zone leakage)
    assert result["y_24h"].isna().all()
    assert result["y_72h"].isna().all()
