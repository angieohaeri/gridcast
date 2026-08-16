from datetime import UTC, datetime
from unittest.mock import MagicMock

from load_producer import LOAD_COLUMNS, poll_eia_load, poll_pjm_load
import pandas as pd

START = datetime(2026, 8, 1, tzinfo=UTC)
END = datetime(2026, 8, 8, tzinfo=UTC)


def test_poll_pjm_load_sums_sub_areas_and_requires_all_verified():
    pjm = MagicMock()
    pjm.get_load_metered_hourly.return_value = pd.DataFrame({
        "Interval End": ["2026-08-01 01:00", "2026-08-01 01:00", "2026-08-01 01:00"],
        "Zone": ["AEP", "AEP", "COMED"],
        "MW": [100.0, 50.0, 200.0],
        "Is Verified": [True, True, False],
    })

    result = poll_pjm_load(pjm, START, END, zone_ids=["AEP", "COMED"])

    pjm.get_load_metered_hourly.assert_called_once_with("2026-08-01", "2026-08-08")
    assert list(result.columns) == LOAD_COLUMNS

    aep = result[result["zone"] == "AEP"].iloc[0]
    assert aep["demand_mw"] == 150.0
    assert aep["is_verified"] == True

    comed = result[result["zone"] == "COMED"].iloc[0]
    assert comed["demand_mw"] == 200.0
    assert comed["is_verified"] == False


def test_poll_pjm_load_filters_to_requested_zones():
    pjm = MagicMock()
    pjm.get_load_metered_hourly.return_value = pd.DataFrame({
        "Interval End": ["2026-08-01 01:00", "2026-08-01 01:00"],
        "Zone": ["AEP", "DOM"],
        "MW": [100.0, 300.0],
        "Is Verified": [True, True],
    })

    result = poll_pjm_load(pjm, START, END, zone_ids=["AEP"])

    assert result["zone"].tolist() == ["AEP"]


def test_poll_eia_load_maps_columns_and_tags_source():
    eia = MagicMock()
    eia.get_dataset.return_value = pd.DataFrame({
        "Interval End": ["2026-08-01 01:00"],
        "Load": [50000.0],
        "Load Forecast": [51000.0],
        "Net Generation": [49000.0],
        "Total Interchange": [-500.0],
    })

    result = poll_eia_load(eia, START, END)

    eia.get_dataset.assert_called_once_with(
        "electricity/rto/region-data",
        start="2026-08-01",
        end="2026-08-08",
        facets={"respondent": "PJM"},
    )
    assert list(result.columns) == LOAD_COLUMNS
    row = result.iloc[0]
    assert row["zone"] == "RTO"
    assert row["source"] == "eia"
    assert row["is_verified"] is None
    assert row["demand_mw"] == 50000.0
    assert row["demand_forecast_mw"] == 51000.0
    assert row["net_generation_mw"] == 49000.0
    assert row["total_interchange_mw"] == -500.0
