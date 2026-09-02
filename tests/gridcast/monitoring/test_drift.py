import numpy as np
import pandas as pd
import pytest

from gridcast.monitoring.drift import (
    drift_row,
    drift_table,
    per_zone_target_drift,
    psi,
)


def test_psi_zero_for_identical_samples():
    x = pd.Series(np.random.default_rng(0).normal(size=5000))
    assert psi(x, x) == pytest.approx(0.0, abs=1e-9)


def test_psi_grows_with_the_size_of_the_shift():
    rng = np.random.default_rng(0)
    ref = pd.Series(rng.normal(0, 1, size=5000))
    nudged = pd.Series(rng.normal(0.2, 1, size=5000))
    shifted = pd.Series(rng.normal(2.0, 1, size=5000))
    assert psi(ref, nudged) < psi(ref, shifted)
    assert psi(ref, shifted) > 0.25


def test_psi_flags_a_constant_reference_that_spreads_out():
    # deciles collapse on the constant reference; the uniform-bin fallback still
    # catches that current has spread across a range the reference never occupied
    ref = pd.Series([3.0] * 500)
    cur = pd.Series(np.random.default_rng(0).normal(size=500))
    assert psi(ref, cur) > 0.25


def test_psi_zero_when_both_sides_are_the_same_constant():
    assert psi(pd.Series([3.0] * 500), pd.Series([3.0] * 500)) == 0.0


def test_psi_nan_on_empty_input():
    assert np.isnan(psi(pd.Series([1.0, 2.0]), pd.Series([], dtype=float)))


def test_drift_row_skipped_when_a_side_is_too_thin():
    row = drift_row("inst_load_lag_1h", pd.Series([1.0, 2.0, 3.0]), pd.Series(range(500)))
    assert row["status"] == "skipped"
    assert row["drifted"] is False
    assert row["psi"] is None


def test_drift_row_flags_a_significant_shift():
    rng = np.random.default_rng(1)
    ref = pd.Series(rng.normal(0, 1, size=3000))
    cur = pd.Series(rng.normal(3, 1, size=3000))
    row = drift_row("temperature", ref, cur)
    assert row["status"] == "ok"
    assert row["drifted"] is True
    assert row["ks_pvalue"] < 0.05


def test_drift_row_leaves_a_stable_column_unflagged():
    rng = np.random.default_rng(2)
    row = drift_row(
        "temperature",
        pd.Series(rng.normal(size=3000)),
        pd.Series(rng.normal(size=3000)),
    )
    assert row["status"] == "ok"
    assert row["drifted"] is False


def test_drift_table_one_row_per_column():
    rng = np.random.default_rng(3)
    ref = pd.DataFrame({"a": rng.normal(size=2000), "b": rng.normal(size=2000)})
    cur = pd.DataFrame({"a": rng.normal(size=2000), "b": rng.normal(size=2000)})
    table = drift_table(ref, cur, ["a", "b"])
    assert list(table["column"]) == ["a", "b"]
    assert set(table["status"]) == {"ok"}


def test_per_zone_target_drift_isolates_the_shifted_zone():
    rng = np.random.default_rng(4)
    reference = pd.DataFrame(
        {
            "zone": ["A"] * 500 + ["B"] * 500,
            "demand_mw": np.concatenate([rng.normal(100, 5, 500), rng.normal(200, 5, 500)]),
        }
    )
    current = pd.DataFrame(
        {
            "zone": ["A"] * 500 + ["B"] * 500,
            "demand_mw": np.concatenate([rng.normal(100, 5, 500), rng.normal(260, 5, 500)]),
        }
    )
    result = per_zone_target_drift(reference, current).set_index("zone")
    assert sorted(result.index) == ["A", "B"]
    assert result.loc["B", "drifted"]
    assert not result.loc["A", "drifted"]
