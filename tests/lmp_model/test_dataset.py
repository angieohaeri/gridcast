import numpy as np
import pandas as pd

from lmp_model import dataset


def test_load_window_scatters_rows_onto_the_grid(monkeypatch):
    rows = pd.DataFrame(
        {
            "time": pd.to_datetime(["2026-01-01 01:00", "2026-01-01 02:00"], utc=True),
            "pnode_id": [20, 10],
            "da_lmp": [5.0, 7.0],
            "da_congestion_price": [1.0, 2.0],
            "da_marginal_loss_price": [0.1, 0.2],
            "spread_last_verified_day": [None, 3.0],
            "congestion_spread_last_verified_day": [0.5, 0.6],
            "spread_mean_7d": [1.0, 2.0],
            "spread_std_7d": [0.1, 0.2],
            "spike_share_7d": [0.0, 0.1],
            "congestion_spread_mean_7d": [0.3, 0.4],
            "spread": [4.0, None],
        }
    )
    monkeypatch.setattr(dataset, "query", lambda sql, params=(): rows)
    monkeypatch.setattr(dataset, "load_system", lambda times: np.zeros((len(times), 1), "float32"))
    monkeypatch.setattr(
        dataset, "load_area_forecast", lambda times: (pd.Index(["AEP", "RTO"]), np.zeros((len(times), 2), "float32"))
    )
    monkeypatch.setattr(dataset, "load_node_area", lambda areas, pnode_ids: np.array([1, 0]))

    window = dataset.load_window(
        pd.Timestamp("2026-01-01 01:00", tz="UTC"),
        pd.Timestamp("2026-01-01 03:00", tz="UTC"),
        pd.Index([10, 20]),
    )

    assert window.x.shape == (2, 2, len(dataset.FEATURES))
    assert window.exists.tolist() == [[False, True], [True, False]]
    assert window.x[0, 1, 0] == 5.0 and window.x[1, 0, 0] == 7.0
    assert window.y[0, 1] == 4.0
    assert pd.isna(window.y[1, 0]) and pd.isna(window.x[0, 1, 3])
    assert window.area_forecast.shape == (2, 2) and window.node_area.tolist() == [1, 0]
