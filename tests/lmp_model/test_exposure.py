import numpy as np

from lmp_model import exposure


def test_fit_weights_uses_train_hours_and_lags_rt():
    rng = np.random.default_rng(0)
    hours, n_facilities = 2000, exposure.TOP_FACILITIES + 5
    rt_shadow = np.where(rng.random((hours, n_facilities)) < 0.2, -rng.exponential(50, (hours, n_facilities)), 0).astype("float32")
    true = np.zeros((n_facilities, 2), dtype="float32")
    true[0, 0], true[1, 1] = -0.5, 0.3
    rt_congestion = rt_shadow @ true
    rt_congestion[:100, 1] = np.nan
    cache = {"rt_shadow": rt_shadow, "da_shadow": rt_shadow, "rt_congestion": rt_congestion}
    train = np.arange(hours) < 1500

    top, weights = exposure.fit_weights(cache, train)
    features = exposure.exposure_features(cache, top, weights)

    assert len(top) == exposure.TOP_FACILITIES
    assert np.corrcoef(features[train, 0, 0], rt_congestion[train, 0])[0, 1] > 0.99
    assert np.isnan(features[: exposure.RT_LAG_HOURS, :, 1]).all()
    np.testing.assert_allclose(features[exposure.RT_LAG_HOURS:, :, 1], features[: -exposure.RT_LAG_HOURS, :, 0], rtol=1e-5)
