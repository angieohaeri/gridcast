from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from gridcast.modeling.predict import predict


def make_model(feature_cols, predictions):
    model = MagicMock()
    model.feature_name_ = feature_cols
    model.predict.return_value = np.array(predictions)
    return model


def test_predict_builds_features_and_dispatches_per_horizon():
    df = pd.DataFrame({
        "time": pd.to_datetime(["2026-01-01 12:00", "2026-01-01 13:00"], utc=True),
        "zone": ["AEP", "AEP"],
        "temperature": [10.0, 20.0],
    })
    model_1h = make_model(["hdd"], [111.0, 222.0])
    model_24h = make_model(["cdd"], [333.0, 444.0])

    result = predict(df, models={1: model_1h, 24: model_24h})

    assert list(result.columns) == ["time", "zone", "y_1h", "y_24h"]
    assert result["zone"].tolist() == ["AEP", "AEP"]
    assert result["y_1h"].tolist() == [111.0, 222.0]
    assert result["y_24h"].tolist() == [333.0, 444.0]

    # each model only sees its own declared feature columns
    model_1h_input = model_1h.predict.call_args[0][0]
    assert list(model_1h_input.columns) == ["hdd"]
    model_24h_input = model_24h.predict.call_args[0][0]
    assert list(model_24h_input.columns) == ["cdd"]
