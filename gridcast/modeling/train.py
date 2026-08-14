from datetime import UTC, datetime

import lightgbm as lgb
from loguru import logger
import mlflow
import mlflow.lightgbm
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
import numpy as np
import pandas as pd
from sklearn.metrics import r2_score
import typer

from gridcast.dataset import dataset
from gridcast.features import features

app = typer.Typer()

horizons = (1, 24, 72)
featureset = "v1"

# val/test are fixed-size windows relative to the most recent row in the data
# (not wall-clock now - load settles 2-3 days late, see references/decisions.md).
# train is everything older than that, so it grows as more data is ingested.
test_months = 7
val_months = 12

# not model features: time is a split key not a predictor, demand_mw is the
# unlagged/raw target, observation_count is a weather data-quality diagnostic
non_features = ["time", "demand_mw", "observation_count"]


def build_labels(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["zone", "time"]).reset_index(drop=True)
    for h in horizons:
        df[f"y_{h}h"] = df.groupby("zone")["demand_mw"].shift(-h)
    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = features(df, drop_time_col=False)
    df["zone"] = df["zone"].astype("category")
    return df[df["demand_lag_168h"].notna()]


def split_by_date(df: pd.DataFrame, start: pd.Timestamp | None, end: pd.Timestamp | None) -> pd.DataFrame:
    mask = pd.Series(True, index=df.index)
    if start is not None:
        mask &= df["time"] >= start
    if end is not None:
        mask &= df["time"] < end
    return df[mask]


def train_horizon(train: pd.DataFrame, val: pd.DataFrame, h: int) -> tuple[lgb.LGBMRegressor, list[str]]:
    y_col = f"y_{h}h"
    other_y_cols = [f"y_{hh}h" for hh in horizons if hh != h]
    feature_cols = [c for c in train.columns if c not in non_features + [y_col] + other_y_cols]

    train = train.dropna(subset=[y_col])
    val = val.dropna(subset=[y_col])

    model = lgb.LGBMRegressor(objective="regression")
    model.fit(
        train[feature_cols],
        train[y_col],
        eval_X=val[feature_cols],
        eval_y=val[y_col],
        categorical_feature=["zone"],
        callbacks=[lgb.early_stopping(50), lgb.log_evaluation(50)],
    )
    return model, feature_cols


def evaluate(model: lgb.LGBMRegressor, df: pd.DataFrame, feature_cols: list[str], h: int) -> dict[str, float]:
    y_col = f"y_{h}h"
    df = df.dropna(subset=[y_col])
    preds = model.predict(df[feature_cols])
    error = preds - df[y_col].to_numpy()

    metrics = {
        "rmse": float(np.sqrt(np.mean(error**2))),
        "mae": float(np.mean(np.abs(error))),
        "r2": float(r2_score(df[y_col], preds)),
    }

    # per-zone MAE/MAPE: an aggregate score can hide one bad zone, and zones
    # vary a lot in size so MAPE normalizes for that
    per_zone = df.assign(pred=preds, error=error)
    for zone, group in per_zone.groupby("zone", observed=True):
        metrics[f"mae_{zone}"] = float(np.mean(np.abs(group["error"])))
        metrics[f"mape_{zone}"] = float(np.mean(np.abs(group["error"] / group[y_col])) * 100)

    return metrics


@app.command()
def main():
    logger.info("Loading analytics.features...")
    df = build_labels(dataset())
    df = build_features(df)

    test_start = df["time"].max() - pd.DateOffset(months=test_months)
    val_start = test_start - pd.DateOffset(months=val_months)

    train = split_by_date(df, None, val_start)
    val = split_by_date(df, val_start, test_start)
    test = split_by_date(df, test_start, None)

    mlflow.set_experiment("gridcast-lgbm")
    client = MlflowClient()
    run_name = f"lgbm_{featureset}_{datetime.now(UTC):%Y%m%d}"

    with mlflow.start_run(run_name=run_name):
        mlflow.set_tags(
            {
                "featureset": featureset,
                "train_date_range": f"{df['time'].min():%Y-%m-%d}:{val_start:%Y-%m-%d}",
                "val_date_range": f"{val_start:%Y-%m-%d}:{test_start:%Y-%m-%d}",
                "test_date_range": f"{test_start:%Y-%m-%d}:{df['time'].max():%Y-%m-%d}",
            }
        )

        for h in horizons:
            logger.info(f"Training {h}h-ahead model...")
            with mlflow.start_run(run_name=f"{h}h", nested=True):
                model, feature_cols = train_horizon(train, val, h)
                metrics = evaluate(model, test, feature_cols, h)
                metrics["train_r2"] = evaluate(model, train, feature_cols, h)["r2"]
                metrics["val_r2"] = evaluate(model, val, feature_cols, h)["r2"]
                mlflow.log_params({"horizon_h": h, **model.get_params()})
                mlflow.log_metrics(metrics)

                registered_name = f"gridcast-lgbm-{h}h"
                signature = infer_signature(train[feature_cols], model.predict(train[feature_cols]))
                logged_model = mlflow.lightgbm.log_model(
                    model,
                    name="model",
                    signature=signature,
                    registered_model_name=registered_name,
                )
                client.transition_model_version_stage(
                    name=registered_name,
                    version=logged_model.registered_model_version,
                    stage="Staging",
                )
                logger.success(f"{h}h model: RMSE={metrics['rmse']:.1f} MAE={metrics['mae']:.1f}")


if __name__ == "__main__":
    app()
