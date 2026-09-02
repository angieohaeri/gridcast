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
from gridcast.features import build_features, build_labels, horizons
from prefect import flow

app = typer.Typer()

featureset = "v2"

# val/test are fixed-size windows relative to the most recent row (not wall-clock now
# - load settles 2-3 days late). train is everything older, so it grows over time.
test_months = 7
val_months = 12

# not model features: time is a split key not a predictor, demand_mw is the
# unlagged/raw target, observation_count is a weather data-quality diagnostic
non_features = ["time", "demand_mw", "observation_count"]


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

    return {
        "rmse": float(np.sqrt(np.mean(error**2))),
        "mae": float(np.mean(np.abs(error))),
        "r2": float(r2_score(df[y_col], preds)),
    }


def per_zone_metrics(model: lgb.LGBMRegressor, df: pd.DataFrame, feature_cols: list[str], h: int) -> pd.DataFrame:
    """Per-zone MAE/MAPE on the test split - an aggregate score can hide one bad zone,
    and zones vary a lot in size so MAPE normalizes for that. Logged as a table artifact
    (one row per zone) rather than flat metrics, so ~20 zones don't clutter the metrics
    tab with mae_<zone>/mape_<zone> entries alongside the handful of real metrics."""
    y_col = f"y_{h}h"
    df = df.dropna(subset=[y_col])
    preds = model.predict(df[feature_cols])
    error = preds - df[y_col].to_numpy()
    per_zone = df.assign(pred=preds, error=error)

    return pd.DataFrame(
        {
            "zone": zone,
            "mae": float(np.mean(np.abs(group["error"]))),
            "mape": float(np.mean(np.abs(group["error"] / group[y_col])) * 100),
        }
        for zone, group in per_zone.groupby("zone", observed=True)
    )


def promote_if_better(client: MlflowClient, registered_name: str, version: str, new_rmse: float) -> None:
    """Promotes a newly-registered version straight to Production if it beats (lower
    RMSE than) whatever's currently in Production - or if nothing's in Production yet.
    Otherwise leaves it in Staging for manual review."""
    current = client.get_latest_versions(registered_name, stages=["Production"])
    if current:
        current_rmse = client.get_run(current[0].run_id).data.metrics["test_rmse"]
        if new_rmse >= current_rmse:
            logger.info(
                f"{registered_name} v{version}: RMSE={new_rmse:.1f} vs Production "
                f"v{current[0].version} (RMSE={current_rmse:.1f}) - staying in Staging"
            )
            return

    client.transition_model_version_stage(
        name=registered_name, version=version, stage="Production", archive_existing_versions=True
    )
    logger.success(f"{registered_name} v{version} promoted to Production (RMSE={new_rmse:.1f})")


@app.command()
@flow(name="train", description="Trains LightGBM models per horizon and registers to MLflow.", log_prints=True)
def main():
    logger.info("Loading analytics.features...")
    df = build_labels(dataset())
    df = build_features(df)
    df = df[df["demand_lag_168h"].notna()]

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
                "source_table": "analytics.features",
                "train_date_range": f"{df['time'].min():%Y-%m-%d}:{val_start:%Y-%m-%d}",
                "val_date_range": f"{val_start:%Y-%m-%d}:{test_start:%Y-%m-%d}",
                "test_date_range": f"{test_start:%Y-%m-%d}:{df['time'].max():%Y-%m-%d}",
            }
        )
        mlflow.log_param("row_count", len(df))

        for h in horizons:
            logger.info(f"Training {h}h-ahead model...")
            with mlflow.start_run(run_name=f"{h}h", nested=True):
                model, feature_cols = train_horizon(train, val, h)
                metrics = {f"test_{k}": v for k, v in evaluate(model, test, feature_cols, h).items()}
                metrics["train_r2"] = evaluate(model, train, feature_cols, h)["r2"]
                metrics["val_r2"] = evaluate(model, val, feature_cols, h)["r2"]
                mlflow.log_params({"horizon_h": h, **model.get_params()})
                mlflow.log_metrics(metrics)
                mlflow.log_table(
                    per_zone_metrics(model, test, feature_cols, h), artifact_file="per_zone_metrics.json"
                )

                registered_name = f"gridcast-lgbm-{h}h"
                signature = infer_signature(train[feature_cols], model.predict(train[feature_cols]))
                logged_model = mlflow.lightgbm.log_model(
                    model,
                    name=f"model_{h}h",
                    signature=signature,
                    registered_model_name=registered_name,
                )
                client.transition_model_version_stage(
                    name=registered_name,
                    version=logged_model.registered_model_version,
                    stage="Staging",
                )
                logger.success(f"{h}h model: RMSE={metrics['test_rmse']:.1f} MAE={metrics['test_mae']:.1f}")
                promote_if_better(
                    client, registered_name, logged_model.registered_model_version, metrics["test_rmse"]
                )


if __name__ == "__main__":
    app()
