"""Snapshots what the live Production models predict, to `public.prediction_log`.

`/history` re-scores the past with *today's* models, so it can't tell you how the
model that was actually serving last week performed, and error-over-time (concept
drift) is invisible. This flow writes a durable row per (zone, horizon) each day
after `dbt_build`, tagged with the model version that produced it. A later job can
join it against settled `demand_mw` for a true served-model accuracy history.

Daily is the right cadence: `analytics.features` only advances as load settles
(~72h), so `latest_features()` gives one genuinely forward-looking forecast per day
(the 72h horizon targets ~now); 1h/24h mostly target already-settled hours.
"""

from datetime import UTC, datetime, timedelta

from loguru import logger
from mlflow.tracking import MlflowClient
import pandas as pd
import typer

from gridcast.config import get_connection
from gridcast.dataset import latest_features
from gridcast.features import horizons
from gridcast.modeling.predict import load_models, predict
from prefect import flow

app = typer.Typer()

UPSERT = """
INSERT INTO prediction_log
    (feature_time, target_time, zone, horizon_h, predicted_mw,
     model_name, model_version, model_run_id, featureset)
VALUES
    (%(feature_time)s, %(target_time)s, %(zone)s, %(horizon_h)s, %(predicted_mw)s,
     %(model_name)s, %(model_version)s, %(model_run_id)s, %(featureset)s)
ON CONFLICT (target_time, zone, horizon_h) DO UPDATE SET
    predicted_at  = now(),
    feature_time  = EXCLUDED.feature_time,
    predicted_mw  = EXCLUDED.predicted_mw,
    model_name    = EXCLUDED.model_name,
    model_version = EXCLUDED.model_version,
    model_run_id  = EXCLUDED.model_run_id,
    featureset    = EXCLUDED.featureset;
"""


def production_model_meta() -> dict[int, dict]:
    """Registry metadata for each horizon's current Production model - version,
    source run, and the run's featureset tag - to stamp on every logged row."""
    client = MlflowClient()
    meta = {}
    for h in horizons:
        name = f"gridcast-lgbm-{h}h"
        version = client.get_latest_versions(name, stages=["Production"])[0]
        run = client.get_run(version.run_id)
        meta[h] = {
            "model_name": name,
            "model_version": version.version,
            "model_run_id": version.run_id,
            "featureset": run.data.tags.get("featureset", ""),
        }
    return meta


def prediction_rows(preds: pd.DataFrame, meta: dict[int, dict]) -> list[dict]:
    """Long form: one row per (zone, horizon) from a wide `predict()` frame
    (columns time, zone, y_1h, y_24h, y_72h). target_time is feature_time + horizon."""
    rows = []
    for h in horizons:
        for r in preds.itertuples():
            rows.append(
                {
                    "feature_time": r.time,
                    "target_time": r.time + timedelta(hours=h),
                    "zone": r.zone,
                    "horizon_h": h,
                    "predicted_mw": float(getattr(r, f"y_{h}h")),
                    **meta[h],
                }
            )
    return rows


def write_prediction_log(rows: list[dict]) -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.executemany(UPSERT, rows)
    conn.close()


@app.command()
@flow(
    name="prediction_log",
    description="Snapshots current Production-model predictions per zone/horizon to prediction_log.",
    log_prints=True,
)
def main():
    models = load_models()
    meta = production_model_meta()

    df = latest_features()
    df["time"] = pd.to_datetime(df["time"], utc=True)
    preds = predict(df, models)

    rows = prediction_rows(preds, meta)
    write_prediction_log(rows)
    logger.success(
        f"Logged {len(rows)} predictions at {datetime.now(UTC):%Y-%m-%d %H:%M} UTC "
        f"({preds['zone'].nunique()} zones x {len(horizons)} horizons)"
    )


if __name__ == "__main__":
    app()
