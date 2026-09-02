"""Data, target, and prediction drift monitoring for the load models.

Compares the trailing 30 days of `analytics.features` against the same 30 calendar
days one year earlier - a seasonally matched reference, so a flag means a real regime
change (load growth, new data-center load) rather than summer-vs-winter. Alert-only:
every run logs a report to the `gridcast-drift` MLflow experiment and emits a loguru
warning when drift is widespread; nothing retrains automatically.

Hand-rolled PSI + KS rather than Evidently: the current Evidently pins `plotly<6` and
the dashboard runs plotly 6. See references/decisions.md (Monitoring, 2026-09-02).
"""

from datetime import UTC, datetime

from loguru import logger
import mlflow
import numpy as np
import pandas as pd
from scipy.stats import ks_2samp
import typer

from gridcast.dataset import dataset
from gridcast.features import horizons
from gridcast.modeling.predict import load_models, predict
from prefect import flow

app = typer.Typer()

# Trailing window scored on each run; the reference is this same span shifted back a year.
current_window_days = 30

# Columns worth watching. zone and the calendar/cyclical features are left out - they
# can't drift by construction. inst_load_* usually reports "skipped" until a full year
# of history accrues (the feed only retains ~30 days; backfill started 2026-07).
feature_cols = [
    "demand_lag_72h",
    "demand_lag_168h",
    "demand_roll_6h",
    "demand_roll_24h",
    "lmp_total_lag_48h",
    "congestion_price_lag_48h",
    "marginal_loss_price_lag_48h",
    "temperature",
    "precipitation",
    "wind_speed",
    "cloud_cover",
    "inst_load_lag_1h",
    "inst_load_lag_3h",
    "inst_load_lag_24h",
]

# PSI convention: <0.1 stable, 0.1-0.25 moderate, >0.25 a real shift.
psi_moderate = 0.1
psi_significant = 0.25

# Share of scored features that must shift before a run warns.
drift_share_alert = 0.3

# Share of zones whose demand_mw must shift before a run warns - a broad load-level
# move (data-center buildout, electrification) rather than one noisy zone.
zone_drift_share_alert = 0.5

# Per column, per side; below this the column reports "skipped" rather than a noisy stat.
min_samples = 100


def psi(reference: pd.Series, current: pd.Series, bins: int = 10) -> float:
    """Population Stability Index between two samples, binned on reference deciles."""
    ref = reference.dropna().to_numpy()
    cur = current.dropna().to_numpy()
    if len(ref) == 0 or len(cur) == 0:
        return float("nan")

    edges = np.unique(np.quantile(ref, np.linspace(0, 1, bins + 1)))
    if len(edges) < 3:
        # quantile bins collapse on a spike-heavy distribution (precipitation is
        # mostly exact zeros) - fall back to uniform-width bins over the pooled range
        lo, hi = min(ref.min(), cur.min()), max(ref.max(), cur.max())
        if lo == hi:
            return 0.0  # genuinely constant on both sides
        edges = np.linspace(lo, hi, bins + 1)
    edges = edges.astype(float)
    edges[0], edges[-1] = -np.inf, np.inf

    ref_frac = np.histogram(ref, bins=edges)[0] / len(ref)
    cur_frac = np.histogram(cur, bins=edges)[0] / len(cur)

    eps = 1e-6
    ref_frac = np.clip(ref_frac, eps, None)
    cur_frac = np.clip(cur_frac, eps, None)
    return float(np.sum((cur_frac - ref_frac) * np.log(cur_frac / ref_frac)))


def drift_row(column: str, reference: pd.Series, current: pd.Series) -> dict:
    """PSI + two-sample KS for one column. `status='skipped'` when either side is too thin
    to score (the inst_load_* features, until a year of history exists)."""
    n_ref = int(reference.notna().sum())
    n_cur = int(current.notna().sum())
    if n_ref < min_samples or n_cur < min_samples:
        return {
            "column": column,
            "psi": None,
            "ks_statistic": None,
            "ks_pvalue": None,
            "n_reference": n_ref,
            "n_current": n_cur,
            "status": "skipped",
            "drifted": False,
        }

    ks_statistic, ks_pvalue = ks_2samp(reference.dropna(), current.dropna())
    score = psi(reference, current)
    return {
        "column": column,
        "psi": score,
        "ks_statistic": float(ks_statistic),
        "ks_pvalue": float(ks_pvalue),
        "n_reference": n_ref,
        "n_current": n_cur,
        "status": "ok",
        "drifted": bool(score >= psi_significant),
    }


def drift_table(reference: pd.DataFrame, current: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(drift_row(c, reference[c], current[c]) for c in columns)


def per_zone_target_drift(reference: pd.DataFrame, current: pd.DataFrame) -> pd.DataFrame:
    """demand_mw drift per zone - a pooled score hides a shift confined to one zone
    (a single data center coming online, say), and zone is a model feature."""
    rows = []
    for zone in sorted(current["zone"].unique()):
        row = drift_row(
            zone,
            reference.loc[reference["zone"] == zone, "demand_mw"],
            current.loc[current["zone"] == zone, "demand_mw"],
        )
        row["zone"] = row.pop("column")
        rows.append(row)
    return pd.DataFrame(rows)


def _psi_bar(feature_table: pd.DataFrame):
    import plotly.graph_objects as go

    scored = feature_table[feature_table["status"] == "ok"].sort_values("psi")
    fig = go.Figure(
        go.Bar(
            x=scored["psi"],
            y=scored["column"],
            orientation="h",
            marker_color=["#d62728" if d else "#1f77b4" for d in scored["drifted"]],
        )
    )
    fig.add_vline(x=psi_moderate, line_dash="dot", line_color="#999999")
    fig.add_vline(x=psi_significant, line_dash="dash", line_color="#d62728")
    fig.update_layout(
        title="Feature drift (PSI) - current 30d vs. the same window one year ago",
        xaxis_title="PSI",
        height=420,
        margin={"l": 150, "r": 30, "t": 50, "b": 40},
    )
    return fig


@app.command()
@flow(name="drift", description="Data/target/prediction drift report for the load models.", log_prints=True)
def main():
    df = dataset()
    df["time"] = pd.to_datetime(df["time"], utc=True)

    # anchor on the newest feature row, not now() - analytics.features trails ~72h
    # behind wall-clock while load settles.
    anchor = df["time"].max()
    current_start = anchor - pd.Timedelta(days=current_window_days)
    reference_end = anchor - pd.DateOffset(years=1)
    reference_start = current_start - pd.DateOffset(years=1)

    current = df[df["time"] > current_start].copy()
    reference = df[(df["time"] > reference_start) & (df["time"] <= reference_end)].copy()
    logger.info(
        f"current {current_start:%Y-%m-%d}..{anchor:%Y-%m-%d} ({len(current)} rows) vs. "
        f"reference {reference_start:%Y-%m-%d}..{reference_end:%Y-%m-%d} ({len(reference)} rows)"
    )

    features = drift_table(reference, current, feature_cols)
    target = drift_table(reference, current, ["demand_mw"])
    per_zone = per_zone_target_drift(reference, current)

    models = load_models()
    pred_cols = [f"y_{h}h" for h in horizons]
    predictions = drift_table(
        predict(reference.copy(), models), predict(current.copy(), models), pred_cols
    )

    scored = features[features["status"] == "ok"]
    drift_share = float(scored["drifted"].mean()) if len(scored) else 0.0
    target_psi = target.loc[0, "psi"]
    pred_psi = dict(zip(predictions["column"], predictions["psi"]))

    zones_scored = per_zone[per_zone["status"] == "ok"]
    zone_drift_share = float(zones_scored["drifted"].mean()) if len(zones_scored) else 0.0

    detected = bool(
        drift_share >= drift_share_alert
        or zone_drift_share >= zone_drift_share_alert
        or (target_psi is not None and target_psi >= psi_significant)
        or any(v is not None and v >= psi_significant for v in pred_psi.values())
    )

    mlflow.set_experiment("gridcast-drift")
    with mlflow.start_run(run_name=f"drift_{datetime.now(UTC):%Y%m%d}"):
        mlflow.log_params(
            {
                "reference_window": f"{reference_start:%Y-%m-%d}:{reference_end:%Y-%m-%d}",
                "current_window": f"{current_start:%Y-%m-%d}:{anchor:%Y-%m-%d}",
                "n_reference_rows": len(reference),
                "n_current_rows": len(current),
                "psi_significant": psi_significant,
                "drift_share_alert": drift_share_alert,
            }
        )
        mlflow.log_metrics(
            {
                "feature_drift_share": drift_share,
                "n_features_drifted": int(scored["drifted"].sum()),
                "n_features_scored": len(scored),
                "zone_drift_share": zone_drift_share,
                "n_zones_drifted": int(zones_scored["drifted"].sum()),
                "n_zones_scored": len(zones_scored),
                "target_psi": float(target_psi) if target_psi is not None else float("nan"),
                **{
                    f"pred_psi_{h}h": float(pred_psi[f"y_{h}h"])
                    for h in horizons
                    if pred_psi.get(f"y_{h}h") is not None
                },
            }
        )
        mlflow.set_tag("drift_detected", detected)
        mlflow.log_table(features, artifact_file="feature_drift.json")
        mlflow.log_table(target, artifact_file="target_drift.json")
        mlflow.log_table(per_zone, artifact_file="per_zone_target_drift.json")
        mlflow.log_table(predictions, artifact_file="prediction_drift.json")
        mlflow.log_figure(_psi_bar(features), "feature_psi.html")

    drifted = features[features["drifted"]]
    target_txt = f"{target_psi:.3f}" if target_psi is not None else "n/a"
    logger.info(
        f"{len(drifted)}/{len(scored)} features drifted (share={drift_share:.2f}); "
        f"{int(zones_scored['drifted'].sum())}/{len(zones_scored)} zones' demand_mw drifted "
        f"(share={zone_drift_share:.2f}); pooled target PSI={target_txt}"
    )
    # KS p-values saturate at this sample size, so the summary shows the KS statistic
    # (max CDF gap) as the effect size; PSI is what the drift flag keys on.
    for row in drifted.itertuples():
        logger.info(f"  {row.column}: PSI={row.psi:.3f} (KS D={row.ks_statistic:.3f})")
    for row in per_zone[per_zone["drifted"]].itertuples():
        logger.info(f"  zone {row.zone}: demand_mw PSI={row.psi:.3f}")

    if detected:
        logger.warning(
            "Drift detected - review the latest gridcast-drift run in MLflow and "
            "consider an out-of-cycle `train` run"
        )
    else:
        logger.success("No significant drift")


if __name__ == "__main__":
    app()
