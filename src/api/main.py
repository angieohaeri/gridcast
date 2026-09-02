from contextlib import asynccontextmanager
from datetime import datetime
import os
from threading import Lock

from cachetools import TTLCache, cached
from fastapi import FastAPI, HTTPException
import lightgbm as lgb
from loguru import logger
import pandas as pd
from pydantic import BaseModel

from gridcast.config import setup_logging
from gridcast.dataset import (
    features_window,
    inst_load_window,
    latest_features,
    latest_inst_load_time,
    recent_peak,
)
from gridcast.modeling.predict import load_models, predict

setup_logging()

models: dict[int, lgb.LGBMRegressor] = {}

# Shared across every dashboard session/device hitting these endpoints, so a burst of
# concurrent page loads pays for one DB query + inference pass, not one each. TTLs match
# the dashboard's own reactive.invalidate_later intervals, so caching adds no extra staleness.
_cache_lock = Lock()
_predict_cache: TTLCache = TTLCache(maxsize=32, ttl=300)
_history_cache: TTLCache = TTLCache(maxsize=32, ttl=300)
_inst_load_cache: TTLCache = TTLCache(maxsize=32, ttl=300)
_peak_cache: TTLCache = TTLCache(maxsize=32, ttl=3600)
_freshness_cache: TTLCache = TTLCache(maxsize=1, ttl=300)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Loading models from MLflow registry...")
    models.update(load_models())
    logger.success(f"Loaded models for horizons: {list(models.keys())}")
    yield
    models.clear()


app = FastAPI(title="gridcast", lifespan=lifespan)


class ZonePrediction(BaseModel):
    time: datetime
    zone: str
    y_1h: float
    y_24h: float
    y_72h: float
    temperature: float | None
    precipitation: float | None
    wind_speed: float | None
    cloud_cover: float | None


class HistoryPoint(BaseModel):
    time: datetime
    zone: str
    horizon_h: int
    actual_mw: float | None
    predicted_mw: float
    inst_load_mw: float | None


class InstLoadPoint(BaseModel):
    time: datetime
    zone: str
    inst_load_mw: float


class ZonePeak(BaseModel):
    zone: str
    peak_mw: float


class Freshness(BaseModel):
    latest_inst_load_time: datetime | None


@app.get("/health")
def health():
    return {"status": "ok", "horizons": list(models.keys())}


@cached(cache=_predict_cache, lock=_cache_lock)
def _predictions_cached(zone: str | None) -> list[dict]:
    df = latest_features(zone=zone)
    if df.empty:
        return []

    weather_cols = ["time", "zone", "temperature", "precipitation", "wind_speed", "cloud_cover"]
    preds = predict(df, models).merge(df[weather_cols], on=["time", "zone"], how="left")
    for col in weather_cols[2:]:
        preds[col] = preds[col].astype(object).where(preds[col].notna(), None)
    return preds.to_dict(orient="records")


@app.get("/predict", response_model=list[ZonePrediction])
def get_predictions(zone: str | None = None):
    records = _predictions_cached(zone)
    if not records:
        raise HTTPException(status_code=404, detail=f"No data for zone '{zone}'")
    return records


@cached(cache=_history_cache, lock=_cache_lock)
def _history_cached(zone: str | None, hours: int) -> list[dict]:
    # fetch extra lookback so every horizon's shifted actual-lookup (time + h) has a
    # real row to match against, not just rows shifted from inside the caller's own
    # `hours` window - otherwise actual_mw reads as a fake gap near the start of the
    # window at large horizons, since there's nothing old enough to shift forward
    # into that range. Callers already re-trim to their own display window downstream
    # (e.g. the dashboard's zone_history()), so the extra rows are harmless there.
    max_horizon = max(models) if models else 0
    df = features_window(hours=hours + max_horizon, zone=zone)
    if df.empty:
        return []

    preds = predict(df, models)
    actual = df[["time", "zone", "demand_mw", "inst_load_mw"]]

    aligned = []
    for h in models:
        target = preds[["time", "zone", f"y_{h}h"]].rename(columns={f"y_{h}h": "predicted_mw"})
        target["time"] = target["time"] + pd.Timedelta(hours=h)
        target = target.merge(actual, on=["time", "zone"], how="left")
        target["horizon_h"] = h
        aligned.append(target.rename(columns={"demand_mw": "actual_mw"}))

    result = pd.concat(aligned, ignore_index=True)
    for col in ["actual_mw", "inst_load_mw"]:
        result[col] = result[col].astype(object).where(result[col].notna(), None)
    return result.to_dict(orient="records")


@app.get("/history", response_model=list[HistoryPoint])
def get_history(zone: str | None = None, hours: int = 48):
    records = _history_cached(zone, hours)
    if not records:
        raise HTTPException(status_code=404, detail=f"No data for zone '{zone}'")
    return records


@cached(cache=_inst_load_cache, lock=_cache_lock)
def _inst_load_cached(zone: str | None, hours: int) -> list[dict]:
    df = inst_load_window(hours=hours, zone=zone)
    return df.to_dict(orient="records")


@app.get("/inst_load_history", response_model=list[InstLoadPoint])
def get_inst_load_history(zone: str | None = None, hours: int = 48):
    return _inst_load_cached(zone, hours)


@cached(cache=_peak_cache, lock=_cache_lock)
def _recent_peak_cached(zone: str | None, days: int) -> list[dict]:
    df = recent_peak(days=days, zone=zone)
    return df.to_dict(orient="records")


@app.get("/peak", response_model=list[ZonePeak])
def get_recent_peak(zone: str | None = None, days: int = 30):
    records = _recent_peak_cached(zone, days)
    if not records:
        raise HTTPException(status_code=404, detail=f"No data for zone '{zone}'")
    return records


@cached(cache=_freshness_cache, lock=_cache_lock)
def _latest_inst_load_time_cached() -> pd.Timestamp | None:
    return latest_inst_load_time()


@app.get("/freshness", response_model=Freshness)
def get_freshness():
    return {"latest_inst_load_time": _latest_inst_load_time_cached()}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=os.getenv("API_HOST", "0.0.0.0"), port=int(os.getenv("API_PORT", "8000")))
