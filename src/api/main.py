from contextlib import asynccontextmanager
from datetime import datetime
import os

from fastapi import FastAPI, HTTPException
import lightgbm as lgb
from loguru import logger
import pandas as pd
from pydantic import BaseModel

from gridcast.config import setup_logging
from gridcast.dataset import features_window, latest_features, latest_load_time, recent_peak
from gridcast.modeling.predict import load_models, predict

setup_logging()

models: dict[int, lgb.LGBMRegressor] = {}


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


class ZonePeak(BaseModel):
    zone: str
    peak_mw: float


class Freshness(BaseModel):
    latest_load_time: datetime | None


@app.get("/health")
def health():
    return {"status": "ok", "horizons": list(models.keys())}


@app.get("/predict", response_model=list[ZonePrediction])
def get_predictions(zone: str | None = None):
    df = latest_features(zone=zone)
    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data for zone '{zone}'")

    weather_cols = ["time", "zone", "temperature", "precipitation", "wind_speed", "cloud_cover"]
    preds = predict(df, models).merge(df[weather_cols], on=["time", "zone"], how="left")
    for col in weather_cols[2:]:
        preds[col] = preds[col].astype(object).where(preds[col].notna(), None)
    return preds.to_dict(orient="records")


@app.get("/history", response_model=list[HistoryPoint])
def get_history(zone: str | None = None, hours: int = 48):
    df = features_window(hours=hours, zone=zone)
    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data for zone '{zone}'")

    preds = predict(df, models)
    actual = df[["time", "zone", "demand_mw"]]

    aligned = []
    for h in models:
        target = preds[["time", "zone", f"y_{h}h"]].rename(columns={f"y_{h}h": "predicted_mw"})
        target["time"] = target["time"] + pd.Timedelta(hours=h)
        target = target.merge(actual, on=["time", "zone"], how="left")
        target["horizon_h"] = h
        aligned.append(target.rename(columns={"demand_mw": "actual_mw"}))

    result = pd.concat(aligned, ignore_index=True)
    result["actual_mw"] = result["actual_mw"].astype(object).where(result["actual_mw"].notna(), None)
    return result.to_dict(orient="records")


@app.get("/peak", response_model=list[ZonePeak])
def get_recent_peak(zone: str | None = None, days: int = 30):
    df = recent_peak(days=days, zone=zone)
    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data for zone '{zone}'")
    return df.to_dict(orient="records")


@app.get("/freshness", response_model=Freshness)
def get_freshness():
    return {"latest_load_time": latest_load_time()}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=os.getenv("API_HOST", "0.0.0.0"), port=int(os.getenv("API_PORT", "8000")))
