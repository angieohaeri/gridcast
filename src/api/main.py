from contextlib import asynccontextmanager
from datetime import datetime
import os

from fastapi import FastAPI, HTTPException
import lightgbm as lgb
from loguru import logger
from pydantic import BaseModel

from gridcast.config import setup_logging
from gridcast.dataset import latest_features
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


@app.get("/health")
def health():
    return {"status": "ok", "horizons": list(models.keys())}


@app.get("/predict", response_model=list[ZonePrediction])
def get_predictions(zone: str | None = None):
    df = latest_features(zone=zone)
    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data for zone '{zone}'")
    return predict(df, models).to_dict(orient="records")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=os.getenv("API_HOST", "0.0.0.0"), port=int(os.getenv("API_PORT", "8000")))
