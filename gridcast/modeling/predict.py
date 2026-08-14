import lightgbm as lgb
from loguru import logger
import mlflow.lightgbm
import pandas as pd
import typer

from gridcast.dataset import latest_features
from gridcast.features import build_features, horizons

app = typer.Typer()


def load_models(stage: str = "Production") -> dict[int, lgb.LGBMRegressor]:
    return {h: mlflow.lightgbm.load_model(f"models:/gridcast-lgbm-{h}h/{stage}") for h in horizons}


def predict(df: pd.DataFrame, models: dict[int, lgb.LGBMRegressor]) -> pd.DataFrame:
    df = build_features(df)
    preds = df[["time", "zone"]].copy()
    for h, model in models.items():
        preds[f"y_{h}h"] = model.predict(df[model.feature_name_])
    return preds


@app.command()
def main():
    logger.info("Loading models from registry...")
    models = load_models()
    df = latest_features()
    preds = predict(df, models)
    logger.info(f"\n{preds}")


if __name__ == "__main__":
    app()
