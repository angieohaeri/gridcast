from pathlib import Path

from loguru import logger
import numpy as np
import pandas as pd
from tqdm import tqdm
import typer

from gridcast.config import PROCESSED_DATA_DIR

app = typer.Typer()


def add_cyclical_features(df: pd.DataFrame, time_col: str="time") -> pd.DataFrame:
      hour = df[time_col].dt.hour
      month = df[time_col].dt.month

      df["hour_sin"] = np.sin(2 * np.pi * hour / 24)
      df["hour_cos"] = np.cos(2 * np.pi * hour / 24)
      df["month_sin"] = np.sin(2 * np.pi * (month - 1) / 12)
      df["month_cos"] = np.cos(2 * np.pi * (month - 1) / 12)

      return df

@app.command()
def main(
    # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
    input_path: Path = PROCESSED_DATA_DIR / "dataset.csv",
    output_path: Path = PROCESSED_DATA_DIR / "features.csv",
    # -----------------------------------------
):
    # ---- REPLACE THIS WITH YOUR OWN CODE ----
    logger.info("Generating features from dataset...")
    for i in tqdm(range(10), total=10):
        if i == 5:
            logger.info("Something happened for iteration 5.")
    logger.success("Features generation complete.")
    # -----------------------------------------


if __name__ == "__main__":
    app()
