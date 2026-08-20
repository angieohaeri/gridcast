import os

from loguru import logger
import requests

from gridcast.config import setup_logging
from prefect import flow

setup_logging()

API_URL = os.getenv("API_URL", "http://localhost:8000")
# same trailing window the dashboard's system_history() reactive.calc requests -
# ACCURACY_WINDOW_HOURS (72) + max(HORIZONS) (72), see src/dashboard/app.py
HISTORY_HOURS = 144


@flow(
    name="api_ping",
    description="Hits the api container's DB/model-backed routes so their working "
    "set stays resident between dashboard visits, instead of getting swapped out.",
    log_prints=True,
)
def main():
    for path, params in (
        ("/health", None),
        ("/predict", None),
        ("/history", {"hours": HISTORY_HOURS}),
    ):
        response = requests.get(f"{API_URL}{path}", params=params, timeout=30)
        response.raise_for_status()
        logger.info(f"pinged {path}: {response.status_code}")


if __name__ == "__main__":
    main()
