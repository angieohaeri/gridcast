import os

from loguru import logger
import requests

from gridcast.config import setup_logging
from prefect import flow

setup_logging()

DASHBOARD_URL = os.getenv("DASHBOARD_URL", "http://localhost:8080")


@flow(
    name="dashboard_ping",
    description="Hits the dashboard container's root page so its process stays "
    "resident between visits, instead of getting swapped out. A plain GET only "
    "fetches the static shell (Shiny sessions start over websocket on connect), "
    "so this doesn't trigger the reactive.calc chain in src/dashboard/app.py.",
    log_prints=True,
)
def main():
    response = requests.get(DASHBOARD_URL, timeout=30)
    response.raise_for_status()
    logger.info(f"pinged dashboard: {response.status_code}")


if __name__ == "__main__":
    main()
