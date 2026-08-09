import os
from pathlib import Path
import sys

from dotenv import load_dotenv
from loguru import logger
import psycopg2

# Load environment variables from .env file if it exists
load_dotenv()

# Paths
PROJ_ROOT = Path(__file__).resolve().parents[1]
logger.info(f"PROJ_ROOT path is: {PROJ_ROOT}")

DATA_DIR = PROJ_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
INTERIM_DATA_DIR = DATA_DIR / "interim"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
EXTERNAL_DATA_DIR = DATA_DIR / "external"

MODELS_DIR = PROJ_ROOT / "models"

REPORTS_DIR = PROJ_ROOT / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"

def setup_logging():
    """Initializes and customizes global Loguru configuration."""
    # Remove the default standard error sink to prevent duplicate logs
    logger.remove()

    # 1. Console Handler (Colorful, clean, for development)
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="DEBUG",
        enqueue=True  # Thread-safe and asynchronous
    )
    logger.add(level="INFO", sink=sys.stderr, colorize=True, format="<fg #FFA500>{time: YYYY-MM-DD HH:mm:ss}</fg #FFA500>"
    " | <level>{message}</level>")

def get_connection():
    conn = psycopg2.connect(
        host=os.environ["TIMESCALEDB_HOST"],
        port=os.environ["TIMESCALEDB_PORT"],
        dbname=os.environ["TIMESCALEDB_DB"],
        user=os.environ["TIMESCALEDB_USER"],
        password=os.environ["TIMESCALEDB_PASSWORD"],
    )
    conn.autocommit = True
    return conn

# If tqdm is installed, configure loguru with tqdm.write
# https://github.com/Delgan/loguru/issues/135
try:
    from tqdm import tqdm

    logger.remove(0)
    logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)
except ModuleNotFoundError:
    pass
