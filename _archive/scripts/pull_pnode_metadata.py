"""Pull PJM's pnode list (Data Miner 2 `pnode` feed), keeping pnodes active today.

pnode_type is only BUS / AGGREGATE; the useful split is pnode_subtype (ZONE, HUB,
INTERFACE, EHV, LOAD, GEN, ...). BUS pnode_name is fixed-width: substation code (8 chars)
+ voltage + equipment.

Output: data/external/pjm_pnodes.parquet
"""

import logging
import os

from dotenv import load_dotenv
import gridstatus as gs
from loguru import logger
import pandas as pd

from gridcast.config import EXTERNAL_DATA_DIR, setup_logging

load_dotenv()
setup_logging()
logging.getLogger("gridstatus").setLevel(logging.WARNING)

pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"])
raw = pjm._get_pjm_json("pnode", start=None, end=None, params={})

today = pd.Timestamp.now(tz="UTC").normalize()
eff = pd.to_datetime(raw["effective_date"], utc=True, errors="coerce")
term = pd.to_datetime(raw["termination_date"], utc=True, errors="coerce")
active = raw[(eff <= today) & (term.isna() | (term > today))]
logger.info(f"{len(raw)} pnodes, {len(active)} active: {active.groupby(['pnode_type', 'pnode_subtype']).size().to_dict()}")

active.to_parquet(EXTERNAL_DATA_DIR / "pjm_pnodes.parquet")
logger.success(f"saved {EXTERNAL_DATA_DIR / 'pjm_pnodes.parquet'}")
