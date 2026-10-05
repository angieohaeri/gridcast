"""Loader for raw_lmp.lmp_da_hourly_nodal / raw_lmp.load_metered_by_area from Data
Miner 2's web-UI CSV export, NOT the API - this project's other backfills all go
through gridstatus/_get_pjm_json, but lmp_da_hourly_nodal's ~350-400M-row full-history
volume made the API path impractical (see decisions.md - "Nodal LMP granularity", a
single day's API pull already hit PJM's rate limiter). The user is instead exporting
CSVs by hand from the Data Miner 2 UI.

CSV headers confirmed 2026-10-04 directly against the live UI (dataminer2.pjm.com/feed/
da_hrl_lmps and .../hrl_load_metered), NOT guessed from the API's snake_case field
names - the UI's grid/export headers are human-readable and don't match 1:1 with either
this project's column names or the API fields documented elsewhere in this repo.

NOT RUN. Written as a reference/starting point for the user's manual load, same as
every other one-off script in this directory - the actual run (file path, table) is on
them. Two DB-shape gotchas the UI export doesn't resolve for you, both handled below:

1. zone - OK AS-IS for both feeds. hrl_load_metered's "Transmission Zone" column
   already uses this project's zone_id short codes (AE, AEP, PS, ...) directly, same as
   every other place in this repo. da_hrl_lmps's "Transmission Zone" column ALSO
   already uses them for the nodal export - confirmed live (PSEG/AECO/etc are only in
   the *filter dropdown*, not the data rows, which showed the short codes directly) -
   simpler than the API path, which needs LOCATION_TO_ZONE. Do not re-map zone names.

2. time - the UI only exports "Datetime Beginning UTC" (Interval Start), never an
   ending timestamp. Every other raw_lmp table in this project uses Hour Ending
   (Interval End) as `time`, to match public.lmp's convention - so this loader adds
   1 hour, same as the API-based flows in raw_lmp_sync.py do via Interval End.

3. da_hrl_lmps's export is NOT pre-filtered to one Pricing Node Type or one row
   version. Confirmed live: a single day with no filter returned 348,120 rows (every
   pnode type, including ZONE/HUB aggregates like PJM-RTO and MID-ATL/APS, and
   apparently multiple row versions for revised intervals); filtering the UI's
   "Pricing Node Type" column to LOAD before exporting brought that down to 283,032 -
   close to but not exactly 11,751 nodes x 24h (282,024), meaning some version
   duplication likely still got through. **Filter "Pricing Node Type" = LOAD in the UI
   before exporting** (matches this table's LOAD-only scope - see schema.md), and
   additionally de-dupe defensively here on (time, pnode_id), keeping the row with the
   highest "Version Number" - the UI has no equivalent of the API's row_is_current=TRUE
   filter, so don't trust the export to already be deduplicated.

Usage (not run by this script itself - call these from a one-off shell/REPL):
    load_lmp_da_hourly_nodal_csv("/path/to/dataminer_export.csv")
    load_load_metered_by_area_csv("/path/to/dataminer_export.csv")
"""

import pandas as pd
import psycopg2.extras

from gridcast.config import get_connection

# --- raw_lmp.lmp_da_hourly_nodal -------------------------------------------------------
# CSV headers confirmed live against dataminer2.pjm.com/feed/da_hrl_lmps, in this exact
# order: Datetime Beginning UTC, Datetime Beginning EPT, Pricing Node ID, Pricing Node
# Name, Voltage, Equipment, Pricing Node Type, Transmission Zone, System Energy Prices
# Day Ahead, Total LMP Day Ahead, Congestion Price Day Ahead, Marginal Loss Price Day
# Ahead, Latest Version, Version Number.
#
# "System Energy Prices Day Ahead", "Datetime Beginning EPT", "Latest Version" are not
# stored (not in this table's DDL) - Energy is derivable as lmp - congestion -
# marginal_loss if ever needed (same identity gridstatus itself uses), EPT is redundant
# with UTC, and Latest Version/Version Number are used only to de-dupe below, not kept.
LMP_DA_HOURLY_NODAL_COLUMN_MAP = {
    "Datetime Beginning UTC": "datetime_beginning_utc",  # transformed into `time` below, not stored as-is
    "Pricing Node ID": "pnode_id",
    "Pricing Node Name": "pnode_name",
    "Voltage": "voltage",
    "Equipment": "equipment",
    "Pricing Node Type": "pnode_type",
    "Transmission Zone": "zone",
    "Total LMP Day Ahead": "lmp",
    "Congestion Price Day Ahead": "congestion_price",
    "Marginal Loss Price Day Ahead": "marginal_loss_price",
    "Version Number": "version_nbr",  # dedup key only, not stored
}

LMP_DA_HOURLY_NODAL_UPSERT = """
INSERT INTO raw_lmp.lmp_da_hourly_nodal
    (time, pnode_id, pnode_name, pnode_type, zone, voltage, equipment, lmp, congestion_price, marginal_loss_price)
VALUES %s
ON CONFLICT (time, pnode_id) DO UPDATE SET
    pnode_name = EXCLUDED.pnode_name,
    pnode_type = EXCLUDED.pnode_type,
    zone = EXCLUDED.zone,
    voltage = EXCLUDED.voltage,
    equipment = EXCLUDED.equipment,
    lmp = EXCLUDED.lmp,
    congestion_price = EXCLUDED.congestion_price,
    marginal_loss_price = EXCLUDED.marginal_loss_price
"""


def load_lmp_da_hourly_nodal_csv(csv_path: str) -> int:
    raw = pd.read_csv(csv_path)
    raw = raw.rename(columns=LMP_DA_HOURLY_NODAL_COLUMN_MAP)

    # Hour Ending, matching every other raw_lmp table's `time` convention - the export
    # only gives Interval Start.
    raw["time"] = pd.to_datetime(raw["datetime_beginning_utc"], utc=True) + pd.Timedelta(hours=1)

    # Export isn't pre-deduplicated by version - keep the highest Version Number per
    # (time, pnode_id). If the UI's "Latest Version" filter was set to TRUE at export
    # time, version_nbr may be absent/constant - this is then a no-op.
    if "version_nbr" in raw.columns:
        raw = raw.sort_values("version_nbr").drop_duplicates(subset=["time", "pnode_id"], keep="last")
    else:
        raw = raw.drop_duplicates(subset=["time", "pnode_id"])

    raw = raw.where(raw.notna(), None)
    rows = list(
        raw[
            ["time", "pnode_id", "pnode_name", "pnode_type", "zone", "voltage", "equipment", "lmp", "congestion_price", "marginal_loss_price"]
        ].itertuples(index=False, name=None)
    )

    conn = get_connection()
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, LMP_DA_HOURLY_NODAL_UPSERT, rows, page_size=5000)
    conn.close()
    return len(rows)


# --- raw_lmp.load_metered_by_area ------------------------------------------------------
# CSV headers confirmed live against dataminer2.pjm.com/feed/hrl_load_metered, in this
# exact order: Datetime Beginning UTC, Datetime Beginning EPT, NERC Region, Market
# Region, Transmission Zone, Load Area, MW, Company Verified.
#
# "Datetime Beginning EPT", "NERC Region", "Market Region" are not stored (not in this
# table's DDL, same as load_producer.py's existing poll_pjm_load - this project never
# kept those fields for load). "Company Verified" is the UI's label for what the API
# calls `is_verified` - same field, different display name, no transform needed beyond
# the rename.
LOAD_METERED_BY_AREA_COLUMN_MAP = {
    "Datetime Beginning UTC": "datetime_beginning_utc",  # transformed into `time` below, not stored as-is
    "Transmission Zone": "zone",
    "Load Area": "load_area",
    "MW": "demand_mw",
    "Company Verified": "is_verified",
}

LOAD_METERED_BY_AREA_UPSERT = """
INSERT INTO raw_lmp.load_metered_by_area (time, zone, load_area, demand_mw, is_verified)
VALUES %s
ON CONFLICT (time, zone, load_area) DO UPDATE SET
    demand_mw = EXCLUDED.demand_mw,
    is_verified = EXCLUDED.is_verified
"""


def load_load_metered_by_area_csv(csv_path: str) -> int:
    raw = pd.read_csv(csv_path)
    raw = raw.rename(columns=LOAD_METERED_BY_AREA_COLUMN_MAP)

    raw["time"] = pd.to_datetime(raw["datetime_beginning_utc"], utc=True) + pd.Timedelta(hours=1)
    raw["is_verified"] = raw["is_verified"].astype(bool)
    raw = raw.where(raw.notna(), None)
    raw = raw.drop_duplicates(subset=["time", "zone", "load_area"])

    rows = list(raw[["time", "zone", "load_area", "demand_mw", "is_verified"]].itertuples(index=False, name=None))

    conn = get_connection()
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, LOAD_METERED_BY_AREA_UPSERT, rows, page_size=5000)
    conn.close()
    return len(rows)
