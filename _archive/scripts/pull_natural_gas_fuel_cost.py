"""Pull EIA average natural gas cost per BTU for electric power generation, by state,
2023-01 -> present.

electricity/electric-power-operational-data, cost-per-btu metric, fueltypeid=NG,
sectorid=98 (Electric Power). Not a PJM source and not one of gridstatus's 5 hardcoded
get_dataset() routes - calls gridstatus's EIA._fetch_page() directly with a manually
built request instead (reuses its auth/pagination handling rather than a new HTTP
client). Single request covers the whole range - ~2,500 rows, far under the 5,000/page
limit - but pagination is still handled in case that changes.
"""

from datetime import UTC, datetime
import json
import os

from dotenv import load_dotenv
import gridstatus as gs
import pandas as pd

from gridcast.config import INTERIM_DATA_DIR

load_dotenv()

OUT = INTERIM_DATA_DIR / "eia_natural_gas_fuel_cost_2023_present.csv"

eia = gs.EIA(api_key=os.environ["EIA_API_KEY"])

url = f"{eia.BASE_URL}electricity/electric-power-operational-data/data/"
params = {
    "start": "2023-01",
    "end": datetime.now(UTC).strftime("%Y-%m"),
    "frequency": "monthly",
    "data": ["cost-per-btu"],
    "facets": {"fueltypeid": ["NG"], "sectorid": ["98"]},
    "offset": 0,
    "length": 5000,
    "sort": [{"column": "period", "direction": "asc"}],
}

frames = []
total = None
while total is None or params["offset"] < total:
    headers = {"X-Api-Key": eia.api_key, "X-Params": json.dumps(params)}
    chunk, total = eia._fetch_page(url, headers)
    frames.append(chunk)
    print(f"offset {params['offset']}: {len(chunk)} rows (total {total})", flush=True)
    params["offset"] += params["length"]

fuel_cost = pd.concat(frames, ignore_index=True)
fuel_cost = fuel_cost.drop_duplicates(subset=["period", "location"])
fuel_cost.to_csv(OUT, index=False)
print(f"\n{len(fuel_cost)} rows -> {OUT}")
