from loguru import logger
import pandas as pd

from gridcast.config import get_connection, setup_logging
from prefect import flow

setup_logging()

SHEET_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/1JJ6kcVo-NjlAYtznwHOki2DVl4WWV6lhy-eXhFCdKKU"
    "/gviz/tq?tqx=out:csv&gid=386766486"
)

# Kept as TEXT (not coerced to numeric/date at read time): the sheet mixes plain
# numbers with ranges ("100-200") and free text ("$14.5 billion", "Full buildout
# by 2037"). dtype=str also stops pandas from turning zip into a float and
# dropping the leading zero off New England codes (e.g. "08037" -> "8037").
TEXT_COLUMNS = [
    "facility_name",
    "address",
    "city",
    "state",
    "zip",
    "county",
    "location_confidence",
    "status",
    "expected_date_online",
    "mw",
    "sizerank",
    "operator_name",
    "facility_size_sqft",
    "property_size_acres",
    "project_cost",
]
COLUMNS = TEXT_COLUMNS + ["lat", "long", "date_created", "date_updated"]

INSERT = f"""
INSERT INTO datacenters ({", ".join(COLUMNS)})
VALUES ({", ".join(f"%({col})s" for col in COLUMNS)});
"""


@flow(
    name="data_center_sync",
    description="Pulls the FracTracker data center tracker sheet and appends a snapshot to datacenters.",
    log_prints=True,
)
def main():
    sheet = pd.read_csv(SHEET_CSV_URL, dtype={col: str for col in TEXT_COLUMNS})
    sheet = sheet[COLUMNS]
    sheet["date_created"] = pd.to_datetime(sheet["date_created"], format="%m/%d/%Y", errors="coerce").dt.date
    sheet["date_updated"] = pd.to_datetime(sheet["date_updated"], format="%m/%d/%Y", errors="coerce").dt.date
    sheet = sheet.where(sheet.notna(), None)

    conn = get_connection()
    cur = conn.cursor()
    cur.executemany(INSERT, sheet.to_dict(orient="records"))
    conn.close()

    logger.success(f"Appended a {len(sheet)}-row snapshot to datacenters")


if __name__ == "__main__":
    main()
