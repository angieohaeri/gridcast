import re

import pandas as pd

from gridcast.config import EXTERNAL_DATA_DIR

EIA_DIR = EXTERNAL_DATA_DIR / "eia860" / "eia8602025"
STATUS_GROUPS = {"Operable": "operable", "Proposed": "proposed", "Retired and Canceled": "retired_or_canceled"}
PLANT_COLUMNS = {
    "Plant Code": "plant_code",
    "Plant Name": "plant_name",
    "State": "state",
    "County": "county",
    "Latitude": "lat",
    "Longitude": "lon",
    "NERC Region": "nerc_region",
    "Balancing Authority Code": "balancing_authority_code",
    "Utility ID": "utility_id",
    "Utility Name": "utility_name",
    "Sector Name": "sector_name",
    "Transmission or Distribution System Owner": "transmission_owner",
    "Grid Voltage (kV)": "grid_voltage_kv",
    "Grid Voltage 2 (kV)": "grid_voltage_2_kv",
    "Grid Voltage 3 (kV)": "grid_voltage_3_kv",
}
GENERATOR_COLUMNS = {
    "Plant Code": "plant_code",
    "Generator ID": "generator_id",
    "Technology": "technology",
    "Prime Mover": "prime_mover",
    "Energy Source 1": "energy_source_1",
    "Nameplate Capacity (MW)": "nameplate_mw",
    "Summer Capacity (MW)": "summer_mw",
    "Winter Capacity (MW)": "winter_mw",
    "Status": "status",
    "Operating Year": "operating_year",
    "Planned Retirement Year": "planned_retirement_year",
    "Retirement Year": "retirement_year",
    "RTO/ISO LMP Node Designation": "lmp_node_designation",
}
NUMERIC = ["lat", "lon", "grid_voltage_kv", "grid_voltage_2_kv", "grid_voltage_3_kv", "nameplate_mw", "summer_mw", "winter_mw", "operating_year", "planned_retirement_year", "retirement_year"]


def read_eia860(pnodes: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    plants = pd.read_excel(EIA_DIR / "2___Plant_Y2025.xlsx", header=1)[list(PLANT_COLUMNS)].rename(columns=PLANT_COLUMNS)
    sheets = []
    for sheet, group in STATUS_GROUPS.items():
        table = pd.read_excel(EIA_DIR / "3_1_Generator_Y2025.xlsx", sheet_name=sheet, header=1, dtype={"RTO/ISO LMP Node Designation": str})
        table = table[[c for c in GENERATOR_COLUMNS if c in table.columns]].rename(columns=GENERATOR_COLUMNS)
        sheets.append(table.assign(status_group=group))
    generators = pd.concat(sheets, ignore_index=True)
    for column in NUMERIC:
        for frame in (plants, generators):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
    generators["lmp_node_designation"] = generators["lmp_node_designation"].str.strip()
    pnode_ids = set(pnodes["pnode_id"])
    designation = generators["lmp_node_designation"]
    numeric = pd.to_numeric(designation.str.extract(r"^(?:pnode:?\s*)?(\d{4,12})$", flags=re.IGNORECASE)[0])
    by_id = numeric.where(numeric.isin(pnode_ids))
    names = pnodes.dropna(subset=["name"]).assign(key=lambda d: d["name"].str.upper().str.strip()).drop_duplicates("key", keep=False)
    by_name = designation.str.upper().map(dict(zip(names["key"], names["pnode_id"])))
    generators["pnode_id"] = by_id.fillna(by_name).astype("Int64")
    generators["pnode_link"] = pd.Series("id", index=generators.index).where(by_id.notna(), pd.Series("name", index=generators.index).where(by_name.notna()))
    keep = set(plants.loc[plants["balancing_authority_code"] == "PJM", "plant_code"]) | set(generators.loc[generators["pnode_id"].notna(), "plant_code"])
    return plants[plants["plant_code"].isin(keep)], generators[generators["plant_code"].isin(keep)]
