import os
from urllib.parse import quote

from dotenv import load_dotenv
from loguru import logger
import requests

from gridcast.config import setup_logging

load_dotenv()
setup_logging()

URL = os.environ["OPENMETADATA_URL"] + "/api/v1"
HEADERS = {"Authorization": f"Bearer {os.environ['OPENMETADATA_JWT_TOKEN']}"}
PATCH_HEADERS = {**HEADERS, "Content-Type": "application/json-patch+json"}
DB = "gridcast-timescaledb.gridcast"
MLFLOW = "gridcast-mlflow"

CLASSIFICATIONS = {
    "Source": {
        "description": "Where the data originates.",
        "tags": {
            "PJM": "PJM Data Miner 2.",
            "EIA": "EIA Open Data API v2 (EIA-930 and Form EIA-923).",
            "Open-Meteo": "Open-Meteo forecast and historical-forecast APIs.",
            "FracTracker": "FracTracker Alliance's public data center tracker. Citation required in public-facing use.",
            "HIFLD": "HIFLD transmission lines and substations.",
            "GridSFM": "Microsoft GridSFM OSM-derived PJM bus/branch network.",
        },
    },
    "Layer": {
        "description": "Position in the pipeline.",
        "tags": {
            "raw": "Landed from a source, unmodified.",
            "reference": "Static or slowly changing lookup/graph tables.",
            "staging": "dbt staging models: cleaned raw data.",
            "feature": "dbt feature tables model inputs are read from.",
            "monitoring": "Model output and drift tracking.",
        },
    },
}

# table (schema.name) -> source tags; Layer tags come from LAYERS and the dbt naming rules
SOURCES = {
    "public.load": ["PJM", "EIA"],
    "public.lmp": ["PJM"],
    "public.instantaneous_load": ["PJM"],
    "public.weather": ["Open-Meteo"],
    "public.datacenters": ["FracTracker"],
    "raw_lmp.da_hrl_lmps_nodal": ["PJM"],
    "raw_lmp.rt_hrl_lmps_nodal": ["PJM"],
    "raw_lmp.lmp_da_hourly": ["PJM"],
    "raw_lmp.marginal_value_da": ["PJM"],
    "raw_lmp.marginal_value_rt": ["PJM"],
    "raw_lmp.transmission_constraints_da": ["PJM"],
    "raw_lmp.generation_by_fuel": ["PJM"],
    "raw_lmp.generation_ehv_losses": ["PJM"],
    "raw_lmp.scheduled_generation": ["PJM"],
    "raw_lmp.operator_initiated_commitments": ["PJM"],
    "raw_lmp.forecasted_generation_outages": ["PJM"],
    "raw_lmp.natural_gas_fuel_cost": ["EIA"],
    "raw_lmp.transmission_nodes": ["HIFLD"],
    "raw_lmp.transmission_edges": ["HIFLD"],
    "raw_lmp.facilities": ["PJM", "HIFLD"],
    "raw_lmp.pnode_topology_nodes": ["PJM", "HIFLD", "GridSFM"],
    "raw_lmp.pnode_topology_edges": ["PJM", "HIFLD", "GridSFM"],
}

REFERENCE = {
    "public.datacenters", "raw_lmp.transmission_nodes", "raw_lmp.transmission_edges", "raw_lmp.facilities",
    "raw_lmp.pnode_topology_nodes", "raw_lmp.pnode_topology_edges", "analytics.pjm_weather_zones",
}

TIER1_TABLES = {"analytics.features", "public.load", "raw_lmp.rt_hrl_lmps_nodal", "raw_lmp.da_hrl_lmps_nodal"}
TIER2_TABLES = {"analytics.load_features", "analytics.weather_features", "analytics.lmp_features", "analytics.inst_load_features"}
MODELS = ["gridcast-lgbm-1h", "gridcast-lgbm-24h", "gridcast-lgbm-72h"]

PRODUCT = {
    "name": "LMP-Model-Inputs",
    "displayName": "LMP Model Inputs",
    "description": "dbt staging models in analytics_lmp that feed the pnode-level LMP model (GNN): cleaned PJM and EIA series at their native grain.",
    "domains": ["LMP"],
}

MANAGED = ("Source.", "Layer.", "Tier.")


def layer(name: str) -> str:
    if name in REFERENCE:
        return "reference"
    if name == "public.prediction_log":
        return "monitoring"
    schema, table = name.split(".")
    if schema in ("public", "raw_lmp"):
        return "raw"
    return "staging" if table.startswith("stg_") else "feature"


def label(tag_fqn: str) -> dict:
    return {"tagFQN": tag_fqn, "source": "Classification", "labelType": "Manual", "state": "Confirmed"}


def desired_tags(name: str) -> list:
    tags = [f"Source.{source}" for source in SOURCES.get(name, [])] + [f"Layer.{layer(name)}"]
    if name in TIER1_TABLES:
        tags.append("Tier.Tier1")
    if name in TIER2_TABLES:
        tags.append("Tier.Tier2")
    return tags


def set_tags(endpoint: str, fqn: str, tags: list) -> dict:
    response = requests.get(f"{URL}/{endpoint}/name/{quote(fqn, safe='')}", headers=HEADERS, params={"fields": "tags"})
    response.raise_for_status()
    entity = response.json()

    kept = [t for t in entity.get("tags", []) if not t["tagFQN"].startswith(MANAGED)]
    patch = [{"op": "add", "path": "/tags", "value": kept + [label(t) for t in tags]}]
    response = requests.patch(f"{URL}/{endpoint}/{entity['id']}", headers=PATCH_HEADERS, json=patch)
    response.raise_for_status()
    return entity


def main() -> None:
    for classification, spec in CLASSIFICATIONS.items():
        response = requests.put(f"{URL}/classifications", headers=HEADERS, json={"name": classification, "description": spec["description"]})
        response.raise_for_status()
        for tag, description in spec["tags"].items():
            response = requests.put(f"{URL}/tags", headers=HEADERS, json={"classification": classification, "name": tag, "description": description})
            response.raise_for_status()

    response = requests.get(f"{URL}/tables", headers=HEADERS, params={"service": "gridcast-timescaledb", "limit": 100})
    response.raise_for_status()
    for table in response.json()["data"]:
        name = table["fullyQualifiedName"].removeprefix(f"{DB}.")
        set_tags("tables", table["fullyQualifiedName"], desired_tags(name))
        logger.info(f"{name}: {desired_tags(name)}")

    for model in MODELS:
        set_tags("mlmodels", f"{MLFLOW}.{model}", ["Tier.Tier1"])

    response = requests.put(f"{URL}/dataProducts", headers=HEADERS, json=PRODUCT)
    response.raise_for_status()
    response = requests.get(f"{URL}/tables", headers=HEADERS, params={"service": "gridcast-timescaledb", "limit": 100})
    staging = [
        {"id": t["id"], "type": "table"}
        for t in response.json()["data"]
        if t["fullyQualifiedName"].startswith(f"{DB}.analytics_lmp.")
    ]
    response = requests.put(f"{URL}/dataProducts/{PRODUCT['name']}/assets/add", headers=HEADERS, json={"assets": staging})
    response.raise_for_status()
    logger.info(f"{PRODUCT['name']}: {len(staging)} assets")

    logger.success("OpenMetadata tags and data product written")


if __name__ == "__main__":
    main()
