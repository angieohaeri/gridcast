import os
from urllib.parse import quote

from dotenv import load_dotenv
from loguru import logger
import requests

from gridcast.config import setup_logging

load_dotenv()
setup_logging()

URL = os.environ["OPENMETADATA_URL"]
HEADERS = {"Authorization": f"Bearer {os.environ['OPENMETADATA_JWT_TOKEN']}"}

DB = "gridcast-timescaledb.gridcast"
PREFECT = "gridcast-prefect"
KAFKA = "gridcast-kafka"
MLFLOW = "gridcast-mlflow"

# raw_lmp sync flows are named after the table they upsert, except the unverified one
RAW_LMP_SYNCS = {
    "marginal_value_rt": "marginal_value_rt",
    "marginal_value_da": "marginal_value_da",
    "lmp_da_hourly": "lmp_da_hourly",
    "generation_by_fuel": "generation_by_fuel",
    "transmission_constraints_da": "transmission_constraints_da",
    "operator_initiated_commitments": "operator_initiated_commitments",
    "scheduled_generation": "scheduled_generation",
    "generation_ehv_losses": "generation_ehv_losses",
    "forecasted_generation_outages": "forecasted_generation_outages",
    "natural_gas_fuel_cost": "natural_gas_fuel_cost",
    "rt_hrl_lmps_nodal": "rt_hrl_lmps_nodal",
    "rt_unverified_hrl_lmps_nodal": "rt_hrl_lmps_nodal",
    "da_hrl_lmps_nodal": "da_hrl_lmps_nodal",
}

# topic -> (producer flow, consumer flow, table the consumer writes)
KAFKA_PIPELINES = {
    "load": ("load_producer", "load_consumer", "load"),
    "lmp": ("lmp_producer", "lmp_consumer", "lmp"),
    "weather": ("weather_producer", "weather_consumer", "weather"),
    "inst_load": ("inst_load_producer", "inst_load_consumer", "instantaneous_load"),
}

MODELS = ["gridcast-lgbm-1h", "gridcast-lgbm-24h", "gridcast-lgbm-72h"]


def entity_ref(kind: str, endpoint: str, fqn: str) -> dict:
    response = requests.get(f"{URL}/api/v1/{endpoint}/name/{quote(fqn, safe='')}", headers=HEADERS)
    response.raise_for_status()
    return {"id": response.json()["id"], "type": kind}


def pipeline(name: str) -> dict:
    return entity_ref("pipeline", "pipelines", f"{PREFECT}.{name}")


def table(schema: str, name: str) -> dict:
    return entity_ref("table", "tables", f"{DB}.{schema}.{name}")


def topic(name: str) -> dict:
    return entity_ref("topic", "topics", f"{KAFKA}.{name}")


def add_edge(source: dict, target: dict, via: dict | None = None) -> None:
    edge = {"fromEntity": source, "toEntity": target}
    if via:
        edge["lineageDetails"] = {"pipeline": via, "source": "Manual"}
    response = requests.put(f"{URL}/api/v1/lineage", headers=HEADERS, json={"edge": edge})
    response.raise_for_status()


def main() -> None:
    for flow_name, table_name in RAW_LMP_SYNCS.items():
        add_edge(pipeline(flow_name), table("raw_lmp", table_name))

    for topic_name, (producer, consumer, table_name) in KAFKA_PIPELINES.items():
        add_edge(pipeline(producer), topic(topic_name))
        add_edge(topic(topic_name), table("public", table_name), via=pipeline(consumer))

    add_edge(pipeline("data_center_sync"), table("public", "datacenters"))
    add_edge(pipeline("prediction_log"), table("public", "prediction_log"))

    features = table("analytics", "features")
    for model in MODELS:
        add_edge(features, entity_ref("mlmodel", "mlmodels", f"{MLFLOW}.{model}"), via=pipeline("train"))

    logger.success("OpenMetadata lineage written")


if __name__ == "__main__":
    main()
