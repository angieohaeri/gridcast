import os

from dotenv import load_dotenv
from loguru import logger
import requests

from gridcast.config import setup_logging
from openmetadata_column_descriptions import DESCRIPTIONS

load_dotenv()
setup_logging()

URL = os.environ["OPENMETADATA_URL"] + "/api/v1"
HEADERS = {"Authorization": f"Bearer {os.environ['OPENMETADATA_JWT_TOKEN']}"}
SERVICE = "gridcast-timescaledb"
DATABASE = "gridcast"


def main() -> None:
    for name, descriptions in DESCRIPTIONS.items():
        response = requests.get(f"{URL}/tables/name/{SERVICE}.{DATABASE}.{name}", headers=HEADERS, params={"fields": "columns"})
        response.raise_for_status()
        table = response.json()

        patch = [
            {"op": "add", "path": f"/columns/{i}/description", "value": descriptions[column["name"]]}
            for i, column in enumerate(table["columns"])
            if column["name"] in descriptions
        ]
        response = requests.patch(
            f"{URL}/tables/{table['id']}",
            headers={**HEADERS, "Content-Type": "application/json-patch+json"},
            json=patch,
        )
        response.raise_for_status()
        logger.info(f"{name}: {len(patch)} column descriptions")

    logger.success("OpenMetadata column descriptions written")


if __name__ == "__main__":
    main()
