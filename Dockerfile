FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# pg_dump + rclone for the db_backup flow - not in the slim base image
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-client rclone \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY gridcast/ gridcast/
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"

COPY src/ src/

# dbt_packages/ is gitignored, so it may or may not exist in the build context -
# install the packages.yml dependencies here rather than depending on a copy.
RUN dbt deps --project-dir src/dbt
