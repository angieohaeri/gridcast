FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# pg_dump + rclone for the db_backup flow - not in the slim base image
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-client rclone \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY gridcast/ gridcast/
# --compile-bytecode: bakes .pyc files into this layer at build time. Without it,
# every container recreate (fresh writable layer, nothing persists) recompiles
# pandas/mlflow's many submodules from scratch on first import - normally a minor
# one-time cost, but under disk I/O contention from the rest of the stack on this
# host it stretched into minutes and looked identical to a startup deadlock.
RUN uv sync --frozen --no-dev --compile-bytecode

ENV PATH="/app/.venv/bin:$PATH"

COPY src/ src/

# dbt_packages/ is gitignored, so it may or may not exist in the build context -
# install the packages.yml dependencies here rather than depending on a copy.
RUN dbt deps --project-dir src/dbt
