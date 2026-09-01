FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# pg_dump + rclone for the db_backup flow - not in the slim base image
# libgomp1 - OpenMP runtime required by lightgbm's compiled library
# curl - fetches mapbox-gl.js to self-host at build time, see below
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-client rclone libgomp1 curl \
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

# Self-host plotly.js instead of the dashboard iframes pulling it from cdn.plot.ly on
# every session - copied from the pinned plotly package so it's never out of sync with
# the version fig.to_html() actually renders against.
RUN cp "$(python -c 'import pathlib, plotly; print(pathlib.Path(plotly.__file__).parent / "package_data" / "plotly.min.js")')" \
    src/dashboard/www/plotly.min.js

# Same idea for the two largest CDN pulls pydeck's Deck.to_html() bakes into every map
# render (~5MB combined): the @deck.gl/jupyter-widget bundle ships inside the pydeck
# package itself (copied, no network needed), while mapbox-gl.js doesn't - so its exact
# URL is read out of the installed pydeck's own template (matches app.py's
# MAPBOX_GL_CDN_PATTERN) and fetched once here instead of by every dashboard visitor.
RUN cp "$(python -c 'import pathlib, pydeck; print(pathlib.Path(pydeck.__file__).parent / "nbextension" / "static" / "index.js")')" \
    src/dashboard/www/deckgl-widget.js \
    && curl -fsSL "$(grep -oE 'https://api\.tiles\.mapbox\.com/mapbox-gl-js/[^\"]+/mapbox-gl\.js' \
        "$(python -c 'import pathlib, pydeck; print(pathlib.Path(pydeck.__file__).parent / "io" / "templates" / "index.j2")')")" \
       -o src/dashboard/www/mapbox-gl.js
