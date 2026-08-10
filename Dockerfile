FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY gridcast/ gridcast/
RUN uv sync --frozen --no-dev

COPY src/ src/

ENV PATH="/app/.venv/bin:$PATH"
