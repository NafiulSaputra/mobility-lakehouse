# API image (ADR 0011): FastAPI + DuckDB, no Spark and no Java. Build and run through compose.yaml.
FROM python:3.12-slim-trixie

COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /uvx /usr/local/bin/

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:${PATH}"

WORKDIR /app

# Dependencies first, so this layer is cached until uv.lock changes. Only the api group: no test tools.
COPY pyproject.toml uv.lock .python-version README.md ./
RUN uv sync --locked --no-default-groups --group api --no-install-project

COPY src ./src
RUN uv sync --locked --no-default-groups --group api --no-editable

# Run as a normal user; the snapshot is mounted read-only at /data/snapshot.
RUN useradd --create-home api
USER api
ENV MOBILITY_SNAPSHOT_DIR=/data/snapshot

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health')"

CMD ["uvicorn", "--factory", "mobility_lakehouse.api.app:create_app", "--host", "0.0.0.0", "--port", "8000"]
