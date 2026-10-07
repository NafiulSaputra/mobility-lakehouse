# Local Spark + Delta Lake environment, pinned to match Databricks serverless (see ADR 0005).
# Build and run through compose.yaml at the repository root.

# Python 3.12 to match Databricks serverless environment 5. Debian trixie provides Java 21,
# the default JDK of Databricks Runtime 18.
FROM python:3.12-slim-trixie

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /uvx /usr/local/bin/

# The virtual environment lives outside /workspace, so mounting the repository does not hide it.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:${PATH}"

WORKDIR /workspace

# Dependencies first, so this layer is cached until uv.lock changes.
COPY pyproject.toml uv.lock .python-version README.md ./
RUN uv sync --locked --group spark --no-install-project

COPY src ./src
RUN uv sync --locked --group spark

# Download the Delta Lake jars once at build time, so containers start fast and work offline.
RUN python -c "from mobility_lakehouse.local_spark import create_local_spark; create_local_spark().stop()"

CMD ["bash"]
