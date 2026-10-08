"""HTTP API that serves gold metrics and data quality from the Parquet snapshot (ADR 0011).

Run it with ``uvicorn --factory mobility_lakehouse.api.app:create_app`` or ``docker compose up api``.
"""
