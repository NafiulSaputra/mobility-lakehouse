"""Fixtures for tests that need a real Spark session with Delta Lake.

These tests run inside Docker (`docker compose run --rm spark pytest -m spark`) and in the CI Spark job.
Each test module calls pytest.importorskip, so they are skipped where PySpark is not installed.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    from mobility_lakehouse.local_spark import create_local_spark

    session = create_local_spark("mobility-lakehouse-tests", driver_memory="1g")
    session.conf.set("spark.sql.shuffle.partitions", "2")
    yield session
    session.stop()
