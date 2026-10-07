"""Local Spark session with Delta Lake, for development and tests inside Docker.

On Databricks the runtime provides the Spark session, so this module is only used locally.
Versions are pinned to match Databricks serverless (see ADR 0005).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def create_local_spark(
    app_name: str = "mobility-lakehouse",
    driver_memory: str | None = None,
) -> SparkSession:
    """Create (or reuse) a local SparkSession with Delta Lake enabled.

    ``driver_memory`` defaults to the SPARK_DRIVER_MEMORY environment variable, then 3g.
    """
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    memory = driver_memory or os.environ.get("SPARK_DRIVER_MEMORY", "3g")
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", memory)
        # Interpret and display timestamps the same way on every machine.
        .config("spark.sql.session.timeZone", "UTC")
        # The default of 200 shuffle partitions is far too many for a single machine.
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
