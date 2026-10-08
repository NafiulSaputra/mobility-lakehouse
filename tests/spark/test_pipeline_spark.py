"""End to end: raw file -> bronze -> silver -> gold -> API snapshot through the shared steps (ADR 0009, 0011).

The same steps run on Databricks with Unity Catalog tables. Running them here with ``persist=False`` covers
the Databricks code path, where DataFrame caching is not available.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from factories import raw_frame, trip_row  # noqa: E402

from mobility_lakehouse.pipeline import (  # noqa: E402
    Layout,
    MissingInputError,
    local_layout,
    run_bronze,
    run_gold,
    run_silver,
    run_snapshot,
)
from mobility_lakehouse.tlc import Month  # noqa: E402

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = pytest.mark.spark

JAN = Month.parse("2025-01")
FEB = Month.parse("2025-02")
ZONES_CSV = '"LocationID","Borough","Zone","service_zone"\n' + "".join(
    f'{i},"Queens","Zone {i}","Boro Zone"\n' for i in range(1, 265)
)


def prepare_inputs(spark: SparkSession, base: Path, persist: bool) -> Layout:
    layout = dataclasses.replace(local_layout(base / "data", base / "lakehouse"), persist=persist)
    rows = [trip_row(JAN, n) for n in range(6)] + [trip_row(JAN, 6, base_passenger_fare=-1.0)]
    raw_frame(spark, rows).write.mode("errorifexists").parquet(layout.raw_file(JAN))
    zones = Path(layout.zones_file)
    zones.parent.mkdir(parents=True, exist_ok=True)
    zones.write_text(ZONES_CSV, encoding="utf-8")
    return layout


@pytest.mark.parametrize("persist", [True, False], ids=["local", "databricks-serverless"])
def test_raw_to_gold(spark: SparkSession, tmp_path: Path, persist: bool) -> None:
    layout = prepare_inputs(spark, tmp_path, persist)

    assert run_bronze(spark, layout, JAN) == 7
    silver = run_silver(spark, layout, JAN)
    gold = run_gold(spark, layout, JAN)

    assert (silver.silver_rows, silver.quarantined_rows) == (6, 1)
    assert gold.rows_per_table["monthly_driver_economics"] == 1
    assert gold.driver_economics[0]["company"] == "Uber"
    assert gold.driver_economics[0]["trips"] == 6


def test_a_step_refuses_to_run_before_its_input_exists(spark: SparkSession, tmp_path: Path) -> None:
    layout = prepare_inputs(spark, tmp_path, persist=True)
    run_bronze(spark, layout, JAN)

    with pytest.raises(MissingInputError, match="no bronze rows for 2025-02"):
        run_silver(spark, layout, FEB)


def test_snapshot_is_safe_to_rerun_and_readable_by_the_api(spark: SparkSession, tmp_path: Path) -> None:
    layout = prepare_inputs(spark, tmp_path, persist=False)
    run_bronze(spark, layout, JAN)
    run_silver(spark, layout, JAN)
    run_gold(spark, layout, JAN)

    first = run_snapshot(spark, layout, JAN)
    second = run_snapshot(spark, layout, JAN)

    assert first == second  # rerunning a month replaces it instead of adding rows
    assert (first["monthly_driver_economics"], first["quality_months"]) == (1, 1)

    # The API reads exactly what Spark wrote.
    store_module = pytest.importorskip("mobility_lakehouse.api.store")
    store = store_module.SnapshotStore(Path(layout.snapshot_dir))
    assert store.months() == [JAN.start]
    assert [(r["company"], r["trips"]) for r in store.driver_economics(JAN.start)] == [("Uber", 6)]
    quality = store.quality()
    assert (quality[0]["rows_checked"], quality[0]["rows_quarantined"]) == (7, 1)


def test_snapshot_refuses_to_run_before_gold(spark: SparkSession, tmp_path: Path) -> None:
    layout = prepare_inputs(spark, tmp_path, persist=True)
    run_bronze(spark, layout, JAN)
    run_silver(spark, layout, JAN)
    run_gold(spark, layout, JAN)

    with pytest.raises(MissingInputError, match="no gold rows for 2025-02"):
        run_snapshot(spark, layout, FEB)
