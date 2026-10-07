from __future__ import annotations

import pytest

from mobility_lakehouse.bronze import month_predicate
from mobility_lakehouse.cli import build_parser
from mobility_lakehouse.contracts import (
    BRONZE_METADATA,
    HVFHV_SOURCE,
    SchemaContractError,
    check_source_columns,
    ddl,
)
from mobility_lakehouse.tlc import Month

ALL_SOURCE_COLUMNS = [c.name for c in HVFHV_SOURCE]


def test_contract_has_the_25_published_columns() -> None:
    assert len(HVFHV_SOURCE) == 25
    assert len(set(ALL_SOURCE_COLUMNS)) == 25


def test_complete_file_has_nothing_missing() -> None:
    assert check_source_columns(ALL_SOURCE_COLUMNS) == []


def test_file_before_2025_misses_only_the_congestion_fee() -> None:
    columns_2024 = [c for c in ALL_SOURCE_COLUMNS if c != "cbd_congestion_fee"]
    assert check_source_columns(columns_2024) == ["cbd_congestion_fee"]


def test_unknown_column_stops_the_load() -> None:
    with pytest.raises(SchemaContractError, match="surprise_fee"):
        check_source_columns([*ALL_SOURCE_COLUMNS, "surprise_fee"])


def test_column_order_in_the_file_does_not_matter() -> None:
    assert check_source_columns(list(reversed(ALL_SOURCE_COLUMNS))) == []


def test_ddl() -> None:
    assert ddl(BRONZE_METADATA) == "`_source_file` string, `_ingested_at` timestamp, `data_month` date"


def test_month_predicate_selects_one_partition() -> None:
    assert month_predicate(Month.parse("2025-01")) == "data_month = DATE'2025-01-01'"


def test_cli_accepts_the_bronze_command() -> None:
    args = build_parser().parse_args(["bronze", "--month", "2024-06"])
    assert args.command == "bronze"
    assert str(args.month) == "2024-06"
