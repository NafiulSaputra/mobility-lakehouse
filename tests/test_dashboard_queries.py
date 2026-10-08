"""The dashboard queries (ADR 0010) must stay in line with the tables and mappings the pipeline writes."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from mobility_lakehouse.gold import COMPANIES
from mobility_lakehouse.pipeline import catalog_layout
from mobility_lakehouse.quality import RULES

QUERIES = Path(__file__).resolve().parents[1] / "dashboards" / "queries"
QUERY_FILES = sorted(QUERIES.glob("*.sql"))
TABLE_REFERENCE = re.compile(r"\bworkspace\.mobility\.(\w+)")


def _pipeline_tables() -> set[str]:
    layout = catalog_layout("workspace", "mobility")
    tables = [layout.bronze, *vars(layout.silver).values(), *vars(layout.gold).values()]
    return {str(table) for table in tables}


def test_there_are_dashboard_queries() -> None:
    assert QUERY_FILES, f"no .sql files in {QUERIES}"


@pytest.mark.parametrize("query", QUERY_FILES, ids=lambda p: p.name)
def test_queries_only_read_tables_the_pipeline_writes(query: Path) -> None:
    referenced = {f"workspace.mobility.{name}" for name in TABLE_REFERENCE.findall(query.read_text())}

    unknown = referenced - _pipeline_tables()

    assert referenced, f"{query.name} reads no pipeline table"
    assert not unknown, f"unknown tables in {query.name}: {unknown}"


def test_quarantine_query_maps_every_company_like_gold() -> None:
    sql = (QUERIES / "quarantine_by_company.sql").read_text()

    for code, name in COMPANIES.items():
        assert f"WHEN '{code}' THEN '{name}'" in sql


def test_spike_rule_is_documented_in_the_adr() -> None:
    adr = (QUERIES.parents[1] / "docs" / "adr" / "0010-data-quality-dashboard.md").read_text()

    assert "twice the previous month" in adr
    assert "100 rows" in adr


def test_rule_ids_in_the_pipeline_are_short_codes() -> None:
    # The dashboard groups and colours by rule_id, so IDs must stay stable codes like Q004 or W003.
    assert all(re.fullmatch(r"[QW]\d{3}", rule.id) for rule in RULES)
