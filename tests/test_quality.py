from __future__ import annotations

import re
from pathlib import Path

from mobility_lakehouse.cli import build_parser
from mobility_lakehouse.quality import RULES, Severity, rules_of

ADR_0006 = Path(__file__).parents[1] / "docs" / "adr" / "0006-data-quality-rules-and-silver-schema.md"


def test_rule_ids_are_unique() -> None:
    ids = [r.id for r in RULES]
    assert len(ids) == len(set(ids))


def test_seven_reject_rules_and_five_warnings() -> None:
    assert [r.id for r in rules_of(Severity.REJECT)] == [f"Q00{i}" for i in range(1, 8)]
    assert [r.id for r in rules_of(Severity.WARNING)] == [f"W00{i}" for i in range(1, 6)]


def test_code_and_adr_0006_list_the_same_rules() -> None:
    """The ADR is the specification. If a rule changes, both must change together."""
    documented = set(re.findall(r"^\| ([QW]\d{3}) \|", ADR_0006.read_text(encoding="utf-8"), re.MULTILINE))
    assert documented == {r.id for r in RULES}


def test_every_rule_has_a_description_and_condition() -> None:
    for rule in RULES:
        assert rule.description.strip()
        assert rule.condition.strip()


def test_cli_accepts_the_silver_command() -> None:
    args = build_parser().parse_args(["silver", "--month", "2025-01"])
    assert args.command == "silver"
