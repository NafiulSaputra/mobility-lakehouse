from __future__ import annotations

from mobility_lakehouse.cli import build_parser
from mobility_lakehouse.gold import COMPANIES, company_name


def test_known_licensees_map_to_company_names() -> None:
    assert company_name("HV0003") == "Uber"
    assert company_name("HV0005") == "Lyft"
    assert set(COMPANIES) == {"HV0002", "HV0003", "HV0004", "HV0005"}


def test_unknown_licensee_is_kept_with_its_code() -> None:
    assert company_name("HV0099") == "Other (HV0099)"
    assert company_name(None) == "Unknown"


def test_cli_accepts_the_gold_command() -> None:
    args = build_parser().parse_args(["gold", "--month", "2025-01"])
    assert args.command == "gold"
