"""Smoke test: the package installs and imports.

Real pipeline tests (idempotency, quality rules) arrive with the pipeline code in Sprints 2 and 3.
"""

import mobility_lakehouse


def test_package_imports() -> None:
    assert mobility_lakehouse.__name__ == "mobility_lakehouse"
