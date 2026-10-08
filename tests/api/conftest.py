"""Fixtures for the API tests."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("duckdb")

from snapshot_data import write_snapshot  # noqa: E402


@pytest.fixture
def snapshot_dir(tmp_path: Path) -> Path:
    return write_snapshot(tmp_path / "snapshot")
