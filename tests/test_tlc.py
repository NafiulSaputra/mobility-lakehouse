from __future__ import annotations

import io
import urllib.request
from datetime import date
from pathlib import Path

import pytest

from mobility_lakehouse.tlc import (
    Month,
    download_month,
    file_name,
    file_url,
    local_path,
)


class TestMonth:
    def test_parse_and_format_round_trip(self) -> None:
        assert str(Month.parse("2025-01")) == "2025-01"

    @pytest.mark.parametrize("text", ["2025-1", "2025-13", "2025-00", "25-01", "2025/01", "", "2025-01-01"])
    def test_rejects_invalid_text(self, text: str) -> None:
        with pytest.raises(ValueError, match="YYYY-MM"):
            Month.parse(text)

    def test_bounds(self) -> None:
        month = Month.parse("2024-02")
        assert month.start == date(2024, 2, 1)
        assert month.end == date(2024, 3, 1)

    def test_next_rolls_over_the_year(self) -> None:
        assert Month.parse("2024-12").next() == Month(2025, 1)

    def test_ordering(self) -> None:
        assert Month.parse("2024-12") < Month.parse("2025-01")


def test_file_naming() -> None:
    month = Month.parse("2025-01")
    assert file_name("fhvhv", month) == "fhvhv_tripdata_2025-01.parquet"
    assert file_url("fhvhv", month).endswith("/trip-data/fhvhv_tripdata_2025-01.parquet")
    assert local_path(Path("data"), "fhvhv", month) == Path(
        "data", "raw", "fhvhv", "fhvhv_tripdata_2025-01.parquet"
    )


def test_unknown_dataset_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown dataset"):
        file_name("unicorns", Month.parse("2025-01"))


class FakeResponse(io.BytesIO):
    def __init__(self, body: bytes, content_length: int | None) -> None:
        super().__init__(body)
        self.headers = {} if content_length is None else {"Content-Length": str(content_length)}


class FakeServer:
    """Stands in for urllib: answers HEAD with a size and GET with a body."""

    def __init__(self, body: bytes, advertised_size: int | None = None) -> None:
        self.body = body
        self.advertised_size = len(body) if advertised_size is None else advertised_size
        self.get_requests = 0

    def __call__(self, request: urllib.request.Request) -> FakeResponse:
        if request.get_method() == "HEAD":
            return FakeResponse(b"", self.advertised_size)
        self.get_requests += 1
        return FakeResponse(self.body, self.advertised_size)


def test_download_writes_the_file(tmp_path: Path) -> None:
    server = FakeServer(b"parquet-bytes")
    result = download_month("fhvhv", Month.parse("2025-01"), tmp_path, opener=server)

    assert result.downloaded is True
    assert result.path.read_bytes() == b"parquet-bytes"
    assert not result.path.with_name(result.path.name + ".part").exists()


def test_download_is_idempotent(tmp_path: Path) -> None:
    server = FakeServer(b"parquet-bytes")
    month = Month.parse("2025-01")

    download_month("fhvhv", month, tmp_path, opener=server)
    second = download_month("fhvhv", month, tmp_path, opener=server)

    assert second.downloaded is False
    assert server.get_requests == 1


def test_download_replaces_a_changed_remote_file(tmp_path: Path) -> None:
    month = Month.parse("2025-01")
    download_month("fhvhv", month, tmp_path, opener=FakeServer(b"old"))

    result = download_month("fhvhv", month, tmp_path, opener=FakeServer(b"corrected file"))

    assert result.downloaded is True
    assert result.path.read_bytes() == b"corrected file"


def test_incomplete_download_leaves_no_file(tmp_path: Path) -> None:
    server = FakeServer(b"truncated", advertised_size=1000)

    with pytest.raises(OSError, match="incomplete download"):
        download_month("fhvhv", Month.parse("2025-01"), tmp_path, opener=server)

    folder = local_path(tmp_path, "fhvhv", Month.parse("2025-01")).parent
    assert list(folder.iterdir()) == []
