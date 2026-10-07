"""NYC TLC trip record source: months, file URLs and an idempotent download.

The TLC publishes one Parquet file per dataset per month. This module only knows how to name, find and
download those files. It has no Spark dependency, so it runs anywhere, including on Windows.
"""

from __future__ import annotations

import re
import shutil
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import BinaryIO

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Dataset key -> file name prefix used by the TLC.
DATASETS = {"fhvhv": "fhvhv_tripdata"}

_MONTH_PATTERN = re.compile(r"(\d{4})-(0[1-9]|1[0-2])")
_CHUNK_SIZE = 1024 * 1024


@dataclass(frozen=True, order=True)
class Month:
    """A calendar month, written as YYYY-MM."""

    year: int
    month: int

    @classmethod
    def parse(cls, text: str) -> Month:
        match = _MONTH_PATTERN.fullmatch(text.strip())
        if match is None:
            raise ValueError(f"invalid month {text!r}: expected YYYY-MM, for example 2025-01")
        return cls(int(match.group(1)), int(match.group(2)))

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"

    @property
    def start(self) -> date:
        """First day of the month."""
        return date(self.year, self.month, 1)

    @property
    def end(self) -> date:
        """First day of the next month (exclusive upper bound)."""
        return self.next().start

    def next(self) -> Month:
        if self.month == 12:
            return Month(self.year + 1, 1)
        return Month(self.year, self.month + 1)


def _prefix(dataset: str) -> str:
    try:
        return DATASETS[dataset]
    except KeyError:
        known = ", ".join(sorted(DATASETS))
        raise ValueError(f"unknown dataset {dataset!r}: expected one of {known}") from None


def file_name(dataset: str, month: Month) -> str:
    return f"{_prefix(dataset)}_{month}.parquet"


def file_url(dataset: str, month: Month) -> str:
    return f"{BASE_URL}/{file_name(dataset, month)}"


def local_path(data_dir: Path, dataset: str, month: Month) -> Path:
    """Where the raw file for one month is stored: <data_dir>/raw/<dataset>/<file name>."""
    return data_dir / "raw" / dataset / file_name(dataset, month)


@dataclass(frozen=True)
class DownloadResult:
    path: Path
    size_bytes: int
    downloaded: bool  # False when an identical file was already present


Opener = Callable[[urllib.request.Request], BinaryIO]


def _default_opener(request: urllib.request.Request) -> BinaryIO:
    return urllib.request.urlopen(request, timeout=60)


def remote_size(url: str, opener: Opener = _default_opener) -> int | None:
    """Size of the remote file from a HEAD request, or None if the server does not say."""
    with opener(urllib.request.Request(url, method="HEAD")) as response:
        length = response.headers.get("Content-Length")
    return int(length) if length is not None else None


def download_month(
    dataset: str,
    month: Month,
    data_dir: Path,
    opener: Opener = _default_opener,
) -> DownloadResult:
    """Download one month of raw data. Safe to run again.

    - If a local file with the same size as the remote file exists, nothing is downloaded.
    - The file is written to a temporary ``.part`` file first and renamed only when complete,
      so an interrupted download never leaves a truncated file under the final name.
    """
    url = file_url(dataset, month)
    target = local_path(data_dir, dataset, month)
    target.parent.mkdir(parents=True, exist_ok=True)

    expected = remote_size(url, opener)
    if target.exists() and expected is not None and target.stat().st_size == expected:
        return DownloadResult(target, expected, downloaded=False)

    partial = target.with_name(target.name + ".part")
    with opener(urllib.request.Request(url)) as response, partial.open("wb") as out:
        shutil.copyfileobj(response, out, _CHUNK_SIZE)

    size = partial.stat().st_size
    if expected is not None and size != expected:
        partial.unlink()
        raise OSError(f"incomplete download of {url}: got {size} bytes, expected {expected}")

    partial.replace(target)
    return DownloadResult(target, size, downloaded=True)
