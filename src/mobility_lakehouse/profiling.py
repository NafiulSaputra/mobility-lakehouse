"""Profile one month of HVFHV trip data and write a Markdown report.

The report answers the open questions in ADR 0003 and gives the facts needed to design the
quality rules in Sprint 3: is there a usable unique key, how many trips fall outside the file's
month, and how many rows contain impossible values.

``profile_month`` needs Spark. ``render_markdown`` and ``write_report`` are plain Python, so the
report format can be tested without Spark.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mobility_lakehouse.tlc import Month

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

NUMERIC_COLUMNS = (
    "trip_miles",
    "trip_time",
    "base_passenger_fare",
    "tolls",
    "bcf",
    "sales_tax",
    "congestion_surcharge",
    "airport_fee",
    "tips",
    "driver_pay",
    "cbd_congestion_fee",
)
FLAG_COLUMNS = (
    "shared_request_flag",
    "shared_match_flag",
    "access_a_ride_flag",
    "wav_request_flag",
    "wav_match_flag",
)
ZONE_COLUMNS = ("PULocationID", "DOLocationID")
# Taxi zone IDs in the TLC zone lookup run from 1 to 265.
MAX_ZONE_ID = 265
# Columns that would identify a trip if no two real trips shared them. Tested, not assumed.
CANDIDATE_KEY = (
    "hvfhs_license_num",
    "dispatching_base_num",
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
)
FLAG_VALUES_SHOWN = 10


@dataclass(frozen=True)
class DuplicateStats:
    """Rows that share the same hash over a set of columns."""

    groups: int  # distinct values that appear more than once
    rows: int  # rows belonging to those groups

    @property
    def extra_rows(self) -> int:
        """Rows that would be removed if every group kept exactly one row."""
        return self.rows - self.groups


@dataclass
class Profile:
    dataset: str
    month: str
    source_file: str
    generated_at: str
    row_count: int
    schema: list[tuple[str, str]]
    nulls: dict[str, int]
    checks: dict[str, int]
    pickup_range: tuple[str, str] | None
    numeric: dict[str, dict[str, Any]]
    zones_out_of_range: dict[str, int]
    flags: dict[str, list[tuple[str | None, int]]]
    full_row_duplicates: DuplicateStats
    candidate_key_duplicates: DuplicateStats | None
    notes: list[str] = field(default_factory=list)


def _duplicate_stats(df: DataFrame, columns: list[str]) -> DuplicateStats:
    from pyspark.sql import functions as F

    per_hash = df.groupBy(F.xxhash64(*[F.col(c) for c in columns]).alias("row_hash")).count()
    repeated = per_hash.where(F.col("count") > 1)
    # A global aggregate always returns exactly one row.
    row = repeated.agg(F.count(F.lit(1)).alias("groups"), F.sum("count").alias("rows")).first()
    return DuplicateStats(groups=int(row["groups"] or 0), rows=int(row["rows"] or 0))


def profile_month(df: DataFrame, dataset: str, month: Month, source_file: str) -> Profile:
    """Compute the profile of one month. Scans the data a few times; no data is cached."""
    from pyspark.sql import functions as F

    columns = df.columns
    present = set(columns)
    exprs = [F.count(F.lit(1)).alias("row_count")]
    exprs += [F.sum(F.col(c).isNull().cast("long")).alias(f"null__{c}") for c in columns]

    check_names: list[str] = []

    def add_check(name: str, condition: Any) -> None:
        check_names.append(name)
        exprs.append(F.sum(condition.cast("long")).alias(f"check__{name}"))

    has_pickup = "pickup_datetime" in present
    if has_pickup:
        pickup = F.col("pickup_datetime")
        add_check(
            "pickup_outside_file_month",
            (pickup < F.lit(month.start)) | (pickup >= F.lit(month.end)),
        )
        exprs += [F.min(pickup).alias("pickup_min"), F.max(pickup).alias("pickup_max")]
        if "dropoff_datetime" in present:
            dropoff = F.col("dropoff_datetime")
            add_check("dropoff_before_pickup", dropoff < pickup)
            add_check("dropoff_equals_pickup", dropoff == pickup)
        if "request_datetime" in present:
            add_check("pickup_before_request", pickup < F.col("request_datetime"))

    numeric_columns = [c for c in NUMERIC_COLUMNS if c in present]
    for c in numeric_columns:
        col = F.col(c)
        exprs += [
            F.min(col).alias(f"min__{c}"),
            F.max(col).alias(f"max__{c}"),
            F.sum((col < 0).cast("long")).alias(f"negative__{c}"),
            F.sum((col == 0).cast("long")).alias(f"zero__{c}"),
            F.percentile_approx(col, [0.5, 0.99]).alias(f"pct__{c}"),
        ]

    zone_columns = [c for c in ZONE_COLUMNS if c in present]
    for c in zone_columns:
        col = F.col(c)
        exprs.append(F.sum(((col < 1) | (col > MAX_ZONE_ID)).cast("long")).alias(f"zone__{c}"))

    # One pass over the data for all column-level metrics. A global aggregate is a single row.
    stats = df.agg(*exprs).first()

    flags: dict[str, list[tuple[str | None, int]]] = {}
    for c in (c for c in FLAG_COLUMNS if c in present):
        top = df.groupBy(c).count().orderBy(F.col("count").desc()).limit(FLAG_VALUES_SHOWN).collect()
        flags[c] = [(r[c], int(r["count"])) for r in top]

    key_columns = [c for c in CANDIDATE_KEY if c in present]
    notes = [
        "Duplicates are detected with a 64-bit hash (xxhash64). Different rows with the same hash are "
        "possible but extremely rare at this volume.",
    ]
    missing_key = sorted(set(CANDIDATE_KEY) - present)
    if missing_key:
        notes.append(f"Candidate key check skipped: missing columns {', '.join(missing_key)}.")

    numeric: dict[str, dict[str, Any]] = {}
    for c in numeric_columns:
        p50, p99 = stats[f"pct__{c}"] or (None, None)
        numeric[c] = {
            "min": stats[f"min__{c}"],
            "p50": p50,
            "p99": p99,
            "max": stats[f"max__{c}"],
            "negative": int(stats[f"negative__{c}"] or 0),
            "zero": int(stats[f"zero__{c}"] or 0),
        }

    return Profile(
        dataset=dataset,
        month=str(month),
        source_file=source_file,
        generated_at=datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
        row_count=int(stats["row_count"]),
        schema=[(f.name, f.dataType.simpleString()) for f in df.schema.fields],
        nulls={c: int(stats[f"null__{c}"] or 0) for c in columns},
        checks={name: int(stats[f"check__{name}"] or 0) for name in check_names},
        pickup_range=(str(stats["pickup_min"]), str(stats["pickup_max"])) if has_pickup else None,
        numeric=numeric,
        zones_out_of_range={c: int(stats[f"zone__{c}"] or 0) for c in zone_columns},
        flags=flags,
        full_row_duplicates=_duplicate_stats(df, columns),
        candidate_key_duplicates=None if missing_key else _duplicate_stats(df, key_columns),
        notes=notes,
    )


# ---------------------------------------------------------------------------------------------
# Report rendering: plain Python, no Spark.
# ---------------------------------------------------------------------------------------------

CHECK_LABELS = {
    "pickup_outside_file_month": "Pickup time outside the file's month",
    "dropoff_before_pickup": "Drop-off time before pickup time",
    "dropoff_equals_pickup": "Drop-off time equal to pickup time",
    "pickup_before_request": "Pickup time before request time",
}


def _pct(part: int, total: int) -> str:
    if total == 0:
        return "n/a"
    return f"{100 * part / total:.4f}%"


def _num(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, float):
        return f"{value:,.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def _table(header: list[str], rows: list[list[str]]) -> list[str]:
    lines = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return lines


def _duplicates_line(label: str, stats: DuplicateStats, total: int) -> str:
    return (
        f"- **{label}:** {stats.groups:,} values appear more than once, covering {stats.rows:,} rows "
        f"({stats.extra_rows:,} extra rows, {_pct(stats.extra_rows, total)} of all rows)."
    )


def render_markdown(profile: Profile) -> str:
    total = profile.row_count
    out = [
        f"# Profiling report: {profile.dataset} {profile.month}",
        "",
        f"Source file `{profile.source_file}` · generated {profile.generated_at} by "
        "`mobility-lakehouse profile`.",
        "",
        "## Summary",
        "",
        f"- Rows: **{total:,}**",
        f"- Columns: **{len(profile.schema)}**",
    ]
    if profile.pickup_range:
        out.append(f"- Pickup time range: {profile.pickup_range[0]} to {profile.pickup_range[1]}")

    out += ["", "## Uniqueness (input for ADR 0003)", ""]
    out.append(_duplicates_line("Identical full rows", profile.full_row_duplicates, total))
    if profile.candidate_key_duplicates is not None:
        key = ", ".join(f"`{c}`" for c in CANDIDATE_KEY)
        out.append(_duplicates_line(f"Candidate key ({key})", profile.candidate_key_duplicates, total))

    if profile.checks:
        out += ["", "## Timestamp checks", ""]
        out += _table(
            ["Check", "Rows", "Share"],
            [[CHECK_LABELS.get(k, k), f"{v:,}", _pct(v, total)] for k, v in profile.checks.items()],
        )

    if profile.numeric:
        out += ["", "## Numeric columns", ""]
        out += _table(
            ["Column", "Min", "Median (approx.)", "P99 (approx.)", "Max", "Negative", "Zero"],
            [
                [
                    f"`{c}`",
                    _num(m["min"]),
                    _num(m["p50"]),
                    _num(m["p99"]),
                    _num(m["max"]),
                    f"{m['negative']:,} ({_pct(m['negative'], total)})",
                    f"{m['zero']:,} ({_pct(m['zero'], total)})",
                ]
                for c, m in profile.numeric.items()
            ],
        )

    if profile.zones_out_of_range:
        out += ["", f"## Zone IDs outside 1-{MAX_ZONE_ID}", ""]
        out += _table(
            ["Column", "Rows", "Share"],
            [[f"`{c}`", f"{v:,}", _pct(v, total)] for c, v in profile.zones_out_of_range.items()],
        )

    if profile.flags:
        out += ["", "## Flag values", ""]
        for c, values in profile.flags.items():
            shown = ", ".join(f"`{v}`: {n:,}" for v, n in values)
            out.append(f"- `{c}`: {shown}")

    out += ["", "## Nulls per column", ""]
    out += _table(
        ["Column", "Nulls", "Share"],
        [[f"`{c}`", f"{n:,}", _pct(n, total)] for c, n in profile.nulls.items()],
    )

    out += ["", "## Schema", ""]
    out += _table(["Column", "Type"], [[f"`{name}`", f"`{dtype}`"] for name, dtype in profile.schema])

    if profile.notes:
        out += ["", "## Notes", ""]
        out += [f"- {note}" for note in profile.notes]

    return "\n".join(out) + "\n"


def report_path(report_dir: Path, dataset: str, month: Month) -> Path:
    return report_dir / f"{dataset}_{month}.md"


def write_report(profile: Profile, report_dir: Path) -> Path:
    path = report_path(report_dir, profile.dataset, Month.parse(profile.month))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_markdown(profile), encoding="utf-8", newline="\n")
    return path
