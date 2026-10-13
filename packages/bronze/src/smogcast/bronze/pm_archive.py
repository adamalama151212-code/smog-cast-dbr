"""GIOŚ archive xlsx (wide: one column per measurement position) → bronze.pm_hourly (long).

Two phases:
1. Python (driver): stream the sheet with openpyxl in read-only mode and write a long,
   gzip-compressed CSV ``position_code, time_cet_end, value`` to landing/gios_archive/long/.
   Spark cannot read xlsx natively; a yearly sheet (~10 MB, ~1.6 M cells) streams in seconds
   with constant memory. Empty cells (no measurement) are skipped and counted.
2. Spark: read the long CSV, derive station/pollutant from the position code, convert time
   with the shared helper from smogcast.core.timeutil and overwrite the year's slice.

Bronze does not correct values. The only normalisation is parsing numbers (a decimal comma
in older files would otherwise turn into NULL) and rounding the spreadsheet timestamp noise.
"""

from __future__ import annotations

import csv
import datetime as dt
import gzip
import logging
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from smogcast.core.schema import GIOS_POSITION_ROW_LABEL
from smogcast.core.timeutil import cet_hour_end_to_utc_start_col, round_to_hour

log = logging.getLogger(__name__)

POSITION_ROW_LABEL = GIOS_POSITION_ROW_LABEL
LONG_HEADER = ["position_code", "time_cet_end", "value"]
LONG_SCHEMA = "position_code STRING, time_cet_end TIMESTAMP, value DOUBLE"


@dataclass
class SheetStats:
    positions: int = 0
    hours: int = 0
    values: int = 0
    empty: int = 0
    unparseable: int = 0


def _to_number(v) -> float | None:
    """Parse a cell value; None for empty or non-numeric cells."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).strip().replace(",", "."))
    except ValueError:
        return None


def sheet_to_long_csv(xlsx_path: Path, out_path: Path) -> SheetStats:
    """Convert one GIOŚ hourly sheet to a long CSV (gzip). Header rows are found by label,
    not by position, so a changed number of metadata rows does not break parsing."""
    import openpyxl  # heavy import, only needed in this phase

    stats = SheetStats()
    wb = openpyxl.load_workbook(xlsx_path, read_only=True)
    ws = wb.active
    positions: list[str] | None = None
    tmp = out_path.with_suffix(out_path.suffix + ".part")
    with gzip.open(tmp, "wt", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(LONG_HEADER)
        for row in ws.iter_rows(values_only=True):
            first = row[0]
            if positions is None:
                if first == POSITION_ROW_LABEL:
                    positions = [str(c) if c is not None else None for c in row[1:]]
                    stats.positions = sum(p is not None for p in positions)
                continue
            if not isinstance(first, dt.datetime):
                continue  # trailing notes or blank rows
            ts = round_to_hour(first).strftime("%Y-%m-%d %H:%M:%S")
            stats.hours += 1
            for code, raw in zip(positions, row[1:], strict=False):
                if code is None:
                    continue
                value = _to_number(raw)
                if value is None:
                    if raw is None or str(raw).strip() == "":
                        stats.empty += 1
                    else:
                        stats.unparseable += 1
                    continue
                w.writerow([code, ts, repr(value)])
                stats.values += 1
    wb.close()
    if positions is None:
        tmp.unlink(missing_ok=True)
        raise ValueError(f"{xlsx_path.name}: no '{POSITION_ROW_LABEL}' header row found")
    tmp.replace(out_path)  # only a complete file is ever visible as cached
    return stats


def long_csv_path(long_dir: str | Path, year: int, archive_token: str) -> Path:
    return Path(long_dir) / f"{year}_{archive_token}_1g.csv.gz"


def extract_year(zip_file: Path, year: int, archive_tokens: list[str], long_dir: Path) -> dict[str, SheetStats]:
    """Convert the hourly PM sheets of one yearly zip; already converted sheets are skipped."""
    long_dir.mkdir(parents=True, exist_ok=True)
    out: dict[str, SheetStats] = {}
    with zipfile.ZipFile(zip_file) as z, tempfile.TemporaryDirectory() as tmpdir:
        names = {n.lower(): n for n in z.namelist()}
        for token in archive_tokens:
            target = long_csv_path(long_dir, year, token)
            if target.exists() and target.stat().st_mtime >= zip_file.stat().st_mtime:
                log.info("%s: already converted", target.name)
                continue
            member = names.get(f"{year}_{token}_1g.xlsx".lower())
            if member is None:
                raise FileNotFoundError(f"{zip_file.name}: no {year}_{token}_1g.xlsx inside")
            # openpyxl needs a seekable file; extracting to a temp dir is simpler than seeking in the zip.
            xlsx = Path(z.extract(member, tmpdir))
            out[token] = sheet_to_long_csv(xlsx, target)
            log.info("%s: %s", target.name, out[token])
    return out


def read_long(spark: SparkSession, paths: list[str]) -> DataFrame:
    """Long CSVs → bronze rows. Position code format: <station>-<pollutant>-<averaging>,
    e.g. 'MpKrakAlKras-PM2.5-1g' (the pollutant itself may contain a dot, never a dash)."""
    parts = F.split(F.col("position_code"), "-")
    return (
        spark.read.schema(LONG_SCHEMA).option("header", True).csv(paths)
        .withColumn("station_code", F.element_at(parts, 1))
        .withColumn("pollutant", F.element_at(parts, 2))
        .withColumn("averaging", F.element_at(parts, 3))
        .withColumn("time_utc", cet_hour_end_to_utc_start_col(F.col("time_cet_end")))
        .withColumn("source_file", F.col("_metadata.file_path"))
        # Year of the archive file, from its name (<year>_<token>_1g.csv.gz). Not from the timestamp:
        # the last hour of a year is stamped "1 January 00:00" of the next year (end of hour).
        .withColumn("source_year", F.regexp_extract("source_file", r"(\d{4})_[^/]*$", 1).cast("int"))
        .withColumn("ingest_ts", F.current_timestamp())
        .select("station_code", "pollutant", "averaging", "position_code", "time_utc", "time_cet_end",
                "value", "source_year", "source_file", "ingest_ts")
    )
