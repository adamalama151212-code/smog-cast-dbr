"""GIOŚ metadata xlsx → bronze.gios_stations and bronze.gios_positions.

The archive contains stations that were closed since (the current API no longer lists them),
so the station → city mapping for 2021–2025 must come from this file, not from the API.
Two sheets: stations (code, old code, town, coordinates, open/close dates) and measurement positions
(position code, pollutant, averaging, measurement type). Sheet and column names are the Polish labels of
the GIOŚ file and are kept verbatim in smogcast.core.schema (they are matched exactly); every column is
renamed to English on the way into bronze.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from smogcast.core.schema import GIOS_POSITION_COLUMNS, GIOS_STATION_COLUMNS

# Polish source column -> English bronze column (contract shared with the synthetic generator in core).
STATION_COLUMNS = GIOS_STATION_COLUMNS
POSITION_COLUMNS = GIOS_POSITION_COLUMNS


def _cell(v):
    # Dates as ISO strings so Spark gets one consistent type per column.
    if isinstance(v, dt.datetime):
        return v.date().isoformat()
    if v is None:
        return None
    text = str(v).strip()
    return text or None  # the positions sheet has empty strings instead of empty cells


def read_sheet(xlsx_path: Path, sheet: str, columns: dict[str, str]) -> list[dict]:
    import openpyxl

    wb = openpyxl.load_workbook(xlsx_path, read_only=True)
    rows = wb[sheet].iter_rows(values_only=True)
    header = [str(h) if h is not None else "" for h in next(rows)]
    missing = set(columns) - set(header)
    if missing:
        raise ValueError(f"{xlsx_path.name}/{sheet}: missing columns {sorted(missing)}")
    idx = {header.index(src): dst for src, dst in columns.items()}
    # A row may be shorter than the header: xlsx files may omit trailing empty cells (found by the
    # integration test on generated files) — a missing cell is an empty value, not an error.
    out = [{dst: _cell(r[i] if i < len(r) else None) for i, dst in idx.items()}
           for r in rows if any(c is not None for c in r)]
    wb.close()
    return out


def to_frame(spark: SparkSession, rows: list[dict], columns: dict[str, str], source_file: str) -> DataFrame:
    schema = ", ".join(f"{c} STRING" for c in columns.values())
    df = spark.createDataFrame([tuple(r[c] for c in columns.values()) for r in rows], schema)
    # try_cast: one malformed cell must not fail the whole metadata load (bronze keeps raw data);
    # it becomes NULL and is visible in silver checks.
    for c in ("opened_on", "closed_on"):
        df = df.withColumn(c, F.expr(f"try_cast({c} AS DATE)"))
    for c in ("lat", "lon"):
        if c in df.columns:
            df = df.withColumn(c, F.expr(f"try_cast({c} AS DOUBLE)"))
    return df.withColumn("source_file", F.lit(source_file)).withColumn("ingest_ts", F.current_timestamp())
