"""Registry snapshot JSON (raw API field names) → bronze.station_snapshots.

One row per (snapshot, station, sensor). The snapshots are small (tens of stations), so
they are flattened in Python; Polish API field names are mapped to English columns here,
in one place.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SCHEMA = (
    "snapshot_ts TIMESTAMP, registry_source STRING, station_id LONG, station_code STRING, station_name STRING, "
    "city_name STRING, voivodeship STRING, lat DOUBLE, lon DOUBLE, sensor_id LONG, pollutant STRING, source_file STRING"
)


def _float(v) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def snapshot_rows(path: Path) -> list[tuple]:
    doc = json.loads(path.read_text(encoding="utf-8"))
    ts = dt.datetime.fromisoformat(doc["snapshot_ts"]).astimezone(dt.timezone.utc)
    rows = []
    for s in doc["stations"]:
        base = (ts, doc.get("source", "gios_api"), int(s["Identyfikator stacji"]), s.get("Kod stacji"),
                s.get("Nazwa stacji"), s.get("Nazwa miasta"), s.get("Województwo"),
                _float(s.get("WGS84 φ N")), _float(s.get("WGS84 λ E")))
        sensors = s.get("sensors") or [None]  # a station without sensors still exists in the registry
        for x in sensors:
            sid = int(x["Identyfikator stanowiska"]) if x else None
            code = x.get("Wskaźnik - kod") if x else None
            rows.append((*base, sid, code, str(path)))
    return rows


def read_snapshots(spark: SparkSession, paths: list[Path]) -> DataFrame:
    rows = [r for p in paths for r in snapshot_rows(p)]
    return spark.createDataFrame(rows, SCHEMA).withColumn("ingest_ts", F.current_timestamp())
