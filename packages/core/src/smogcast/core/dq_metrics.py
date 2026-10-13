"""Data-quality metrics written to ``ops.dq_metrics``.

Why a table and not just logs: the dashboard must show that data quality is
MEASURED — how many records were quarantined, how many arrived late, etc.
Logs disappear; a Delta table stays and can be queried with SQL.
"""

from __future__ import annotations

import datetime as dt
import logging

from pyspark.sql import types as T

from smogcast.core.storage import Storage

log = logging.getLogger(__name__)

_SCHEMA = T.StructType(
    [
        T.StructField("run_ts", T.TimestampType()),
        T.StructField("step", T.StringType()),
        T.StructField("metric", T.StringType()),
        T.StructField("value", T.DoubleType()),
    ]
)


def log_metrics(storage: Storage, step: str, metrics: dict[str, float]) -> None:
    # Time-zone-aware datetime — a naive one would be shifted by PySpark to the OS zone.
    now = dt.datetime.now(dt.timezone.utc)
    rows = [(now, step, k, float(v)) for k, v in metrics.items()]
    for _, _, k, v in rows:
        log.info("[DQ] %s: %s = %s", step, k, v)
    df = storage.spark.createDataFrame(rows, _SCHEMA)
    # Plain append (not MERGE): this is an event log, every run gets its own entries.
    storage.save(df.write.format("delta").mode("append"), "ops", "dq_metrics")
