"""gold.dq_summary: data quality of the stream path, MEASURED per source (live API, fault replay).

The dashboard shows how much of what arrived was usable and why the rest was not. Counts are
DISTINCT readings (station, pollutant, hour), because the GIOŚ API re-sends the last ~3 days on
every poll — raw record counts would mostly measure the re-sending, not the data. Percentages
are relative to the unique readings received.

The stream tables are the CURRENT state (recomputed from bronze.pm_stream): a reading first sent empty
and later filled in by GIOŚ counts in silver, no longer under ``quarantined_*``.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

KEY = ["station_code", "pollutant", "time_utc"]
SCHEMA = "source STRING, metric STRING, value DOUBLE"


def _metric(df: DataFrame, name, value) -> DataFrame:
    """(source, metric, value) rows; ``name`` may be a column, e.g. one metric per quarantine reason."""
    metric = name if not isinstance(name, str) else F.lit(name)
    return df.select("source", metric.alias("metric"), value.cast("double").alias("value"))


def build_dq_summary(bronze: DataFrame, late: DataFrame, quarantine: DataFrame, silver: DataFrame) -> DataFrame:
    """Inputs are the stream tables already limited to the stream sources."""
    received = bronze.groupBy("source").agg(
        F.count("*").alias("records"), F.countDistinct(*KEY).alias("unique"))
    late_n = late.groupBy("source").agg(F.countDistinct(*KEY).alias("n"))
    q_total = quarantine.groupBy("source").agg(F.countDistinct(*KEY).alias("n"))
    q_reason = quarantine.groupBy("source", "quarantine_reason").agg(F.countDistinct(*KEY).alias("n"))
    s_total = silver.groupBy("source").agg(F.count("*").alias("n"), F.sum(F.col("is_valid").cast("int")).alias("valid"))
    s_flag = silver.groupBy("source", "dq_flag").agg(F.count("*").alias("n"))

    parts = [
        _metric(received, "records_received", F.col("records")),
        _metric(received, "unique_readings", F.col("unique")),
        _metric(received, "resent_duplicates", F.col("records") - F.col("unique")),
        _metric(late_n, "late_rejected", F.col("n")),
        _metric(q_total, "quarantined", F.col("n")),
        _metric(q_reason, F.concat(F.lit("quarantined_"), F.col("quarantine_reason")), F.col("n")),
        _metric(s_total, "silver_readings", F.col("n")),
        _metric(s_total, "silver_valid", F.col("valid")),
        _metric(s_flag, F.concat(F.lit("flag_"), F.col("dq_flag")), F.col("n")),
    ]
    rows = parts[0]
    for p in parts[1:]:
        rows = rows.unionByName(p)
    # Raw record counts are not shares of the unique readings (re-sends make them > 100%).
    no_pct = F.col("metric").isin("records_received", "resent_duplicates")
    return (
        rows.join(received.select("source", "unique"), "source", "left")
        .withColumn("pct_of_unique", F.when(no_pct, None).otherwise(F.round(F.try_divide(F.col("value") * 100, F.col("unique")), 2)))
        .drop("unique")
        .withColumn("computed_at", F.current_timestamp())
    )
