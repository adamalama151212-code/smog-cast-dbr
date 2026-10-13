"""Open-Meteo landing JSON → bronze.weather_forecast (one row per city, source and hour).

Spark reads the JSON files directly; the hourly arrays of the API response are zipped
and exploded into rows — no Python loop over hours. The variable suffix of the day_ahead
archive (``_previous_day1``) is removed here, so both sources share one schema.

``time_utc`` keeps Open-Meteo's meaning: instantaneous variables are valid AT that time,
precipitation is the sum over the PRECEDING hour.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_source(spark: SparkSession, paths: list[str], variables: list[str], suffix: str) -> DataFrame:
    raw = spark.read.option("multiLine", True).json(paths)
    hourly = F.col("payload.hourly")
    zipped = F.arrays_zip(
        hourly["time"].alias("time"),
        *[hourly[v + suffix].alias(v) for v in variables],
    )
    return (
        raw.select(
            "city_id",
            "source",
            F.to_timestamp("fetched_at").alias("fetched_at"),
            "requested_lat",
            "requested_lon",
            F.col("payload.latitude").alias("grid_lat"),
            F.col("payload.longitude").alias("grid_lon"),
            F.col("_metadata.file_path").alias("source_file"),
            F.explode(zipped).alias("h"),
        )
        .select(
            "city_id",
            "source",
            # API time strings like "2024-01-01T00:00", already UTC (timezone=GMT is enforced at ingest).
            F.to_timestamp("h.time", "yyyy-MM-dd'T'HH:mm").alias("time_utc"),
            *[F.col(f"h.{v}").cast("double").alias(v) for v in variables],
            "requested_lat",
            "requested_lon",
            "grid_lat",
            "grid_lon",
            "fetched_at",
            "source_file",
        )
        .withColumn("ingest_ts", F.current_timestamp())
    )
