"""Reference forecasts the model must beat (decision D9). Both need no model at all:

- persistence: "tomorrow like today" — probability 1 if yesterday's (D-1) city mean exceeded
  the limit, else 0. Hard to beat for smog, which comes in multi-day episodes;
- climatology: the share of exceedance days in the same city, pollutant and calendar month
  in the TRAINING years only (using later years would leak the future into the reference).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def persistence(prepared: DataFrame) -> DataFrame:
    return prepared.withColumn("probability", F.coalesce(F.col("pm_d1_exceeded_f"), F.lit(0.0)))


def climatology_table(prepared: DataFrame) -> DataFrame:
    train = prepared.where("split = 'train'")
    by_month = train.groupBy("city_id", "pollutant", "target_month").agg(F.avg("label").alias("clim_month"))
    overall = train.groupBy("city_id", "pollutant").agg(F.avg("label").alias("clim_all"))
    return by_month.join(overall, ["city_id", "pollutant"], "right")


def climatology(prepared: DataFrame, table: DataFrame) -> DataFrame:
    month = table.select("city_id", "pollutant", "target_month", "clim_month").where("target_month IS NOT NULL")
    overall = table.select("city_id", "pollutant", "clim_all").distinct()
    return (
        prepared.join(month, ["city_id", "pollutant", "target_month"], "left")
        .join(overall, ["city_id", "pollutant"], "left")
        # a month never seen in training falls back to the city's overall rate
        .withColumn("probability", F.coalesce("clim_month", "clim_all", F.lit(0.0)))
        .drop("clim_month", "clim_all")
    )
