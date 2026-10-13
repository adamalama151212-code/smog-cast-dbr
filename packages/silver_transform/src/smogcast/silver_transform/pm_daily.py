"""silver.pm_hourly → daily means per station and per city.

Definitions (ARCHITECTURE.md, section 11.1):
- a station-day is valid with at least ``daily_min_valid_hours`` valid hours (75% of 24 h);
- a city exceeds the limit on a day when AT LEAST ONE valid station mean is above it
  (decision D4 — this is how exceedances are reported per measurement position);
- the "morning" mean (hours starting 00:00–``morning_last_hour_cet`` CET) is what is already
  published at the forecast issue time on that same day.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_station_daily(hourly: DataFrame, min_valid_hours: int, morning_last_hour: int, morning_min_hours: int) -> DataFrame:
    v = hourly.where("is_valid")
    morning = F.col("hour_cet") <= morning_last_hour
    daily = v.groupBy("station_code", "city_id", "pollutant", "day_cet").agg(
        F.count("*").alias("n_valid_hours"),
        F.avg("value").alias("mean_ug_m3"),
        F.max("value").alias("max_hour_ug_m3"),
        F.sum(F.when(morning, 1).otherwise(0)).alias("n_morning_hours"),
        F.avg(F.when(morning, F.col("value"))).alias("morning_mean_ug_m3"),
    )
    return (
        daily.withColumn("is_valid", F.col("n_valid_hours") >= min_valid_hours)
        .withColumn("is_morning_valid", F.col("n_morning_hours") >= morning_min_hours)
    )


def build_city_daily(station_daily: DataFrame, limits: dict[str, float]) -> DataFrame:
    limit = F.create_map(*[x for p, lim in limits.items() for x in (F.lit(p), F.lit(float(lim)))])
    full = (
        station_daily.where("is_valid")
        .groupBy("city_id", "pollutant", "day_cet")
        .agg(
            F.count("*").alias("n_stations"),
            F.max("mean_ug_m3").alias("max_station_mean_ug_m3"),
            F.avg("mean_ug_m3").alias("mean_station_mean_ug_m3"),
        )
    )
    morning = (
        station_daily.where("is_morning_valid")
        .groupBy("city_id", "pollutant", "day_cet")
        .agg(
            F.count("*").alias("n_morning_stations"),
            F.max("morning_mean_ug_m3").alias("morning_max_station_ug_m3"),
            F.avg("morning_mean_ug_m3").alias("morning_mean_ug_m3"),
        )
    )
    return (
        full.join(morning, ["city_id", "pollutant", "day_cet"], "full_outer")
        .withColumn("limit_ug_m3", limit[F.col("pollutant")])
        # NULL when no station had a valid full day: unknown, not "no exceedance".
        .withColumn("exceeded", F.col("max_station_mean_ug_m3") > F.col("limit_ug_m3"))
    )
