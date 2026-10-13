"""Measured daily values per city and the verification of official forecasts (ARCHITECTURE.md, section 4).

- ``gold.measured_daily``: one row per (city, pollutant, CET day) with at least one valid station-day — the archive
  (silver.pm_daily_city, GIOŚ 2021–2025, verified) and the live stream (silver.pm_daily_city_live, finished days
  only). Same definitions as the model's label: daily mean valid with 18 h, city = worst station (D4). Where both
  sources have a day, the archive wins (verified data).
- ``gold.forecast_verification``: every OFFICIAL forecast next to what was measured on its target day — the live
  track record the test year 2025 cannot show. Preliminary and update runs are not judged (ARCHITECTURE.md, section 7).

Pure functions: DataFrames in, DataFrames out.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

DAY_KEYS = ["city_id", "pollutant", "day_cet"]
MEASURED_COLS = ["max_station_mean_ug_m3", "mean_station_mean_ug_m3", "n_stations", "limit_ug_m3", "exceeded"]
FORECAST_KEYS = ["city_id", "pollutant", "target_day"]
OUTCOMES = ("hit", "miss", "false_alarm", "correct_no_warning", "no_forecast", "no_data", "pending")


def heating_season(day: Column) -> Column:
    """'2024/25' for October 2024 – April 2025, NULL from May to September.

    Same months as the model's heating-season feature (silver_transform.features: month >= 10 or <= 4).
    """
    year, month = F.year(day), F.month(day)
    start = F.when(month >= 10, year).when(month <= 4, year - 1)
    return F.concat(start.cast("string"), F.lit("/"), F.lpad(((start + 1) % 100).cast("string"), 2, "0"))


def build_measured_daily(archive: DataFrame, live: DataFrame, city_info: DataFrame) -> DataFrame:
    """Archive and live city-days in one table; ``source`` tells them apart, the archive wins on overlap."""
    a = archive.select(*DAY_KEYS, *MEASURED_COLS).withColumn("source", F.lit("archive"))
    lv = (live.select(*DAY_KEYS, *MEASURED_COLS).withColumn("source", F.lit("live"))
          .join(a.select(*DAY_KEYS), DAY_KEYS, "left_anti"))
    return (
        a.unionByName(lv)
        # a morning-only row (no station had a valid full day) says nothing about the day
        .where(F.col("max_station_mean_ug_m3").isNotNull())
        .join(F.broadcast(city_info), "city_id", "inner")      # only the configured cities
        .withColumn("heating_season", heating_season(F.col("day_cet")))
        .withColumn("computed_at", F.current_timestamp())
        .select("city_id", "city_name", "jurisdiction_code", "pollutant", "day_cet", "heating_season",
                *MEASURED_COLS, "source", "computed_at")
    )


def outcome(warned: Column, exceeded: Column, has_forecast: Column, measured: Column, target_day: Column,
            today: dt.date) -> Column:
    """Contingency-table class of one official forecast (the same counts as POD / FAR in the backtest)."""
    return (
        F.when(~has_forecast, "no_forecast")
        .when(~measured & (target_day >= F.lit(today)), "pending")     # the target day is not over yet
        .when(~measured, "no_data")
        .when(warned & exceeded, "hit")
        .when(exceeded, "miss")
        .when(warned, "false_alarm")
        .otherwise("correct_no_warning")
    )


def build_verification(history: DataFrame, measured: DataFrame, today: dt.date) -> DataFrame:
    """Official forecasts (one per city, pollutant and target day) joined with the measured value of the target day."""
    first = Window.partitionBy(*FORECAST_KEYS).orderBy("issued_at")
    official = (
        history.where(F.col("run_kind") == "official")
        .withColumn("_n", F.row_number().over(first)).where("_n = 1")  # defensive: the rule gives one per day
        .select("city_id", "city_name", "jurisdiction_code", "pollutant", "issue_day", "target_day", "probability",
                "warned", "limit_ug_m3", "status", "issued_at")
    )
    fact = measured.select(
        "city_id", "pollutant", F.col("day_cet").alias("target_day"),
        F.col("max_station_mean_ug_m3").alias("measured_max_ug_m3"), "exceeded",
        F.col("source").alias("measured_source"))
    v = official.join(fact, FORECAST_KEYS, "left")
    has_forecast = F.col("probability").isNotNull()
    measured_ok = F.col("exceeded").isNotNull()
    return (
        v.withColumn("outcome", outcome(F.coalesce(F.col("warned"), F.lit(False)), F.col("exceeded"), has_forecast,
                                        measured_ok, F.col("target_day"), today))
        .withColumn("squared_error", F.when(has_forecast & measured_ok,
                                            F.pow(F.col("probability") - F.col("exceeded").cast("double"), 2)))
        .withColumn("computed_at", F.current_timestamp())
        .select("city_id", "city_name", "jurisdiction_code", "pollutant", "issue_day", "target_day", "probability",
                "warned", "status", "issued_at", "measured_max_ug_m3", "limit_ug_m3", "exceeded", "measured_source",
                "outcome", "squared_error", "computed_at")
    )
