"""silver.features: one row per (city, pollutant, issue day D) predicting day D+1.

Leakage rule (decision D6): a feature may use only what is known on day D at the issue
hour (12:00 CET):
- PM of day D-1 (complete day) and of the morning of day D (published hours),
- the weather FORECAST for D+1 (from a forecast archive — never the observed weather),
- the calendar of D+1.
The label ``exceeded_next_day`` is the only column that looks at D+1 measurements.

Weather source per split (decision D7): training rows use the short_range archive (the only
one before 2024); validation and test rows use real day-ahead forecasts. A row whose weather
is missing or incomplete is kept but marked ``is_usable = false`` — never back-filled.

Live forecasts (stage E6e) reuse the same function: weather forced to the ``live`` source,
no label required (tomorrow has not happened yet) and an explicit grid of issue keys, so every
city gets a row even when its PM data for today is missing.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from smogcast.core.schema import CALENDAR_FEATURES, PM_FEATURES, WEATHER_FEATURES


def split_of(target_day: Column, split_cfg: dict) -> Column:
    y = F.year(target_day)
    lo, hi = split_cfg["train_years"]
    return (
        F.when(y.between(lo, hi), "train")
        .when(y == split_cfg["validation_year"], "validation")
        .when(y == split_cfg["test_year"], "test")
        .otherwise("none")
    )


def build_features(city_daily: DataFrame, weather_daily: DataFrame, model_cfg: dict,
                   weather_source: str | None = None, require_label: bool = True,
                   issues: DataFrame | None = None) -> DataFrame:
    """``weather_source``: one forecast source for every row (live) instead of the per-split choice;
    ``require_label``: a row needs the observed D+1 to be usable (history) or not (live);
    ``issues``: rows to build (city_id, pollutant, issue_day) — default every day with PM data."""
    d = city_daily.select(
        "city_id", "pollutant", "day_cet", "max_station_mean_ug_m3", "mean_station_mean_ug_m3", "exceeded",
        "morning_max_station_ug_m3", "morning_mean_ug_m3",
    )
    # Issue days = every day that has any PM information; D-1 / D+1 are looked up by date arithmetic,
    # so a missing day stays NULL instead of silently shifting to the nearest available one.
    base = issues if issues is not None else d.select("city_id", "pollutant", F.col("day_cet").alias("issue_day"))
    keys = ["city_id", "pollutant"]

    def at(offset: int, cols: dict[str, str]) -> DataFrame:
        # The row of day X is attached to issue day X - offset: offset -1 makes it "D-1"
        # (date_sub(X, -1) = X + 1), offset 1 makes it "D+1" (the label).
        sel = [F.col(src).alias(dst) for src, dst in cols.items()]
        return d.select(*keys, F.date_sub("day_cet", offset).alias("issue_day"), *sel)

    f = (
        base.join(at(-1, {"max_station_mean_ug_m3": "pm_d1_max_ug_m3", "mean_station_mean_ug_m3": "pm_d1_mean_ug_m3",
                          "exceeded": "pm_d1_exceeded"}), keys + ["issue_day"], "left")
        .join(at(-2, {"max_station_mean_ug_m3": "pm_d2_max_ug_m3"}), keys + ["issue_day"], "left")
        .join(at(0, {"morning_max_station_ug_m3": "pm_d_morning_max_ug_m3",
                     "morning_mean_ug_m3": "pm_d_morning_mean_ug_m3"}), keys + ["issue_day"], "left")
        .join(at(1, {"exceeded": "exceeded_next_day", "max_station_mean_ug_m3": "next_day_max_ug_m3"}),
              keys + ["issue_day"], "left")
        .withColumn("target_day", F.date_add("issue_day", 1))
        .withColumn("split", split_of(F.col("target_day"), model_cfg["split"]))
    )

    ws = model_cfg["weather_source"]
    per_split = (
        F.when(F.col("split") == "train", ws["train"])
        .when(F.col("split") == "validation", ws["validation"])
        .when(F.col("split") == "test", ws["test"])
        .otherwise(ws["test"])  # out-of-split history rows behave like operational forecasts
    )
    f = f.withColumn("weather_source", F.lit(weather_source) if weather_source else per_split)
    w = weather_daily.select(
        "city_id", F.col("source").alias("weather_source"), F.col("day_cet").alias("target_day"),
        F.col("is_complete").alias("weather_complete"), *WEATHER_FEATURES,
    )
    f = f.join(w, ["city_id", "weather_source", "target_day"], "left")

    month = F.month("target_day")
    f = (
        f.withColumn("target_month", month)
        .withColumn("target_dow", F.dayofweek("target_day"))
        .withColumn("target_is_weekend", F.dayofweek("target_day").isin(1, 7))
        .withColumn("target_is_heating_season", (month >= 10) | (month <= 4))
    )
    usable = F.col("weather_complete").eqNullSafe(True) & F.col("pm_d1_max_ug_m3").isNotNull()
    if require_label:
        usable = usable & F.col("exceeded_next_day").isNotNull()
    return f.withColumn("is_usable", usable).select(
        "city_id", "pollutant", "issue_day", "target_day", "split", "weather_source",
        *PM_FEATURES, *WEATHER_FEATURES, *CALENDAR_FEATURES,
        "exceeded_next_day", "next_day_max_ug_m3", "is_usable",
    )
