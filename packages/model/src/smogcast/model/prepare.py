"""silver.features → model-ready numeric columns.

Transformations are FIXED rules (no statistics learned from data), so applying them to the
validation or test year cannot leak information from those years:
- PM concentrations → log1p: smog is multiplicative (a jump from 20 to 40 matters as much as
  from 100 to 200) and the log tames the long tail for the logistic model;
- wind direction and month → sine/cosine: 359° is next to 1°, December next to January;
- a missing morning mean (the station had < 8 published hours) → filled with yesterday's mean
  plus a 0/1 "missing" indicator; a missing D-2 value → yesterday's value;
- every model input rounded to ROUND_DECIMALS (float noise must not change the model — see below).
City identity enters the models as a one-hot vector (see train.py).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from smogcast.core.schema import WEATHER_FEATURES

NUMERIC_FEATURES = [
    "log_pm_d1_max", "log_pm_d1_mean", "pm_d1_exceeded_f", "log_pm_d2_max",
    "log_pm_morning_max", "log_pm_morning_mean", "morning_missing",
    *[c for c in WEATHER_FEATURES if c != "wind_from_deg_mean"],
    "wind_from_sin", "wind_from_cos", "month_sin", "month_cos",
    "is_weekend_f", "is_heating_season_f",
]


# Model inputs are rounded to this many decimals. Means and logs computed on a laptop and on a Databricks
# cluster differ in the last bit (different summation order), and GBT picks split thresholds from the data
# values, so that noise alone changed its trees: a 2e-16 relative perturbation moved the PM10 validation
# Brier from 0.03970 to 0.03922 (2026-10-06), the size of the gap seen between Docker and DEV. Six decimals
# are far below any physical meaning (µg/m³, °C, m/s) and far above float noise.
ROUND_DECIMALS = 6


def prepare(features: DataFrame) -> DataFrame:
    log1p = F.log1p
    d1_max, d1_mean = F.col("pm_d1_max_ug_m3"), F.col("pm_d1_mean_ug_m3")
    two_pi = 2 * 3.141592653589793
    prepared = (
        features.where("is_usable")
        .withColumn("label", F.col("exceeded_next_day").cast("double"))
        .withColumn("log_pm_d1_max", log1p(d1_max))
        .withColumn("log_pm_d1_mean", log1p(d1_mean))
        .withColumn("pm_d1_exceeded_f", F.col("pm_d1_exceeded").cast("double"))
        .withColumn("log_pm_d2_max", log1p(F.coalesce("pm_d2_max_ug_m3", d1_max)))
        .withColumn("morning_missing", F.col("pm_d_morning_max_ug_m3").isNull().cast("double"))
        .withColumn("log_pm_morning_max", log1p(F.coalesce("pm_d_morning_max_ug_m3", d1_max)))
        .withColumn("log_pm_morning_mean", log1p(F.coalesce("pm_d_morning_mean_ug_m3", d1_mean)))
        .withColumn("wind_from_sin", F.sin(F.radians("wind_from_deg_mean")))
        .withColumn("wind_from_cos", F.cos(F.radians("wind_from_deg_mean")))
        .withColumn("month_sin", F.sin(F.col("target_month") * two_pi / 12))
        .withColumn("month_cos", F.cos(F.col("target_month") * two_pi / 12))
        .withColumn("is_weekend_f", F.col("target_is_weekend").cast("double"))
        .withColumn("is_heating_season_f", F.col("target_is_heating_season").cast("double"))
    )
    return prepared.withColumns({c: F.round(c, ROUND_DECIMALS) for c in NUMERIC_FEATURES})
