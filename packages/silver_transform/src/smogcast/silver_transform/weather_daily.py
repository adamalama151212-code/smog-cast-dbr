"""bronze.weather_forecast → silver.weather_daily: forecast weather aggregated per city and CET day.

Smog builds up under specific weather: weak wind (no ventilation), cold (more heating),
no rain (no washout), clear calm nights (temperature inversion). The daily aggregates
below describe exactly that. Both forecast sources are kept side by side (column ``source``);
which one a model day uses is decided when building features (stage E4, decision D7).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from smogcast.core.timeutil import day_cet_col

# Below this speed the air is practically stagnant for dispersion purposes.
CALM_WIND_MS = 1.5
# Hours with valid values needed for a usable day (some days lack a few hours).
MIN_VALID_HOURS = 20


def build_weather_daily(hourly: DataFrame) -> DataFrame:
    # Open-Meteo times are instants; the CET day of an instant T is the date of T + 1 h, which is
    # what day_cet_col computes. (Precipitation at T covers the preceding hour, so the 00:00 CET
    # value belongs to the previous evening — a one-hour edge effect we accept for daily sums.)
    if "fetched_at" in hourly.columns:
        # The live source holds several forecasts for the same hour: keep the newest one.
        newest = Window.partitionBy("city_id", "source", "time_utc").orderBy(F.col("fetched_at").desc_nulls_last())
        hourly = hourly.withColumn("_rn", F.row_number().over(newest)).where("_rn = 1").drop("_rn")
    rad = F.radians("wind_direction_10m")
    h = (
        hourly.where(F.col("temperature_2m").isNotNull())
        .withColumn("day_cet", day_cet_col(F.col("time_utc")))
        # Wind vector components (direction the air comes FROM, meteorological convention);
        # averaging angles directly would give nonsense around north (350° and 10° → 180°).
        .withColumn("u", -F.col("wind_speed_10m") * F.sin(rad))
        .withColumn("v", -F.col("wind_speed_10m") * F.cos(rad))
    )
    daily = h.groupBy("city_id", "source", "day_cet").agg(
        F.count("*").alias("n_hours"),
        F.avg("temperature_2m").alias("t_mean_c"),
        F.min("temperature_2m").alias("t_min_c"),
        F.max("temperature_2m").alias("t_max_c"),
        F.avg("relative_humidity_2m").alias("rh_mean_pct"),
        F.avg("wind_speed_10m").alias("wind_mean_ms"),
        F.min("wind_speed_10m").alias("wind_min_ms"),
        F.max("wind_speed_10m").alias("wind_max_ms"),
        F.sum(F.when(F.col("wind_speed_10m") < CALM_WIND_MS, 1).otherwise(0)).alias("calm_hours"),
        F.avg("u").alias("_u"),
        F.avg("v").alias("_v"),
        F.sum("precipitation").alias("precip_sum_mm"),
        F.sum(F.when(F.col("precipitation") > 0.1, 1).otherwise(0)).alias("precip_hours"),
        F.avg("surface_pressure").alias("pressure_mean_hpa"),
        F.avg("cloud_cover").alias("cloud_mean_pct"),
        # W/m² for one hour = Wh/m²
        F.sum("shortwave_radiation").alias("radiation_sum_wh_m2"),
    )
    return (
        daily.withColumn("t_range_c", F.col("t_max_c") - F.col("t_min_c"))
        .withColumn("wind_from_deg_mean", F.pmod(F.degrees(F.atan2(-F.col("_u"), -F.col("_v"))), F.lit(360.0)))
        .withColumn("is_complete", F.col("n_hours") >= MIN_VALID_HOURS)
        .drop("_u", "_v")
    )
