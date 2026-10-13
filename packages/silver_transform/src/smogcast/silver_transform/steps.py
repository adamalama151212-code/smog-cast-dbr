"""silver_transform steps: each is ``(Context) -> None``."""

from __future__ import annotations

import datetime as dt
import logging

from pyspark.sql import functions as F

from smogcast.core.config import active_cities
from smogcast.core.context import Context
from smogcast.core.dq_metrics import log_metrics
from smogcast.core.timeutil import issue_day_cet
from smogcast.silver_transform.features import build_features
from smogcast.silver_transform.pm_daily import build_city_daily, build_station_daily
from smogcast.silver_transform.weather_daily import build_weather_daily

log = logging.getLogger(__name__)


def step_weather_daily(ctx: Context) -> None:
    """bronze.weather_forecast → silver.weather_daily (full overwrite; a few tens of thousands of rows)."""
    st = ctx.storage
    hourly = st.read("bronze", "weather_forecast").where(F.col("city_id").isin(ctx.run["cities"]))
    st.overwrite(build_weather_daily(hourly), "silver", "weather_daily")

    daily = st.read("silver", "weather_daily")
    metrics = {}
    for r in daily.groupBy("source").agg(F.count("*").alias("days"), F.sum(F.when(~F.col("is_complete"), 1).otherwise(0)).alias("incomplete")).collect():
        metrics[f"days_{r['source']}"] = r["days"]
        metrics[f"incomplete_days_{r['source']}"] = r["incomplete"]
    # How much worse is a real day-ahead forecast than the short-range archive? (same days, both sources)
    a, b = daily.where("source = 'short_range'").alias("a"), daily.where("source = 'day_ahead'").alias("b")
    diff = a.join(b, ["city_id", "day_cet"]).agg(
        F.avg(F.abs(F.col("a.t_mean_c") - F.col("b.t_mean_c"))).alias("t"),
        F.avg(F.abs(F.col("a.wind_mean_ms") - F.col("b.wind_mean_ms"))).alias("w"),
    ).first()
    if diff["t"] is not None:
        metrics["mae_t_mean_short_range_vs_day_ahead_c"] = diff["t"]
        metrics["mae_wind_mean_short_range_vs_day_ahead_ms"] = diff["w"]
    log_metrics(st, "silver_weather_daily", metrics)


def step_pm_daily(ctx: Context) -> None:
    """silver.pm_hourly → silver.pm_daily_station, silver.pm_daily_city (full overwrite)."""
    st, cfg = ctx.storage, ctx.cfg
    m, th = cfg["model"], cfg["thresholds"]
    station = build_station_daily(
        st.read("silver", "pm_hourly"), th["daily_min_valid_hours"], m["morning_last_hour_cet"], m["morning_min_hours"]
    )
    st.overwrite(station, "silver", "pm_daily_station")
    city = build_city_daily(st.read("silver", "pm_daily_station"), th["daily_limit_ug_m3"])
    st.overwrite(city, "silver", "pm_daily_city")

    c = st.read("silver", "pm_daily_city")
    metrics = {"station_days_invalid": st.read("silver", "pm_daily_station").where("NOT is_valid").count()}
    # Sanity check worth reading in the log: share of exceedance days per city and pollutant.
    for r in c.where("exceeded IS NOT NULL").groupBy("city_id", "pollutant").agg(
        F.avg(F.col("exceeded").cast("double")).alias("rate")
    ).collect():
        metrics[f"exceedance_rate_{r['city_id']}_{r['pollutant']}"] = round(r["rate"], 4)
    log_metrics(st, "silver_pm_daily", metrics)


def step_features(ctx: Context) -> None:
    """silver.pm_daily_city + silver.weather_daily → silver.features (full overwrite)."""
    st = ctx.storage
    feats = build_features(st.read("silver", "pm_daily_city"), st.read("silver", "weather_daily"), ctx.cfg["model"])
    st.overwrite(feats, "silver", "features")

    f = st.read("silver", "features")
    metrics = {}
    for r in f.groupBy("split", "pollutant").agg(
        F.count("*").alias("rows"), F.sum(F.col("is_usable").cast("int")).alias("usable"),
        F.avg(F.when(F.col("is_usable"), F.col("exceeded_next_day").cast("double"))).alias("positive_rate"),
    ).collect():
        key = f"{r['split']}_{r['pollutant']}"
        metrics[f"rows_{key}"], metrics[f"usable_{key}"] = r["rows"], r["usable"]
        metrics[f"positive_rate_{key}"] = round(r["positive_rate"] or 0.0, 4)
    log_metrics(st, "silver_features", metrics)


def step_pm_daily_live(ctx: Context) -> None:
    """silver.pm_stream (source = live) → silver.pm_daily_city_live: measured daily means of FINISHED CET days.

    The same functions and rules as the archive (18 valid hours, city = worst station), so measured live days compare
    with the history and with the model's label. Today (CET) is left out — it is not over yet. Full overwrite: a few
    hundred rows, recomputed every run, so values the GIOŚ fills in late are picked up by the next run.
    """
    st, cfg = ctx.storage, ctx.cfg
    m, th = cfg["model"], cfg["thresholds"]
    if not st.exists("silver", "pm_stream"):
        raise FileNotFoundError("silver.pm_stream missing — run `smogcast-silver-clean --step pm-stream` first")
    today = issue_day_cet()
    stream = st.read("silver", "pm_stream").where((F.col("source") == "live") & (F.col("day_cet") < F.lit(today)))
    station = build_station_daily(stream, th["daily_min_valid_hours"], m["morning_last_hour_cet"], m["morning_min_hours"])
    st.overwrite(build_city_daily(station, th["daily_limit_ug_m3"]), "silver", "pm_daily_city_live")

    c = st.read("silver", "pm_daily_city_live")
    days = c.where("exceeded IS NOT NULL")
    log_metrics(st, "silver_pm_daily_live", {
        "city_days": days.count(), "exceedances": days.where("exceeded").count(),
        "days": days.select("day_cet").distinct().count()})


LIVE_KEYS = ["city_id", "pollutant", "issue_day"]


def step_features_live(ctx: Context) -> None:
    """silver.pm_stream (source = live) + live weather forecast → silver.features_live (MERGE).

    One row per configured city and pollutant for issue day D = today in CET (``SMOGCAST_ISSUE_DATE``
    overrides it). Daily means come from the cleaned live stream with the same functions and
    rules as the history (18 valid hours, morning window), so the model sees the same features
    it was trained on. A city without yesterday's PM or without tomorrow's weather still gets
    a row — ``is_usable = false`` with ``unusable_reason`` — so a missing forecast is visible.
    """
    st, cfg = ctx.storage, ctx.cfg
    m, th = cfg["model"], cfg["thresholds"]
    if not st.exists("silver", "pm_stream"):
        raise FileNotFoundError("silver.pm_stream missing — run `smogcast-silver-clean --step pm-stream` first")
    day = issue_day_cet()
    stream = st.read("silver", "pm_stream").where(
        (F.col("source") == "live") & F.col("day_cet").between(day - dt.timedelta(days=2), day))
    station = build_station_daily(stream, th["daily_min_valid_hours"], m["morning_last_hour_cet"], m["morning_min_hours"])
    city = build_city_daily(station, th["daily_limit_ug_m3"])

    grid = [(c, p, day) for c in active_cities(cfg) for p in cfg["pollutants"]]
    issues = ctx.spark.createDataFrame(grid, "city_id STRING, pollutant STRING, issue_day DATE")
    weather = st.read("silver", "weather_daily").where("source = 'live'")
    feats = (
        build_features(city, weather, m, weather_source="live", require_label=False, issues=issues)
        .withColumn("split", F.lit("live"))
        .withColumn("unusable_reason",
                    F.when(F.col("pm_d1_max_ug_m3").isNull(), "no_valid_pm_yesterday")
                    .when(~F.col("is_usable"), "no_complete_weather_forecast"))
        .withColumn("built_at", F.current_timestamp())
    )
    st.merge(feats, "silver", "features_live", LIVE_KEYS)

    today = st.read("silver", "features_live").where(F.col("issue_day") == F.lit(day))
    metrics = {"rows": today.count(), "usable": today.where("is_usable").count()}
    for r in today.where("NOT is_usable").groupBy("unusable_reason").count().collect():
        metrics[f"unusable_{r['unusable_reason']}"] = r["count"]
    log.info("features-live: issue day %s, %d of %d rows usable", day, metrics["usable"], metrics["rows"])
    log_metrics(st, "silver_features_live", metrics)
