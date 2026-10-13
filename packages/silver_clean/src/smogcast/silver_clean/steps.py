"""silver_clean steps: each is ``(Context) -> None``."""

from __future__ import annotations

import logging

from pyspark.sql import functions as F

from smogcast.core.config import active_cities
from smogcast.core.context import Context
from smogcast.core.dq_metrics import log_metrics
from smogcast.core.timeutil import day_cet_col
from smogcast.silver_clean.pm_clean import flag_patterns, split_hard_rules
from smogcast.silver_clean.station_city import build_station_city, cities_frame
from smogcast.silver_clean.stations_scd2 import apply_scd2, changes_from_snapshots

log = logging.getLogger(__name__)


def step_pm_hourly(ctx: Context) -> None:
    """bronze.pm_hourly (archive) → silver.station_city, silver.pm_hourly, ops.pm_quarantine.

    Only stations of the configured cities are kept. Renamed stations are unified under their
    CURRENT code (``source_station_code`` keeps what the archive said).
    """
    st, dq = ctx.storage, ctx.cfg["pm_dq"]
    station_city = build_station_city(
        st.read("bronze", "gios_stations"), cities_frame(ctx.spark, active_cities(ctx.cfg))
    )
    st.overwrite(station_city, "silver", "station_city")
    station_city = st.read("silver", "station_city")

    pm = (
        st.read("bronze", "pm_hourly")
        .withColumnRenamed("station_code", "source_station_code")
        .join(F.broadcast(station_city.select(
            F.col("station_code").alias("source_station_code"), "current_station_code", "city_id", "jurisdiction_code"
        )), "source_station_code")
        .withColumnRenamed("current_station_code", "station_code")
        .select("station_code", "source_station_code", "city_id", "jurisdiction_code", "pollutant",
                "time_utc", "value", "source_year")
        .withColumn("source", F.lit("archive"))
        # A renamed station could report the same hour under both codes — keep one.
        .dropDuplicates(dq["dedup"]["keys"])
    )
    ok, quarantined = split_hard_rules(pm, dq)
    st.overwrite(quarantined, "ops", "pm_quarantine", replace_where="source = 'archive'")

    clean = (
        flag_patterns(ok, dq)
        .withColumn("day_cet", day_cet_col(F.col("time_utc")))
        .withColumn("hour_cet", F.hour(F.col("time_utc") + F.expr("INTERVAL 1 HOUR")))
    )
    st.overwrite(clean, "silver", "pm_hourly", partition_by=["source_year"])

    out = st.read("silver", "pm_hourly")
    metrics = {
        "stations_matched": station_city.where("code_kind = 'current'").count(),
        "old_codes_mapped": station_city.where("code_kind = 'old'").count(),
        "quarantined_archive": st.read("ops", "pm_quarantine").where("source = 'archive'").count(),
    }
    for r in out.groupBy("dq_flag").count().collect():
        metrics[f"rows_{r['dq_flag']}"] = r["count"]
    log_metrics(st, "silver_clean_pm_hourly", metrics)


def step_stations_scd2(ctx: Context) -> None:
    """bronze.station_snapshots → silver.station_changes (CDC feed) → silver.stations (SCD2)."""
    st = ctx.storage
    changes = changes_from_snapshots(st.read("bronze", "station_snapshots"))
    st.overwrite(changes, "silver", "station_changes")
    st.overwrite(apply_scd2(st.read("silver", "station_changes")), "silver", "stations")
    metrics = {f"changes_{r['registry_source']}_{r['op']}": r["count"]
               for r in st.read("silver", "station_changes").groupBy("registry_source", "op").count().collect()}
    metrics["current_stations"] = st.read("silver", "stations").where("is_current").count()
    log_metrics(st, "silver_stations_scd2", metrics)


# ----------------------------------------------------------------------------- stream (E6d, E9)
# Locally this step runs the stream path; on Databricks the declarative pipeline pipelines/smogcast_stream does,
# with the same table definitions (smogcast.silver_clean.pm_stream).
STREAM_TABLES = [("bronze", "pm_stream"), ("ops", "pm_late_rejected"), ("ops", "pm_stream_quarantine"),
                 ("silver", "pm_stream")]


def _stream_mapping(ctx: Context):
    from smogcast.silver_clean.pm_stream import stream_station_city

    st = ctx.storage
    registry = (st.read("silver", "stations").where("registry_source = 'gios_api' AND is_current")
                if st.exists("silver", "stations") else
                ctx.spark.createDataFrame([], "station_code STRING, city_name STRING"))
    return stream_station_city(st.read("silver", "station_city"), registry,
                               cities_frame(ctx.spark, active_cities(ctx.cfg)))


def _bronze_checkpoint(ctx: Context, source: str) -> str:
    return ctx.storage.checkpoint(f"pm_stream_bronze_{source}")


def step_pm_stream(ctx: Context) -> None:
    """Landing poll files (live API, fault replay) → bronze.pm_stream → the cleaned stream tables.

    1. Structured Streaming, trigger(availableNow): every waiting poll file is appended to bronze.pm_stream,
       then the query stops. The Delta sink with its checkpoint is exactly-once — a re-run appends only new
       files (on Databricks: Auto Loader in the pipeline).
    2. ops.pm_late_rejected, ops.pm_stream_quarantine and silver.pm_stream are RECOMPUTED from the whole of
       bronze.pm_stream (smogcast.silver_clean.pm_stream) — on Databricks: the pipeline's materialized views.
    """
    from pathlib import Path

    from smogcast.silver_clean import pm_stream as ps

    st, dq = ctx.storage, ctx.cfg["pm_dq"]
    for source, folder in ps.SOURCES.items():
        path = st.live_landing(folder) if source in ps.API_SOURCES else st.landing(folder)
        if not Path(path).exists() or not any(Path(path).glob("*.json")):
            log.info("pm-stream: no batches for %s", source)
            continue
        stream = (
            ctx.spark.readStream.schema(ps.LANDING_SCHEMA)
            .option("multiLine", True)
            .option("pathGlobFilter", "*.json")   # skip *.json.part files that are still being written
            .json(path)
        )
        st.append_stream(ps.flatten(stream), "bronze", "pm_stream", _bronze_checkpoint(ctx, source))
        log.info("pm-stream: %s appended to bronze", source)
    if not st.exists("bronze", "pm_stream"):
        log.info("pm-stream: nothing received yet")
        return

    bronze = st.read("bronze", "pm_stream").where(F.col("source").isin(list(ps.SOURCES)))
    r = ps.readings(bronze, _stream_mapping(ctx), dq["late_arrival"]["max_lag_hours"]).cache()
    st.overwrite(ps.late(r), "ops", "pm_late_rejected")
    st.overwrite(ps.quarantine(r, dq), "ops", "pm_stream_quarantine")
    st.overwrite(ps.silver(ps.accepted(r, dq), dq), "silver", "pm_stream")
    r.unpersist()

    metrics: dict[str, float] = {}
    for row in st.read("bronze", "pm_stream").groupBy("source").count().collect():
        metrics[f"{row['source']}_records"] = row["count"]
    for layer, name, label in (("ops", "pm_late_rejected", "late_rejected"),
                               ("ops", "pm_stream_quarantine", "quarantined"), ("silver", "pm_stream", "accepted")):
        for row in st.read(layer, name).groupBy("source").count().collect():
            metrics[f"{row['source']}_{label}"] = row["count"]
    log_metrics(st, "silver_pm_stream", metrics)


def pipeline_owns_stream_tables(cfg: dict) -> bool:
    """On Databricks the declarative pipeline owns the stream tables (conf/<env>.yaml, storage.stream_tables_owner)."""
    return cfg["storage"].get("stream_tables_owner", "steps") == "pipeline"


def _reset_stream_source(ctx: Context, source: str) -> None:
    """Forget everything the stream derived from one source: its checkpoint and its rows in the
    stream tables. The next pm-stream run then re-reads every landing file of that source.

    When the declarative pipeline owns the stream tables (Databricks), they are left alone — a step may not edit a
    pipeline's tables (some are materialized views) — and the FULL refresh of the pipeline that follows the reset
    rebuilds them from the landing files (Job smogcast-fault-demo)."""
    import shutil

    st = ctx.storage
    # pm_stream_<source>: checkpoint of the per-micro-batch cleaning used until 2026-10-07.
    for name in (f"pm_stream_bronze_{source}", f"pm_stream_{source}"):
        shutil.rmtree(st.checkpoint(name), ignore_errors=True)
    # ops.pm_quarantine held the stream's quarantine until 2026-10-07 (now only the archive's) — clean it too.
    tables = [("ops", "pm_quarantine")]
    if pipeline_owns_stream_tables(ctx.cfg):
        log.info("stream tables belong to the pipeline — a full refresh of it rebuilds them without %s", source)
    else:
        tables = [*STREAM_TABLES, *tables]
    for layer, name in tables:
        if st.exists(layer, name):
            st.delta_table(layer, name).delete(F.col("source") == source)


def step_fault_reset(ctx: Context) -> None:
    """Remove everything the fault-injection demo produced, so it can be replayed from scratch.

    Its landing files, its registry changes (silver.station_changes / silver.stations) and — locally — its rows in
    the stream tables; on Databricks the pipeline's full refresh drops those (``_reset_stream_source``)."""
    import shutil

    st = ctx.storage
    for folder in ("fault_replay", "fault_replay_registry"):
        shutil.rmtree(st.landing(folder), ignore_errors=True)
    _reset_stream_source(ctx, "fault_replay")
    for layer, name in (("silver", "station_changes"), ("silver", "stations")):
        if st.exists(layer, name):
            st.delta_table(layer, name).delete("registry_source = 'fault_replay'")
    log.info("fault-reset: fault_replay data removed")


def step_live_reset(ctx: Context) -> None:
    """Re-clean the live stream from its landing files (after a fix to the cleaning rules).

    Unlike fault-reset, landing/gios_live stays: those polls cannot be repeated (the API only
    returns the last ~3 days). Needed e.g. when readings were quarantined as ``unknown_station``
    before the registry knew the station — re-sent with the same value they would otherwise
    stay duplicates of the quarantined record forever.
    """
    _reset_stream_source(ctx, "live")
    log.info("live-reset: live stream state removed (landing kept)")
