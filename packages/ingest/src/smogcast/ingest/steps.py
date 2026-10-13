"""Ingest steps: each is ``(Context) -> None`` and writes raw files to landing only."""

from __future__ import annotations

import datetime as dt
import logging
import shutil
from pathlib import Path

from pyspark.sql import functions as F

from smogcast.core.context import Context
from smogcast.ingest import fault_replay, gios_live, gios_registry, synthetic
from smogcast.ingest.gios_archive import download_archive
from smogcast.ingest.weather import download_history, download_live

log = logging.getLogger(__name__)


def _synthetic(ctx: Context, kind: str) -> bool:
    """Synthetic inputs instead of the real sources (integration tests, CI, --offline) — smogcast.ingest.synthetic."""
    return ctx.cfg["sources"][kind] == "synthetic"


def step_gios_archive(ctx: Context) -> None:
    raw = ctx.storage.landing("gios_archive", "raw")
    if _synthetic(ctx, "pm"):
        new = synthetic.write_archive(ctx.cfg, raw)
    else:
        new = download_archive(ctx.cfg, raw)
    log.info("gios-archive: %d new files", len(new))


def step_weather_forecast_history(ctx: Context) -> None:
    if _synthetic(ctx, "weather"):
        new = synthetic.write_weather_history(ctx.cfg, ctx.storage.landing("weather"))
    else:
        new = download_history(ctx.cfg, ctx.storage.landing("weather"))
    log.info("weather-forecast-history: %d files downloaded", len(new))


def step_gios_registry(ctx: Context) -> None:
    """Snapshot of the GIOŚ station registry for the configured cities (input for CDC / SCD2)."""
    if _synthetic(ctx, "pm"):
        stations = synthetic.registry_stations(ctx.cfg)
    else:
        cities = {ctx.cfg["cities"][c]["gios_city_name"] for c in ctx.run["cities"]}
        stations = gios_registry.fetch_snapshot(ctx.cfg["gios"]["api"]["base_url"], cities)
    path = gios_registry.write_snapshot(ctx.storage.live_landing("gios_registry"), stations)
    log.info("gios-registry: %d stations, %d sensors -> %s", len(stations),
             sum(len(s["sensors"]) for s in stations), path.name)


def step_gios_live(ctx: Context) -> None:
    """One poll of live PM10/PM2.5 values for all sensors of the configured cities."""
    st = ctx.storage
    if _synthetic(ctx, "pm"):
        now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
        batch_dir = st.live_landing("gios_live")
        path = synthetic.write_live_batch(ctx.cfg, batch_dir, now, gios_live.previous_fetch(batch_dir))
        log.info("gios-live (synthetic): -> %s", path.name)
        return
    snapshot = gios_live.latest_snapshot(st.live_landing("gios_registry"))
    codes = {p["gios_code"] for p in ctx.cfg["pollutants"].values()}
    sensors = gios_live.pm_sensors(snapshot, codes)
    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    batch_dir = st.live_landing("gios_live")
    skipped: list[int] = []
    records = gios_live.poll(ctx.cfg["gios"]["api"]["base_url"], sensors, now, skipped=skipped)
    path = gios_live.write_batch(batch_dir, "live", records, now, gios_live.previous_fetch(batch_dir))
    log.info("gios-live: %d sensors (%d without live data, e.g. manual), %d records (%d without value) -> %s",
             len(sensors), len(skipped), len(records), sum(r["value"] is None for r in records), path.name)


def step_fault_replay(ctx: Context) -> None:
    """Replay a window of REAL hourly measurements as corrupted stream batches (decision D17).

    Reads a copy of bronze.pm_hourly for the configured city and window; writes hourly batch files
    to landing/fault_replay/ and registry snapshots to landing/fault_replay_registry/. The folders
    are cleared first, so a re-run produces exactly the same files.
    """
    st, fi = ctx.storage, ctx.cfg["fault_injection"]
    city = ctx.cfg["cities"][fi["city"]]
    start = dt.datetime.fromisoformat(fi["start_utc"])
    end = start + dt.timedelta(hours=fi["hours"])
    codes = (
        st.read("silver", "station_city").where(F.col("city_id") == fi["city"])
        .select(F.col("station_code").alias("source_station_code"), "current_station_code")
    )
    rows = (
        st.read("bronze", "pm_hourly")
        .where(F.col("pollutant").isin(fi["pollutants"]) & (F.col("time_utc") >= start) & (F.col("time_utc") < end))
        .join(codes.withColumnRenamed("source_station_code", "station_code"), "station_code")
        .select(F.col("current_station_code").alias("station_code"), "pollutant", "time_utc", "value")
        .collect()
    )
    if not rows:
        raise ValueError(f"No archive data for {fi['city']} in the replay window — run the batch path first")
    values = {(r["station_code"], r["pollutant"], r["time_utc"]): r["value"] for r in rows}
    stations = sorted({k[0] for k in values})

    out_dir, reg_dir = Path(st.landing("fault_replay")), Path(st.landing("fault_replay_registry"))
    for d in (out_dir, reg_dir):
        shutil.rmtree(d, ignore_errors=True)
    batches = fault_replay.build_batches(values, stations, start, fi["hours"], fi["schedule"], fi["send_delay_minutes"])
    for b in batches:
        gios_live.write_batch(out_dir, "fault_replay", b.records, b.fetched_at, b.previous_fetched_at, name=b.name)
    snapshot = gios_live.latest_snapshot(st.live_landing("gios_registry"))
    variants = fault_replay.registry_variants(snapshot, city["gios_city_name"], start, fi["schedule"])
    for ts, stations_variant in variants:
        gios_registry.write_snapshot(reg_dir, stations_variant, now=ts, source="fault_replay")
    log.info("fault-replay: %d stations, %d batches, %d records, %d registry snapshots",
             len(stations), len(batches), sum(len(b.records) for b in batches), len(variants))


def step_weather_forecast_live(ctx: Context) -> None:
    """Current weather forecast for the configured cities (input for tomorrow's PM forecast)."""
    if _synthetic(ctx, "weather"):
        now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
        files = synthetic.write_weather_live(ctx.cfg, ctx.storage.landing("weather"), now)
    else:
        files = download_live(ctx.cfg, ctx.storage.landing("weather"))
    log.info("weather-forecast-live: %d cities", len(files))

