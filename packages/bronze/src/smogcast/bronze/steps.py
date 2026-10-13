"""Bronze steps: each is ``(Context) -> None`` — landing files in, bronze Delta tables out."""

from __future__ import annotations

import glob
import logging
from pathlib import Path

from smogcast.bronze import pm_archive, station_meta, station_snapshots, weather
from smogcast.core.context import Context
from smogcast.core.dq_metrics import log_metrics
from smogcast.core.schema import GIOS_POSITIONS_SHEET, GIOS_STATIONS_SHEET

log = logging.getLogger(__name__)


def step_pm_hourly(ctx: Context) -> None:
    """GIOŚ yearly archive zips → bronze.pm_hourly (all stations, PM10 and PM2.5, hourly).

    Idempotent per year: the table is partitioned by ``source_year`` and each processed year
    replaces exactly its own partition (replaceWhere), so re-runs never duplicate rows.
    """
    st, cfg = ctx.storage, ctx.cfg
    raw_dir = Path(st.landing("gios_archive", "raw"))
    long_dir = Path(st.landing("gios_archive", "long"))
    tokens = [p["archive_token"] for p in cfg["pollutants"].values()]
    years = cfg["run"]["archive_years"]

    metrics: dict[str, float] = {}
    paths: list[str] = []
    for year in years:
        zip_file = raw_dir / f"{year}.zip"
        if not zip_file.exists():
            raise FileNotFoundError(f"{zip_file} missing — run `smogcast-ingest --step gios-archive` first")
        for token, s in pm_archive.extract_year(zip_file, year, tokens, long_dir).items():
            for k, v in vars(s).items():
                metrics[f"{year}_{token}_{k}"] = v
        paths += [str(pm_archive.long_csv_path(long_dir, year, t)) for t in tokens]

    df = pm_archive.read_long(ctx.spark, paths)
    years_sql = ", ".join(str(y) for y in years)
    st.overwrite(df, "bronze", "pm_hourly", partition_by=["source_year"], replace_where=f"source_year IN ({years_sql})")

    written = st.read("bronze", "pm_hourly").where(f"source_year IN ({years_sql})")
    for r in written.groupBy("source_year", "pollutant").count().collect():
        metrics[f"rows_{r['source_year']}_{r['pollutant']}"] = r["count"]
    log_metrics(st, "bronze_pm_hourly", metrics)


def step_station_meta(ctx: Context) -> None:
    """GIOŚ metadata xlsx → bronze.gios_stations, bronze.gios_positions (full refresh; small tables)."""
    st = ctx.storage
    xlsx = Path(st.landing("gios_archive", "raw", "metadata.xlsx"))
    if not xlsx.exists():
        raise FileNotFoundError(f"{xlsx} missing — run `smogcast-ingest --step gios-archive` first")
    for sheet, columns, table in (
        (GIOS_STATIONS_SHEET, station_meta.STATION_COLUMNS, "gios_stations"),
        (GIOS_POSITIONS_SHEET, station_meta.POSITION_COLUMNS, "gios_positions"),
    ):
        rows = station_meta.read_sheet(xlsx, sheet, columns)
        st.overwrite(station_meta.to_frame(ctx.spark, rows, columns, str(xlsx)), "bronze", table)
        log_metrics(st, f"bronze_{table}", {"rows": len(rows)})


def step_weather_forecast(ctx: Context) -> None:
    """Open-Meteo landing JSON (short_range / day_ahead archives, live forecasts) → bronze.weather_forecast.

    Full overwrite: the table is small (cities × hours × 2 sources, well under a million rows)
    and rebuilding it from landing is the simplest idempotent strategy.
    """
    st, w = ctx.storage, ctx.cfg["weather"]
    frames = []
    for source in ("short_range", "day_ahead", "live"):
        paths = sorted(glob.glob(st.landing("weather", source, "*", "*.json")))
        if not paths:
            log.warning("weather-forecast: no landing files for %s", source)
            continue
        frames.append(weather.read_source(ctx.spark, paths, w["hourly_variables"], w["sources"][source]["variable_suffix"]))
    if not frames:
        raise FileNotFoundError("No weather files in landing — run `smogcast-ingest --step weather-forecast-history` first")
    df = frames[0]
    for f in frames[1:]:
        df = df.unionByName(f)
    st.overwrite(df, "bronze", "weather_forecast")

    out = st.read("bronze", "weather_forecast")
    metrics = {f"rows_{r['source']}": r["count"] for r in out.groupBy("source").count().collect()}
    # Hours the API returned without values (day_ahead before 2024-01-19 12:00 UTC is all null).
    metrics.update({f"null_hours_{r['source']}": r["count"]
                    for r in out.where("temperature_2m IS NULL").groupBy("source").count().collect()})
    log_metrics(st, "bronze_weather_forecast", metrics)


def step_station_snapshots(ctx: Context) -> None:
    """Registry snapshots (real API and fault-replay variants) → bronze.station_snapshots (full rebuild)."""
    st = ctx.storage
    # Real snapshots come from the API poll (live landing zone), the replay's variants from the landing zone.
    patterns = [st.live_landing("gios_registry", "snapshot_*.json"), st.landing("fault_replay_registry", "snapshot_*.json")]
    paths = sorted(Path(p) for pattern in patterns for p in glob.glob(pattern))
    if not paths:
        raise FileNotFoundError("No registry snapshots in landing — run `smogcast-ingest --step gios-registry` first")
    st.overwrite(station_snapshots.read_snapshots(ctx.spark, paths), "bronze", "station_snapshots")
    log_metrics(st, "bronze_station_snapshots", {"snapshots": len(paths)})
