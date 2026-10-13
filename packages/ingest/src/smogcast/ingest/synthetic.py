"""Deterministic synthetic inputs for integration tests, CI and `--offline` runs — never real data.

The generator writes the SAME files as the real ingest, so every later step runs unchanged; that is what
an integration test needs:

    landing/gios_archive/raw/<year>.zip     GIOŚ wide sheets <year>_PM10_1g.xlsx, <year>_PM25_1g.xlsx
    landing/gios_archive/raw/metadata.xlsx  sheets STACJE and STANOWISKA
    landing/weather/<source>/<city>/<year>.json, landing/weather/live/<city>/<stamp>.json   Open-Meteo payloads
    landing/gios_registry/snapshot_*.json   API station registry
    landing/gios_live/batch_*.json          one API poll (the last ~3 days)

Every value is a pure function of (seed, station or city, time): the archive and a live poll agree on the
same hour, and a re-run writes identical content. Each city has one hidden "true weather" made of
overlapping slow waves (so days are correlated, like real weather); PM follows it — cold means heating,
calm wind keeps smoke in the city, rain washes it out — and the archived forecasts are that truth plus a
source-dependent error. The model therefore has something to learn, as with real data. Numbers are
plausible, not real.
"""

from __future__ import annotations

import datetime as dt
import functools
import json
import math
import random
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo

from smogcast.core.schema import (
    GIOS_HOURLY_HEADER_LABELS,
    GIOS_POSITION_COLUMNS,
    GIOS_POSITIONS_SHEET,
    GIOS_STATION_COLUMNS,
    GIOS_STATIONS_SHEET,
)
from smogcast.ingest.gios_live import write_batch
from smogcast.ingest.gios_registry import write_snapshot

WARSAW = ZoneInfo("Europe/Warsaw")
CET = dt.timedelta(hours=1)  # the archive is stamped in CET all year (no daylight saving)
TWO_PI = 2 * math.pi
EPOCH = dt.datetime(2020, 1, 1)
WAVE_PERIODS_DAYS = (2.7, 4.3, 7.1, 13.0)  # overlapping waves: irregular but autocorrelated series


# ------------------------------------------------------------------------------------------- stations
@dataclass(frozen=True)
class Station:
    city_id: str
    index: int
    station_id: int
    code: str
    factor: float  # some stations (e.g. next to a busy street) read higher than others


def stations(cfg: dict) -> list[Station]:
    """Stations of the configured cities; the fault-injection city gets as many as its schedule needs."""
    syn, fi = cfg["synthetic"], cfg["fault_injection"]
    # every station index the schedule refers to (station_index, remove_station_index, add_sensor_station_index …)
    needed = 1 + max((v for e in fi["schedule"] for k, v in e.items() if k.endswith("station_index")), default=0)
    out = []
    for c_no, city_id in enumerate(cfg["run"]["cities"]):
        n = max(syn["stations_per_city"], needed if city_id == fi["city"] else 0)
        for i in range(n):
            rng = random.Random(f"{cfg['run']['seed']}|station|{city_id}|{i}")
            out.append(Station(city_id, i, 9000 + 100 * c_no + i, f"Syn{city_id[:4].capitalize()}{i + 1:02d}",
                               round(rng.uniform(0.85, 1.25), 3)))
    return out


def position_code(station: Station, gios_code: str) -> str:
    return f"{station.code}-{gios_code}-1g"


# ------------------------------------------------------------------------------------------- signals
@functools.cache
def _phases(seed: int, key: str) -> tuple[float, ...]:
    rng = random.Random(f"{seed}|wave|{key}")
    return tuple(rng.uniform(0, TWO_PI) for _ in WAVE_PERIODS_DAYS)


def _wave(seed: int, key: str, ts_utc: dt.datetime) -> float:
    """Smooth pseudo-random series, roughly in [-1.5, 1.5], deterministic in (seed, key, time)."""
    t = (ts_utc - EPOCH).total_seconds() / 86400
    return sum(math.sin(TWO_PI * t / p + f) for p, f in zip(WAVE_PERIODS_DAYS, _phases(seed, key), strict=True)) / 2


def true_weather(seed: int, city_id: str, ts_utc: dt.datetime) -> dict[str, float]:
    """The hidden 'real' weather of a city in one hour (Open-Meteo variable names)."""
    season = -math.cos(TWO_PI * (ts_utc.timetuple().tm_yday - 20) / 365.25)  # -1 mid-January … +1 mid-July
    hour = ts_utc.hour + ts_utc.minute / 60
    diurnal = math.sin(TWO_PI * (hour - 8) / 24)  # warmest and windiest mid-afternoon
    wet = _wave(seed, f"{city_id}|wet", ts_utc)
    cloud = min(100.0, max(0.0, 55 + 35 * wet))
    daylight = max(0.0, math.sin(math.pi * (hour - 5) / 13)) if 5 < hour < 18 else 0.0
    return {
        "temperature_2m": 8 + 11 * season + 4 * _wave(seed, f"{city_id}|temp", ts_utc) + 3 * diurnal,
        "relative_humidity_2m": min(100.0, max(20.0, 78 - 12 * season + 12 * wet - 10 * diurnal)),
        "wind_speed_10m": max(0.2, 3.0 + 2.0 * _wave(seed, f"{city_id}|wind", ts_utc) + 0.8 * diurnal),
        "wind_direction_10m": (240 + 90 * _wave(seed, f"{city_id}|dir", ts_utc)) % 360,
        "precipitation": max(0.0, 1.4 * wet - 0.7),
        "surface_pressure": 1000 + 9 * _wave(seed, f"{city_id}|pres", ts_utc),
        "cloud_cover": cloud,
        "shortwave_radiation": (300 + 250 * season) * daylight * (1 - cloud / 140),
    }


def pm_value(seed: int, level: float, station: Station, gios_code: str, ts_utc: dt.datetime) -> float:
    """Hourly concentration at a station [µg/m³]: smoke from the city's weather, a station factor and noise."""
    w = true_weather(seed, station.city_id, ts_utc)
    heating = max(0.0, 12 - w["temperature_2m"]) / 12
    calm = 1 / (0.45 + w["wind_speed_10m"] / 3)
    wash = 1 / (1 + 2 * w["precipitation"])
    local_hour = ts_utc.replace(tzinfo=dt.timezone.utc).astimezone(WARSAW).hour  # ts_utc: naive UTC
    evening = 1 + 0.35 * math.sin(TWO_PI * (local_hour - 13) / 24)  # stoves at night, peak in the evening
    pm25 = level * (0.35 + 1.6 * heating) * calm * wash * evening * station.factor
    rng = random.Random(f"{seed}|pm|{station.code}|{gios_code}|{ts_utc:%Y%m%d%H}")
    noise = math.exp(rng.gauss(0, 0.15))
    if gios_code == "PM2.5":
        return round(pm25 * noise, 1)
    coarse = 6.0 if w["precipitation"] == 0 else 1.0  # dust from dry streets — PM10 only
    return round((1.25 * pm25 + coarse) * noise, 1)


def _missing(seed: int, share: float, *key) -> bool:
    return random.Random(f"{seed}|missing|{'|'.join(map(str, key))}").random() < share


def _level(cfg: dict, city_id: str) -> float:
    levels = cfg["synthetic"]["pm25_level"]
    return float(levels.get(city_id, levels["default"]))


# ------------------------------------------------------------------------------------------- archive
def archive_hours(year: int, months: list[int]) -> list[dt.datetime]:
    """UTC start of every hour of the selected months, by CET calendar day (as the archive counts them)."""
    out = []
    day = dt.date(year, 1, 1)
    while day.year == year:
        if day.month in months:
            first = dt.datetime.combine(day, dt.time()) - CET  # 00:00 CET of that day, in UTC
            out += [first + dt.timedelta(hours=h) for h in range(24)]
        day += dt.timedelta(days=1)
    return out


def write_archive(cfg: dict, raw_dir: str | Path) -> list[Path]:
    """Yearly zips with GIOŚ hourly sheets + the metadata workbook; existing files are kept (like a cache)."""
    import openpyxl

    raw = Path(raw_dir)
    raw.mkdir(parents=True, exist_ok=True)
    seed, syn = cfg["run"]["seed"], cfg["synthetic"]
    sts = stations(cfg)
    written = []
    meta = raw / "metadata.xlsx"
    if not meta.exists():
        _write_metadata(cfg, sts, meta)
        written.append(meta)
    for year in cfg["run"]["archive_years"]:
        target = raw / f"{year}.zip"
        if target.exists():
            continue
        hours = archive_hours(year, syn["months"])
        with tempfile.TemporaryDirectory() as tmp, zipfile.ZipFile(target.with_suffix(".zip.part"), "w") as z:
            for p in cfg["pollutants"].values():
                code, token = p["gios_code"], p["archive_token"]
                wb = openpyxl.Workbook(write_only=True)
                ws = wb.create_sheet()
                header_values = [
                    list(range(1, len(sts) + 1)), [s.code for s in sts], [code] * len(sts), ["1g"] * len(sts),
                    ["ug/m3"] * len(sts), [position_code(s, code) for s in sts],
                ]
                for label, values in zip(GIOS_HOURLY_HEADER_LABELS, header_values, strict=True):
                    ws.append([label, *values])
                for ts in hours:
                    row = [None if _missing(seed, syn["missing_share"], s.code, code, ts)
                           else pm_value(seed, _level(cfg, s.city_id), s, code, ts) for s in sts]
                    ws.append([ts + 2 * CET, *row])  # stamped as the END of the hour in CET
                name = f"{year}_{token}_1g.xlsx"
                wb.save(Path(tmp) / name)
                z.write(Path(tmp) / name, name)
        target.with_suffix(".zip.part").replace(target)
        written.append(target)
    return written


def _write_metadata(cfg: dict, sts: list[Station], target: Path) -> None:
    import openpyxl

    wb = openpyxl.Workbook(write_only=True)
    opened = dt.datetime(2015, 1, 1)
    st_ws = wb.create_sheet(GIOS_STATIONS_SHEET)
    st_ws.append(list(GIOS_STATION_COLUMNS))
    for s in sts:
        city = cfg["cities"][s.city_id]
        values = {
            "Kod stacji": s.code, "Kod międzynarodowy": f"PL{s.station_id}A", "Nazwa stacji": f"{city['name']} synthetic {s.index + 1}",
            "Data uruchomienia": opened, "Typ stacji": "tło", "Typ obszaru": "miejski", "Rodzaj stacji": "automatyczna",
            "Województwo": city["voivodeship"].upper(), "Miejscowość": city["gios_city_name"],
            "WGS84 φ N": city["lat"] + 0.01 * s.index, "WGS84 λ E": city["lon"] + 0.01 * s.index,
        }
        st_ws.append([values.get(col) for col in GIOS_STATION_COLUMNS])
    pos_ws = wb.create_sheet(GIOS_POSITIONS_SHEET)
    pos_ws.append(list(GIOS_POSITION_COLUMNS))
    for s in sts:
        for p in cfg["pollutants"].values():
            values = {"Kod stanowiska": position_code(s, p["gios_code"]), "Kod stacji": s.code,
                      "Wskaźnik - kod": p["gios_code"], "Czas uśredniania": "1g", "Typ pomiaru": "automatyczny",
                      "Data uruchomienia": opened}
            pos_ws.append([values.get(col) for col in GIOS_POSITION_COLUMNS])
    tmp = target.with_suffix(".xlsx.part")
    wb.save(tmp)
    tmp.replace(target)


# ------------------------------------------------------------------------------------------- weather
def _forecast(seed: int, city_id: str, source: str, error: float, ts: dt.datetime) -> dict[str, float]:
    """True weather plus a smooth, source-specific error (fresher forecasts err less)."""
    w = true_weather(seed, city_id, ts)

    def err(var: str, scale: float) -> float:
        return error * scale * _wave(seed, f"{city_id}|{source}|err|{var}", ts)

    return {
        "temperature_2m": w["temperature_2m"] + err("t", 1.5),
        "relative_humidity_2m": min(100.0, max(5.0, w["relative_humidity_2m"] + err("rh", 6))),
        "wind_speed_10m": max(0.0, w["wind_speed_10m"] * (1 + err("ws", 0.25))),
        "wind_direction_10m": (w["wind_direction_10m"] + err("wd", 25)) % 360,
        "precipitation": max(0.0, w["precipitation"] + err("p", 0.3)),
        "surface_pressure": w["surface_pressure"] + err("ps", 2),
        "cloud_cover": min(100.0, max(0.0, w["cloud_cover"] + err("c", 15))),
        "shortwave_radiation": max(0.0, w["shortwave_radiation"] * (1 + err("r", 0.2))),
    }


def _weather_record(cfg: dict, city_id: str, source: str, hours: list[dt.datetime], fetched_at: dt.datetime) -> dict:
    seed, w = cfg["run"]["seed"], cfg["weather"]
    suffix = w["sources"][source]["variable_suffix"]
    error = cfg["synthetic"]["forecast_error"][source]
    city = cfg["cities"][city_id]
    rows = [_forecast(seed, city_id, source, error, ts) for ts in hours]
    hourly = {"time": [f"{ts:%Y-%m-%dT%H:%M}" for ts in hours]}
    for v in w["hourly_variables"]:
        hourly[v + suffix] = [round(r[v], 2) for r in rows]
    units = {"time": "iso8601", **{v + suffix: "m/s" if v == "wind_speed_10m" else "" for v in w["hourly_variables"]}}
    return {
        "city_id": city_id, "source": source, "variable_suffix": suffix,
        "requested_lat": city["lat"], "requested_lon": city["lon"],
        "fetched_at": fetched_at.isoformat(timespec="seconds"),
        "payload": {"latitude": round(city["lat"], 2), "longitude": round(city["lon"], 2), "utc_offset_seconds": 0,
                    "hourly_units": units, "hourly": hourly},
    }


def _write_json(target: Path, record: dict) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".json.part")
    tmp.write_text(json.dumps(record), encoding="utf-8")
    tmp.replace(target)
    return target


def write_weather_history(cfg: dict, weather_dir: str | Path) -> list[Path]:
    """Archived forecasts for every source, city and archive year (same layout as the Open-Meteo download)."""
    written = []
    for source in ("short_range", "day_ahead"):
        first_year = cfg["weather"]["sources"][source]["first_year"]
        for city_id in cfg["run"]["cities"]:
            for year in (y for y in cfg["run"]["archive_years"] if y >= first_year):
                target = Path(weather_dir) / source / city_id / f"{year}.json"
                if target.exists():
                    continue
                # One CET day more on both sides, as an Open-Meteo year in UTC covers the whole CET year.
                hours = [ts for ts in _utc_hours(dt.datetime(year, 1, 1) - 2 * CET, dt.datetime(year + 1, 1, 1))
                         if (ts + CET).month in cfg["synthetic"]["months"]]
                fetched = dt.datetime(year + 1, 1, 2, tzinfo=dt.timezone.utc)
                written.append(_write_json(target, _weather_record(cfg, city_id, source, hours, fetched)))
    return written


def write_weather_live(cfg: dict, weather_dir: str | Path, now: dt.datetime) -> list[Path]:
    """Today's forecast for today and the next two days (UTC), one file per city — like `download_live`.
    ``now``: timezone-aware UTC, as in the real ingest steps."""
    start = dt.datetime.combine(now.date(), dt.time())
    hours = _utc_hours(start, start + dt.timedelta(days=3))
    return [_write_json(Path(weather_dir) / "live" / city_id / f"{now:%Y%m%dT%H%M%SZ}.json",
                        _weather_record(cfg, city_id, "live", hours, now))
            for city_id in cfg["run"]["cities"]]


def _utc_hours(start: dt.datetime, end: dt.datetime) -> list[dt.datetime]:
    n = int((end - start).total_seconds() // 3600)
    return [start + dt.timedelta(hours=h) for h in range(n)]


# ------------------------------------------------------------------------------------------- live API
def registry_stations(cfg: dict) -> list[dict]:
    """The station registry as the GIOŚ API returns it (Polish field names), with each station's sensors."""
    out = []
    for s in stations(cfg):
        city = cfg["cities"][s.city_id]
        out.append({
            "Identyfikator stacji": s.station_id, "Kod stacji": s.code, "Nazwa stacji": f"{city['name']} synthetic {s.index + 1}",
            "WGS84 φ N": f"{city['lat'] + 0.01 * s.index:.6f}", "WGS84 λ E": f"{city['lon'] + 0.01 * s.index:.6f}",
            "Nazwa miasta": city["gios_city_name"], "Województwo": city["voivodeship"].upper(),
            "sensors": [{"Identyfikator stanowiska": s.station_id * 10 + k, "Identyfikator stacji": s.station_id,
                         "Wskaźnik": p["gios_code"], "Wskaźnik - kod": p["gios_code"]}
                        for k, p in enumerate(cfg["pollutants"].values())],
        })
    return out


def write_registry(cfg: dict, registry_dir: str | Path, now: dt.datetime) -> Path:
    return write_snapshot(registry_dir, registry_stations(cfg), now=now, source="gios_api")


def live_records(cfg: dict, now: dt.datetime, hours: int = 72) -> list[dict]:
    """One poll: every sensor's last ``hours`` hours, stamped as the END of the hour in local time like
    the API; the newest hour is still empty (GIOŚ publishes with a delay). ``now``: timezone-aware UTC."""
    seed, syn = cfg["run"]["seed"], cfg["synthetic"]
    now_utc = now.astimezone(dt.timezone.utc).replace(tzinfo=None, minute=0, second=0, microsecond=0)
    sent = now.isoformat(timespec="seconds")
    records = []
    for s in stations(cfg):
        for k, p in enumerate(cfg["pollutants"].values()):
            code = p["gios_code"]
            for h in range(1, hours + 1):
                start = now_utc - dt.timedelta(hours=h)
                end_local = (start + dt.timedelta(hours=1)).replace(tzinfo=dt.timezone.utc).astimezone(WARSAW)
                empty = h == 1 or _missing(seed, syn["missing_share"], s.code, code, start)
                records.append({
                    "position_code": position_code(s, code), "station_code": s.code, "pollutant": code,
                    "sensor_id": s.station_id * 10 + k, "time_local_end": f"{end_local:%Y-%m-%d %H:%M:%S}",
                    "value": None if empty else pm_value(seed, _level(cfg, s.city_id), s, code, start),
                    "sent_at": sent,
                })
    return records


def write_live_batch(cfg: dict, batch_dir: str | Path, now: dt.datetime, previous_fetched_at: str | None) -> Path:
    return write_batch(batch_dir, "live", live_records(cfg, now), now, previous_fetched_at)
