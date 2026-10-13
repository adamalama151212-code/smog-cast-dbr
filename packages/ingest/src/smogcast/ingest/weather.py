"""Open-Meteo forecast archives → landing/weather/<source>/<city_id>/<year>.json.

Safeguards carried over from the radplume ingest (each one caught a real bug once):
1. wind unit forced (``wind_speed_unit=ms``) AND checked in the response — otherwise the API
   returns km/h and the model would silently see 3.6× stronger wind;
2. time in UTC (``timezone=GMT``) and checked (``utc_offset_seconds == 0``);
3. requested and returned coordinates are both stored (the API snaps to its grid).
A response that breaks the contract is never written to landing.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import time
from pathlib import Path

import requests

log = logging.getLogger(__name__)


class WeatherValidationError(ValueError):
    """The API response breaks the data contract — the file is NOT written to landing."""


def build_params(lat: float, lon: float, start: str, end: str, variables: list[str], suffix: str) -> dict:
    return {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "hourly": ",".join(v + suffix for v in variables),
        "wind_speed_unit": "ms",  # the default is km/h!
        "timezone": "GMT",        # everything in UTC
    }


def validate_payload(payload: dict, variables: list[str], suffix: str) -> None:
    """Data contract. Rejecting a file is better than "fixing" it silently.

    Converting km/h → m/s on the fly would look convenient but hide a broken request.
    """
    units = payload.get("hourly_units", {})
    wind = "wind_speed_10m" + suffix
    if wind in units and units[wind] != "m/s":
        raise WeatherValidationError(f"{wind}: unit {units[wind]!r}, expected 'm/s' (is wind_speed_unit=ms in the request?)")
    if payload.get("utc_offset_seconds", 0) != 0:
        raise WeatherValidationError("Expected UTC (utc_offset_seconds = 0)")
    hourly = payload.get("hourly", {})
    n = len(hourly.get("time", []))
    if n == 0:
        raise WeatherValidationError("Empty response (no hours)")
    for v in variables:
        if len(hourly.get(v + suffix, [])) != n:
            raise WeatherValidationError(f"{v + suffix}: length {len(hourly.get(v + suffix, []))} != {n}")


class NotJsonResponse(requests.RequestException):
    """A successful status with a body that is not JSON — transient, retried like 429/5xx."""


def fetch(url: str, params: dict, retries: int = 6, delay: float = 5.0) -> dict:
    """GET with exponential backoff — the API is occasionally overloaded (HTTP 429/5xx).

    Six attempts wait 5 + 10 + 20 + 40 + 80 s in total, longer than Open-Meteo's per-minute rate window.
    """
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=180)
            if resp.status_code == 429 or resp.status_code >= 500:
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
            resp.raise_for_status()
            try:
                return resp.json()
            except ValueError as exc:
                # Seen on Databricks (2026-10-06): the first full archive download (100 requests in one go)
                # got a non-JSON body after ~35 files and the task failed without a retry.
                raise NotJsonResponse(f"HTTP {resp.status_code}, body is not JSON: {resp.text[:200]!r}") from exc
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError, NotJsonResponse) as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status is not None and 400 <= status < 500 and status != 429:
                raise
            if attempt == retries:
                raise
            log.warning("Open-Meteo: attempt %d failed (%s), retrying in %.0fs", attempt, exc, delay)
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("unreachable")


def landing_file(weather_dir: str | Path, source: str, city_id: str, year: int) -> Path:
    return Path(weather_dir) / source / city_id / f"{year}.json"


def download_history(cfg: dict, weather_dir: str, today: dt.date | None = None) -> list[Path]:
    """Download the short_range and day_ahead archives for every city and archive year.

    Complete past years are cached; the current year is re-downloaded (it is still growing).
    """
    today = today or dt.date.today()
    w = cfg["weather"]
    variables = w["hourly_variables"]
    new: list[Path] = []
    for source in ("short_range", "day_ahead"):
        src = w["sources"][source]
        years = [y for y in cfg["run"]["archive_years"] if y >= src["first_year"]]
        for city_id in cfg["run"]["cities"]:
            city = cfg["cities"][city_id]
            for year in years:
                target = landing_file(weather_dir, source, city_id, year)
                if target.exists() and year < today.year:
                    continue
                end = min(dt.date(year, 12, 31), today - dt.timedelta(days=1))
                params = build_params(city["lat"], city["lon"], f"{year}-01-01", end.isoformat(), variables, src["variable_suffix"])
                payload = fetch(src["url"], params)
                validate_payload(payload, variables, src["variable_suffix"])
                target.parent.mkdir(parents=True, exist_ok=True)
                record = {
                    "city_id": city_id,
                    "source": source,
                    "variable_suffix": src["variable_suffix"],
                    "requested_lat": city["lat"],
                    "requested_lon": city["lon"],
                    "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                    "payload": payload,
                }
                tmp = target.with_suffix(".json.part")
                tmp.write_text(json.dumps(record), encoding="utf-8")
                tmp.replace(target)
                new.append(target)
                log.info("weather %s %s %s: %d hours", source, city_id, year, len(payload["hourly"]["time"]))
    return new


def download_live(cfg: dict, weather_dir: str, now: dt.datetime | None = None) -> list[Path]:
    """Current forecast (today + 2 days) for every city → landing/weather/live/<city>/<UTC stamp>.json.

    Every run keeps its own file: the same hour appears in several forecasts and silver keeps the
    newest one per hour (``fetched_at``), so the day-ahead input is the latest issued forecast.
    """
    now = now or dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    w = cfg["weather"]
    src = w["sources"]["live"]
    out = []
    for city_id in cfg["run"]["cities"]:
        city = cfg["cities"][city_id]
        params = build_params(city["lat"], city["lon"], now.date().isoformat(),
                              (now.date() + dt.timedelta(days=2)).isoformat(), w["hourly_variables"], src["variable_suffix"])
        payload = fetch(src["url"], params)
        validate_payload(payload, w["hourly_variables"], src["variable_suffix"])
        target = Path(weather_dir) / "live" / city_id / f"{now:%Y%m%dT%H%M%SZ}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        record = {"city_id": city_id, "source": "live", "variable_suffix": src["variable_suffix"],
                  "requested_lat": city["lat"], "requested_lon": city["lon"],
                  "fetched_at": now.isoformat(timespec="seconds"), "payload": payload}
        tmp = target.with_suffix(".json.part")
        tmp.write_text(json.dumps(record), encoding="utf-8")
        tmp.replace(target)
        out.append(target)
    return out
