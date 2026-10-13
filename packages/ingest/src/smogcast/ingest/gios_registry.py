"""GIOŚ station registry snapshot → landing/gios_registry/snapshot_<UTC timestamp>.json.

The API has no change feed, so CDC is built from SNAPSHOTS: each run stores the full list of
stations of the configured cities with their measurement positions (sensors), exactly as the
API returned them (Polish field names). Comparing consecutive snapshots (silver_clean,
stations-scd2) yields INSERT / UPDATE / DELETE changes and an SCD2 history.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import time
from collections.abc import Callable
from pathlib import Path

import requests

log = logging.getLogger(__name__)

STATIONS_KEY = "Lista stacji pomiarowych"
SENSORS_KEY = "Lista stanowisk pomiarowych dla podanej stacji"
CITY_FIELD = "Nazwa miasta"
STATION_ID_FIELD = "Identyfikator stacji"


def get_json(url: str, retries: int = 4) -> dict:
    delay = 2.0
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=60, headers={"User-Agent": "smogcast/0.1"})
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}", response=r)
            r.raise_for_status()
            return r.json()
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if (status is not None and 400 <= status < 500 and status != 429) or attempt == retries:
                raise
            log.warning("GIOŚ API: attempt %d failed (%s), retrying in %.0fs", attempt, exc, delay)
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("unreachable")


def fetch_snapshot(base_url: str, city_names: set[str], fetch: Callable[[str], dict] = get_json) -> list[dict]:
    """Stations of the given cities, each with its raw ``sensors`` list."""
    stations, page = [], 0
    while True:
        body = fetch(f"{base_url}/station/findAll?page={page}&size=500")
        stations += body.get(STATIONS_KEY, [])
        page += 1
        if page >= int(body.get("totalPages", 1)):
            break
    selected = [s for s in stations if s.get(CITY_FIELD) in city_names]
    for s in selected:
        s["sensors"] = fetch(f"{base_url}/station/sensors/{s[STATION_ID_FIELD]}").get(SENSORS_KEY, [])
    return selected


def snapshot_document(stations: list[dict], now: dt.datetime, source: str = "gios_api") -> tuple[str, str]:
    """(file name, JSON text) of a registry snapshot — written to a folder here, to blob storage by the
    Azure Function (functions/gios_poller)."""
    text = json.dumps({"snapshot_ts": now.isoformat(timespec="seconds"), "source": source,
                       "stations": stations}, ensure_ascii=False)
    return f"snapshot_{now:%Y%m%dT%H%M%SZ}.json", text


def write_snapshot(landing_dir: str | Path, stations: list[dict], now: dt.datetime | None = None,
                   source: str = "gios_api") -> Path:
    name, text = snapshot_document(stations, now or dt.datetime.now(dt.timezone.utc), source)
    target = Path(landing_dir) / name
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".json.part")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(target)
    return target
