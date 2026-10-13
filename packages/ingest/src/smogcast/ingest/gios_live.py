"""Live PM measurements from the GIOŚ API → landing/gios_live/batch_<UTC timestamp>.json.

Each poll asks ``data/getData/{sensor}`` for every PM10/PM2.5 sensor of the configured cities
(sensor list = newest registry snapshot). The API returns roughly the last three days, so most
records repeat between polls — deduplication and late-arrival rules live in silver_clean.

Batch file contract (shared with the fault-injection replay, so both feed the same stream):
    {"source": "live", "fetched_at": "<UTC ISO>", "previous_fetched_at": "<UTC ISO or null>",
     "records": [{"position_code", "station_code", "pollutant", "sensor_id",
                  "time_local_end": "YYYY-MM-DD HH:MM:SS" (END of hour, Europe/Warsaw),
                  "value": float or null, "sent_at": "<UTC ISO>"}]}
``previous_fetched_at`` lets the stream tell a genuinely late value (it should have been
available at the previous poll) from a first-run backfill.
"""

from __future__ import annotations

import datetime as dt
import glob
import json
import logging
from collections.abc import Callable
from pathlib import Path

import requests

from smogcast.ingest.gios_registry import get_json

log = logging.getLogger(__name__)

DATA_KEY = "Lista danych pomiarowych"


def latest_snapshot(registry_dir: str) -> dict:
    files = sorted(glob.glob(str(Path(registry_dir) / "snapshot_*.json")))
    if not files:
        raise FileNotFoundError(f"No registry snapshot in {registry_dir} — run `smogcast-ingest --step gios-registry`")
    return json.loads(Path(files[-1]).read_text(encoding="utf-8"))


def pm_sensors(snapshot: dict, pollutant_codes: set[str]) -> list[dict]:
    out = []
    for st in snapshot["stations"]:
        for s in st.get("sensors", []):
            if s.get("Wskaźnik - kod") in pollutant_codes:
                out.append({"sensor_id": int(s["Identyfikator stanowiska"]), "station_code": st["Kod stacji"],
                            "pollutant": s["Wskaźnik - kod"]})
    return out


def _reason(response) -> str:
    try:
        return response.json().get("error_reason", "")[:120]
    except ValueError:
        return ""


def previous_fetch(batch_dir: str) -> str | None:
    files = sorted(glob.glob(str(Path(batch_dir) / "batch_*.json")))
    return json.loads(Path(files[-1]).read_text(encoding="utf-8"))["fetched_at"] if files else None


def poll(base_url: str, sensors: list[dict], fetched_at: dt.datetime,
         fetch: Callable[[str], dict] = get_json, skipped: list[int] | None = None) -> list[dict]:
    """Records of all sensors. A sensor the API refuses (HTTP 400) is skipped and reported in
    ``skipped``: manual (24 h, laboratory) positions publish results only after 4–8 weeks."""
    sent = fetched_at.isoformat(timespec="seconds")
    records = []
    for s in sensors:
        try:
            body = fetch(f"{base_url}/data/getData/{s['sensor_id']}?size=500")
        except requests.HTTPError as exc:
            if getattr(exc.response, "status_code", None) != 400:
                raise
            if skipped is not None:
                skipped.append(s["sensor_id"])
            log.info("sensor %s: no live data (%s)", s["sensor_id"], _reason(exc.response))
            continue
        for r in body.get(DATA_KEY, []):
            records.append({
                "position_code": r.get("Kod stanowiska"), "station_code": s["station_code"],
                "pollutant": s["pollutant"], "sensor_id": s["sensor_id"],
                "time_local_end": r.get("Data"), "value": r.get("Wartość"), "sent_at": sent,
            })
    return records


def batch_document(source: str, records: list[dict], fetched_at: dt.datetime, previous_fetched_at: str | None,
                   name: str | None = None) -> tuple[str, str]:
    """(file name, JSON text) of one poll — written to a folder here, to blob storage by the Azure Function."""
    text = json.dumps({"source": source, "fetched_at": fetched_at.isoformat(timespec="seconds"),
                       "previous_fetched_at": previous_fetched_at, "records": records})
    return name or f"batch_{fetched_at:%Y%m%dT%H%M%SZ}.json", text


def write_batch(batch_dir: str | Path, source: str, records: list[dict], fetched_at: dt.datetime,
                previous_fetched_at: str | None, name: str | None = None) -> Path:
    name, text = batch_document(source, records, fetched_at, previous_fetched_at, name)
    target = Path(batch_dir) / name
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".json.part")
    tmp.write_text(text, encoding="utf-8")
    # Atomic rename: the stream never sees a half-written file.
    tmp.replace(target)
    return target
