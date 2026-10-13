"""One hourly poll of the GIOŚ API — registry snapshot + live PM batch — written to any file store.

On Databricks the polling runs OUTSIDE the cluster, in an Azure Function (functions/gios_poller): starting a
cluster every hour costs minutes for ~20 s of HTTP calls. The function writes to a blob container that Unity
Catalog exposes as the external volume raw.landing_live, where the stream pipeline's Auto Loader picks the
files up. Locally the steps `ingest gios-registry` and `ingest gios-live` do the same with the same modules.

The store is anything with ``names(prefix)``, ``read_text(path)`` and ``write_text(path, text)`` — a folder
(``FolderStore``, tests) or a blob container (the function's adapter). Paths are relative:
``gios_registry/snapshot_<ts>.json`` and ``gios_live/batch_<ts>.json``, the same layout as the landing zone.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Protocol

from smogcast.ingest import gios_live, gios_registry

log = logging.getLogger(__name__)

REGISTRY_DIR = "gios_registry"
LIVE_DIR = "gios_live"


class FileStore(Protocol):
    def names(self, prefix: str) -> list[str]: ...
    def read_text(self, path: str) -> str: ...
    def write_text(self, path: str, text: str) -> None: ...


class FolderStore:
    """A local folder as a FileStore (tests; the same layout as the landing zone)."""

    def __init__(self, root: str | Path):
        self.root = Path(root)

    def names(self, prefix: str) -> list[str]:
        folder, _, start = prefix.rpartition("/")
        base = self.root / folder
        return sorted(f"{folder}/{p.name}" for p in base.glob(f"{start}*")) if base.exists() else []

    def read_text(self, path: str) -> str:
        return (self.root / path).read_text(encoding="utf-8")

    def write_text(self, path: str, text: str) -> None:
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")


def previous_fetch(store: FileStore) -> str | None:
    """``fetched_at`` of the newest batch already in the store (names sort by time)."""
    names = store.names(f"{LIVE_DIR}/batch_")
    return json.loads(store.read_text(names[-1]))["fetched_at"] if names else None


def poll_once(cfg: dict, store: FileStore, now: dt.datetime | None = None,
              fetch: Callable[[str], dict] = gios_registry.get_json) -> dict:
    """Registry snapshot, then one poll of every PM sensor in it; both written to ``store``. Returns a summary."""
    now = (now or dt.datetime.now(dt.timezone.utc)).replace(microsecond=0)
    base = cfg["gios"]["api"]["base_url"]
    cities = {cfg["cities"][c]["gios_city_name"] for c in cfg["run"]["cities"]}

    stations = gios_registry.fetch_snapshot(base, cities, fetch)
    snap_name, snap_text = gios_registry.snapshot_document(stations, now)
    store.write_text(f"{REGISTRY_DIR}/{snap_name}", snap_text)

    codes = {p["gios_code"] for p in cfg["pollutants"].values()}
    sensors = gios_live.pm_sensors(json.loads(snap_text), codes)
    skipped: list[int] = []
    records = gios_live.poll(base, sensors, now, fetch=fetch, skipped=skipped)
    # The previous fetch is read BEFORE writing this batch, so the batch never points at itself.
    batch_name, batch_text = gios_live.batch_document("live", records, now, previous_fetch(store))
    store.write_text(f"{LIVE_DIR}/{batch_name}", batch_text)

    summary = {"stations": len(stations), "sensors": len(sensors), "sensors_without_live_data": len(skipped),
               "records": len(records), "records_without_value": sum(r["value"] is None for r in records),
               "batch": batch_name}
    log.info("live poll: %s", summary)
    return summary
