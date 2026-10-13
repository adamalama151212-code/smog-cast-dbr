"""Registry snapshot: pagination, city filter, sensors per station, file written atomically."""

import datetime as dt
import json

from smogcast.ingest.gios_registry import SENSORS_KEY, STATIONS_KEY, fetch_snapshot, write_snapshot

BASE = "https://api.example/v1/rest"


def fake_api(url: str) -> dict:
    if "findAll?page=0" in url:
        return {STATIONS_KEY: [{"Identyfikator stacji": 400, "Nazwa miasta": "Kraków"},
                               {"Identyfikator stacji": 11, "Nazwa miasta": "Czerniawa"}], "totalPages": 2}
    if "findAll?page=1" in url:
        return {STATIONS_KEY: [{"Identyfikator stacji": 500, "Nazwa miasta": "Gdańsk"}], "totalPages": 2}
    if url.endswith("/station/sensors/400"):
        return {SENSORS_KEY: [{"Identyfikator stanowiska": 2750, "Wskaźnik - kod": "PM10"}]}
    if url.endswith("/station/sensors/500"):
        return {SENSORS_KEY: []}
    raise AssertionError(f"unexpected URL {url}")


def test_snapshot_reads_all_pages_and_only_configured_cities(tmp_path):
    stations = fetch_snapshot(BASE, {"Kraków", "Gdańsk"}, fetch=fake_api)
    assert sorted(s["Identyfikator stacji"] for s in stations) == [400, 500]
    assert stations[0]["sensors"][0]["Wskaźnik - kod"] == "PM10"

    path = write_snapshot(tmp_path, stations, now=dt.datetime(2026, 10, 1, 8, 0, tzinfo=dt.timezone.utc))
    assert path.name == "snapshot_20261001T080000Z.json"
    doc = json.loads(path.read_text(encoding="utf-8"))
    assert doc["source"] == "gios_api" and len(doc["stations"]) == 2
