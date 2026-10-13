"""Live poll: PM sensors from the registry snapshot, batch contract with previous fetch time."""

import datetime as dt
import json

from smogcast.ingest.gios_live import DATA_KEY, pm_sensors, poll, previous_fetch, write_batch

SNAP = {"stations": [{"Kod stacji": "MpKrakAlKras", "sensors": [
    {"Identyfikator stanowiska": 2745, "Wskaźnik - kod": "CO"},
    {"Identyfikator stanowiska": 2750, "Wskaźnik - kod": "PM10"},
    {"Identyfikator stanowiska": 2752, "Wskaźnik - kod": "PM2.5"}]}]}


def test_only_pm_sensors_are_polled():
    assert [s["sensor_id"] for s in pm_sensors(SNAP, {"PM10", "PM2.5"})] == [2750, 2752]


def test_poll_and_batch_contract(tmp_path):
    now = dt.datetime(2026, 10, 1, 9, 0, tzinfo=dt.timezone.utc)

    def fake(url):
        return {DATA_KEY: [{"Kod stanowiska": "MpKrakAlKras-PM10-1g", "Data": "2026-10-01 10:00:00", "Wartość": 31.2},
                           {"Kod stanowiska": "MpKrakAlKras-PM10-1g", "Data": "2026-10-01 11:00:00", "Wartość": None}]}

    records = poll("https://x", pm_sensors(SNAP, {"PM10"}), now, fetch=fake)
    assert records[0]["time_local_end"] == "2026-10-01 10:00:00" and records[1]["value"] is None
    assert previous_fetch(tmp_path) is None
    write_batch(tmp_path, "live", records, now, None)
    assert previous_fetch(tmp_path) == "2026-10-01T09:00:00+00:00"
    doc = json.loads(next(tmp_path.glob("batch_*.json")).read_text())
    assert doc["source"] == "live" and len(doc["records"]) == 2 and not list(tmp_path.glob("*.part"))


def test_manual_sensor_without_live_data_is_skipped():
    import requests

    class Resp:
        status_code = 400

        def json(self):
            return {"error_reason": "Dla stanowiska typu manualnego wyniki pomiarów nie są dostępne na bieżąco"}

    def fake(url):
        if url.split("/")[-1].startswith("2752"):
            raise requests.HTTPError("400", response=Resp())
        return {DATA_KEY: [{"Kod stanowiska": "X", "Data": "2026-10-01 10:00:00", "Wartość": 1.0}]}

    skipped = []
    records = poll("https://x", pm_sensors(SNAP, {"PM10", "PM2.5"}),
                   dt.datetime(2026, 10, 1, tzinfo=dt.timezone.utc), fetch=fake, skipped=skipped)
    assert skipped == [2752] and len(records) == 1
