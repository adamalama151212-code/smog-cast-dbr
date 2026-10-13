"""One hourly poll as the Azure Function runs it: registry snapshot + live batch, both in the file store,
each batch pointing at the previous one (the stream needs it to judge late readings)."""

import datetime as dt
import json

from smogcast.core.config import load_config
from smogcast.ingest.gios_live import DATA_KEY
from smogcast.ingest.gios_registry import SENSORS_KEY, STATIONS_KEY
from smogcast.ingest.live_poll import FolderStore, poll_once, previous_fetch

CFG = load_config("local")


def fake_api(url: str) -> dict:
    if "/station/findAll" in url:
        return {STATIONS_KEY: [
            {"Identyfikator stacji": 400, "Kod stacji": "MpKrakAlKras", "Nazwa miasta": "Kraków"},
            {"Identyfikator stacji": 1, "Kod stacji": "SlKatoKossu", "Nazwa miasta": "Katowice"},   # not our city
        ], "totalPages": 1}
    if "/station/sensors/400" in url:
        return {SENSORS_KEY: [{"Identyfikator stanowiska": 2750, "Wskaźnik - kod": "PM10"},
                              {"Identyfikator stanowiska": 2745, "Wskaźnik - kod": "CO"}]}
    if "/data/getData/2750" in url:
        return {DATA_KEY: [{"Kod stanowiska": "MpKrakAlKras-PM10-1g", "Data": "2026-10-07 10:00:00", "Wartość": 31.2},
                           {"Kod stanowiska": "MpKrakAlKras-PM10-1g", "Data": "2026-10-07 11:00:00", "Wartość": None}]}
    raise AssertionError(f"unexpected call {url}")


def test_two_polls_write_snapshots_and_chained_batches(tmp_path):
    store = FolderStore(tmp_path)
    t1 = dt.datetime(2026, 10, 7, 9, 5, tzinfo=dt.timezone.utc)
    s1 = poll_once(CFG, store, t1, fetch=fake_api)
    poll_once(CFG, store, t1 + dt.timedelta(hours=1), fetch=fake_api)

    assert (s1["stations"], s1["sensors"], s1["records"], s1["records_without_value"]) == (1, 1, 2, 1)
    snaps = store.names("gios_registry/snapshot_")
    batches = store.names("gios_live/batch_")
    assert len(snaps) == 2 and len(batches) == 2
    snap = json.loads(store.read_text(snaps[0]))
    assert [s["Kod stacji"] for s in snap["stations"]] == ["MpKrakAlKras"]          # only the configured cities
    first, second = (json.loads(store.read_text(b)) for b in batches)
    assert first["previous_fetched_at"] is None                                      # first poll ever: a backfill
    assert second["previous_fetched_at"] == first["fetched_at"]
    assert previous_fetch(store) == second["fetched_at"]
    assert first["records"][0]["pollutant"] == "PM10" and first["source"] == "live"
    # The same layout as the landing zone, so the local steps and the pipeline read these files unchanged.
    assert (tmp_path / "gios_live").is_dir() and (tmp_path / "gios_registry").is_dir()
