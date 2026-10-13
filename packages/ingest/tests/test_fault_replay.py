"""Fault-injection replay: each scheduled fault produces exactly the corruption it promises."""

import datetime as dt

import pytest

from smogcast.ingest.fault_replay import build_batches, corrupt_values, local_end_string, registry_variants

START = dt.datetime(2025, 1, 17)
STATIONS = ["A", "B", "C"]


def values(hours=12):
    return {(s, "PM10", START + dt.timedelta(hours=h)): 10.0 + i + h for i, s in enumerate(STATIONS) for h in range(hours)}


def at(vals, station, hour):
    return vals[(station, "PM10", START + dt.timedelta(hours=hour))]


def test_local_end_string_is_end_of_hour_in_local_time():
    assert local_end_string(dt.datetime(2025, 1, 17, 0)) == "2025-01-17 02:00:00"    # CET = UTC+1
    assert local_end_string(dt.datetime(2025, 7, 17, 0)) == "2025-07-17 03:00:00"    # CEST = UTC+2


def test_value_faults():
    sched = [
        {"type": "negative", "station_index": 0, "hour": 1},
        {"type": "out_of_range", "station_index": 1, "hour": 1, "value": 1500},
        {"type": "missing_value", "station_index": 2, "hour": 1},
        {"type": "frozen", "station_index": 0, "hour": 3, "hours": 4},
        {"type": "spike", "station_index": 1, "hour": 5, "factor": 6.0},
        {"type": "drift", "station_index": 2, "hour": 8, "hours": 2, "rate_per_hour": 0.5},
    ]
    v, out = values(), corrupt_values(values(), STATIONS, START, sched)
    assert at(out, "A", 1) < 0 and at(out, "B", 1) == 1500.0 and at(out, "C", 1) is None
    assert [at(out, "A", h) for h in range(3, 7)] == [at(v, "A", 3)] * 4 and at(out, "A", 7) == at(v, "A", 7)
    assert at(out, "B", 5) == pytest.approx(6 * at(v, "B", 5))
    assert at(out, "C", 8) == pytest.approx(at(v, "C", 8) * 1.5) and at(out, "C", 9) == pytest.approx(at(v, "C", 9) * 2.25)
    assert at(out, "B", 0) == at(v, "B", 0)  # untouched elsewhere


def test_outage_holds_records_and_releases_them_late():
    batches = build_batches(values(), STATIONS, START, 12, [{"type": "outage", "station_index": 0, "hour": 2, "hours": 5}], 20)
    per_batch = [{r["station_code"] for r in b.records} for b in batches]
    assert all("A" not in s for s in per_batch[2:6])        # silent during the outage
    released = [r for r in batches[6].records if r["station_code"] == "A"]
    assert len(released) == 5                               # backlog of hours 2..6 sent together
    assert len({r["sent_at"] for r in released}) == 1
    assert batches[3].previous_fetched_at == batches[2].fetched_at.isoformat(timespec="seconds")


def test_duplicate_batch_is_sent_twice_with_identical_records():
    batches = build_batches(values(), STATIONS, START, 4, [{"type": "duplicate_batch", "hour": 1}], 20)
    assert len(batches) == 5 and batches[2].name.endswith("_resend.json")
    assert batches[1].records == batches[2].records


def test_registry_change_removes_a_station_and_adds_a_sensor():
    snap = {"stations": [{"Kod stacji": c, "Nazwa miasta": "Kraków", "sensors": []} for c in ("S1", "S2", "S3")]
            + [{"Kod stacji": "X", "Nazwa miasta": "Gdańsk", "sensors": []}]}
    variants = registry_variants(snap, "Kraków", START,
                                 [{"type": "registry_change", "hour": 5, "remove_station_index": 2, "add_sensor_station_index": 0}])
    assert [len(v[1]) for v in variants] == [3, 2]
    after = {s["Kod stacji"]: s for s in variants[1][1]}
    assert "S3" not in after and len(after["S1"]["sensors"]) == 1
    assert variants[1][0] == (START + dt.timedelta(hours=5)).replace(tzinfo=dt.timezone.utc)
