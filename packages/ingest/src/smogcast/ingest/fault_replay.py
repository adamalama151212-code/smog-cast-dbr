"""Fault-injection replay: real hourly measurements → corrupted stream batches (decision D17).

Successor of the radplume sensor simulator: the VALUES are real (a copy from bronze.pm_hourly),
only the transport and single readings are corrupted, on the deterministic schedule from
conf/fault_injection.yaml. Output uses exactly the live batch format (see gios_live.py), so the
same streaming cleaning handles both. Written to landing/fault_replay/ — never mixed with the
model's history.

``build_batches`` is a pure function (no Spark, no I/O) so every fault is unit-testable.
"""

from __future__ import annotations

import copy
import datetime as dt
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo

UTC = dt.timezone.utc
LOCAL = ZoneInfo("Europe/Warsaw")
HOUR = dt.timedelta(hours=1)


@dataclass
class Batch:
    name: str
    fetched_at: dt.datetime
    previous_fetched_at: str | None
    records: list[dict] = field(default_factory=list)


def local_end_string(time_utc_start: dt.datetime) -> str:
    """Start of hour in UTC → END of hour in local time, as the GIOŚ API writes it."""
    end = (time_utc_start + HOUR).replace(tzinfo=UTC).astimezone(LOCAL)
    return end.strftime("%Y-%m-%d %H:%M:%S")


def _faults(schedule: list[dict], kind: str) -> list[dict]:
    return [f for f in schedule if f["type"] == kind]


def corrupt_values(values: dict[tuple, float | None], stations: list[str], start: dt.datetime,
                   schedule: list[dict]) -> dict[tuple, float | None]:
    """Apply value faults. ``values`` is keyed by (station_code, pollutant, time_utc_start)."""
    out = dict(values)

    def keys(station_index: int, hour: int, hours: int = 1):
        code = stations[station_index]
        return [k for k in out if k[0] == code and 0 <= int((k[2] - start) / HOUR) - hour < hours]

    for f in _faults(schedule, "negative"):
        for k in keys(f["station_index"], f["hour"]):
            out[k] = -abs(out[k] or 5.0)
    for f in _faults(schedule, "out_of_range"):
        for k in keys(f["station_index"], f["hour"]):
            out[k] = float(f["value"])
    for f in _faults(schedule, "missing_value"):
        for k in keys(f["station_index"], f["hour"]):
            out[k] = None
    for f in _faults(schedule, "frozen"):
        ks = sorted(keys(f["station_index"], f["hour"], f["hours"]), key=lambda k: k[2])
        for k in ks[1:]:
            out[k] = out[ks[0]]
    for f in _faults(schedule, "spike"):
        for k in keys(f["station_index"], f["hour"]):
            out[k] = (out[k] or 0.0) * f["factor"]
    for f in _faults(schedule, "drift"):
        for k in keys(f["station_index"], f["hour"], f["hours"]):
            step = int((k[2] - start) / HOUR) - f["hour"] + 1
            if out[k] is not None:
                out[k] = round(out[k] * (1 + f["rate_per_hour"]) ** step, 2)
    return out


def build_batches(values: dict[tuple, float | None], stations: list[str], start: dt.datetime, hours: int,
                  schedule: list[dict], send_delay_minutes: int) -> list[Batch]:
    """One batch per hour; outages hold a station's records back and release them together."""
    values = corrupt_values(values, stations, start, schedule)
    outages = [(stations[f["station_index"]], f["hour"], f["hour"] + f["hours"]) for f in _faults(schedule, "outage")]
    duplicates = {f["hour"] for f in _faults(schedule, "duplicate_batch")}
    held: dict[str, list[tuple]] = {}
    batches: list[Batch] = []
    previous: str | None = None
    for h in range(hours):
        hour_start = start + h * HOUR
        fetched = hour_start + HOUR + dt.timedelta(minutes=send_delay_minutes)
        sent = fetched.replace(tzinfo=UTC).isoformat(timespec="seconds")
        keys = [k for k in values if k[2] == hour_start]
        to_send = []
        for k in sorted(keys):
            if any(code == k[0] and a <= h < b for code, a, b in outages):
                held.setdefault(k[0], []).append(k)
            else:
                to_send.append(k)
        for code, _a, b in outages:
            if h == b - 1:
                to_send += held.pop(code, [])   # connection back: the backlog goes out with this batch
        records = [{"position_code": f"{k[0]}-{k[1]}-1g", "station_code": k[0], "pollutant": k[1], "sensor_id": None,
                    "time_local_end": local_end_string(k[2]), "value": values[k], "sent_at": sent} for k in to_send]
        batch = Batch(f"batch_{fetched:%Y%m%dT%H%M%SZ}.json", fetched.replace(tzinfo=UTC), previous, records)
        batches.append(batch)
        if h in duplicates:
            batches.append(Batch(batch.name.replace(".json", "_resend.json"), batch.fetched_at, previous,
                                 copy.deepcopy(records)))
        previous = sent
    return batches


def registry_variants(snapshot: dict, city_name: str, start: dt.datetime, schedule: list[dict]) -> list[tuple]:
    """(timestamp, stations) snapshots for the CDC demo: the real registry at the start of the window
    and a modified copy at each ``registry_change`` (a station removed, a sensor added)."""
    city = sorted((s for s in snapshot["stations"] if s.get("Nazwa miasta") == city_name), key=lambda s: s["Kod stacji"])
    out = [(start.replace(tzinfo=UTC), copy.deepcopy(city))]
    for f in _faults(schedule, "registry_change"):
        changed = copy.deepcopy(city)
        removed = changed[f["remove_station_index"]]["Kod stacji"]
        target = changed[f["add_sensor_station_index"]]
        target.setdefault("sensors", []).append({"Identyfikator stanowiska": 990000 + f["hour"],
                                                 "Wskaźnik - kod": "PM2.5", "Wskaźnik": "pył zawieszony PM2.5 (injected)"})
        changed = [s for s in changed if s["Kod stacji"] != removed]
        out.append(((start + f["hour"] * HOUR).replace(tzinfo=UTC), changed))
    return out
