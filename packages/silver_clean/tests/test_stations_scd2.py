"""CDC from registry snapshots and SCD2 history: insert, update, delete, re-appearance."""

import datetime as dt

from smogcast.silver_clean.stations_scd2 import apply_scd2, changes_from_snapshots

SCHEMA = ("snapshot_ts TIMESTAMP, registry_source STRING, station_id LONG, station_code STRING, station_name STRING, "
          "city_name STRING, voivodeship STRING, lat DOUBLE, lon DOUBLE, sensor_id LONG, pollutant STRING")
T = [dt.datetime(2026, 10, d) for d in (1, 2, 3, 4)]


def snap(ts, station_id, sensors):
    return [(ts, "gios_api", station_id, f"S{station_id}", f"Station {station_id}", "Kraków", "MAŁOPOLSKIE",
             50.0, 19.9, sid, pol) for sid, pol in sensors]


def test_insert_update_delete_and_reappearance(spark):
    rows = (
        snap(T[0], 1, [(10, "PM10")]) + snap(T[0], 2, [(20, "PM10")])
        + snap(T[1], 1, [(10, "PM10"), (11, "PM2.5")]) + snap(T[1], 2, [(20, "PM10")])   # 1: new sensor -> UPDATE
        + snap(T[2], 1, [(10, "PM10"), (11, "PM2.5")])                                  # 2 missing -> DELETE
        + snap(T[3], 1, [(10, "PM10"), (11, "PM2.5")]) + snap(T[3], 2, [(20, "PM10")])   # 2 back -> INSERT
    )
    changes = changes_from_snapshots(spark.createDataFrame(rows, SCHEMA))
    ops = sorted((r["station_id"], r["change_ts"].day, r["op"]) for r in changes.collect())
    assert ops == [(1, 1, "INSERT"), (1, 2, "UPDATE"), (2, 1, "INSERT"), (2, 3, "DELETE"), (2, 4, "INSERT")]

    scd = {(r["station_id"], r["valid_from"].day): r for r in apply_scd2(changes).collect()}
    assert scd[(1, 1)]["valid_to"].day == 2 and not scd[(1, 1)]["is_current"]
    assert scd[(1, 2)]["is_current"] and scd[(1, 2)]["sensors"] == ["PM10:10", "PM2.5:11"]
    assert scd[(2, 1)]["valid_to"].day == 3       # closed by the DELETE, no version for the gap
    assert scd[(2, 4)]["is_current"]
    assert len(scd) == 4


def test_unchanged_snapshots_produce_no_changes(spark):
    rows = snap(T[0], 1, [(10, "PM10")]) + snap(T[1], 1, [(10, "PM10")])
    changes = changes_from_snapshots(spark.createDataFrame(rows, SCHEMA)).collect()
    assert [(r["op"], r["change_ts"].day) for r in changes] == [("INSERT", 1)]
