"""Stream cleaning as table definitions: first arrival / newest value, late readings, quarantine reasons,
drift vs a real episode, and an end-to-end replay (late, duplicate batch, quarantine, spike, re-run, reset).

The same functions define the declarative pipeline's tables on Databricks (pipelines/smogcast_stream)."""

import datetime as dt
import math

from pyspark.sql import functions as F

from smogcast.core.config import load_config
from smogcast.ingest.fault_replay import build_batches
from smogcast.ingest.gios_live import write_batch
from smogcast.silver_clean import pm_stream as ps
from smogcast.silver_clean.steps import step_pm_stream

DQ = load_config("local")["pm_dq"]
T0 = dt.datetime(2025, 1, 17)
H = dt.timedelta(hours=1)
BRONZE = ("source STRING, source_file STRING, station_code STRING, pollutant STRING, time_utc TIMESTAMP, "
          "value DOUBLE, sent_at TIMESTAMP, previous_fetched_at TIMESTAMP, lag_hours DOUBLE")
MAPPING = "station_code STRING, city_id STRING, jurisdiction_code STRING"


def readings(spark, rows, known=("A", "B", "C")):
    bronze = spark.createDataFrame(
        [("live", f"poll_{r[4]:%H}.json", *r) for r in rows], BRONZE)   # one poll file per send time
    mapping = spark.createDataFrame([(s, "krakow", "PL-12") for s in known], MAPPING)
    return ps.readings(bronze, mapping, DQ["late_arrival"]["max_lag_hours"])


def by_key(df):
    return {(r["station_code"], r["time_utc"].hour): r for r in df.collect()}


def test_first_arrival_newest_value_late_and_backfill(spark):
    prev = T0 + 10 * H
    rows = [
        ("A", "PM10", T0 + H, 10.0, T0 + 2 * H, None, 0.0),          # on time ...
        ("A", "PM10", T0 + H, 10.0, T0 + 12 * H, prev, 10.0),       # ... re-sent later, same value: still on time
        ("A", "PM10", T0 + 2 * H, 20.0, T0 + 3 * H, None, 0.0),
        ("A", "PM10", T0 + 2 * H, 99.0, T0 + 12 * H, prev, 9.0),     # GIOŚ re-published another value
        ("B", "PM10", T0 + 3 * H, 5.0, T0 + 12 * H, prev, 8.0),      # due before the previous fetch -> late
        ("B", "PM10", T0 + 9 * H, 5.0, T0 + 12 * H, prev, 2.0),      # fresh -> new
        ("C", "PM10", T0 + 3 * H, 5.0, T0 + 12 * H, None, 8.0),      # first fetch ever -> backfill, new
    ]
    r = by_key(readings(spark, rows))
    assert {k: v["status"] for k, v in r.items()} == {
        ("A", 1): "new", ("A", 2): "correction", ("B", 3): "late", ("B", 9): "new", ("C", 3): "new"}
    assert r[("A", 1)]["n_received"] == 2 and r[("A", 1)]["lag_hours"] == 0.0      # judged by the first arrival
    assert r[("A", 2)]["value"] == 99.0                                             # carries the newest value


def test_two_polls_are_judged_by_the_first_arrival(spark):
    p1, p2 = T0 + 10 * H, T0 + 15 * H
    rows = [("A", "PM10", T0 + H, None, p1, None, 9.0),       # empty in poll 1 (first poll ever) ...
            ("A", "PM10", T0 + H, 30.0, p2, p1, 14.0),        # ... filled in by poll 2 — not late
            ("B", "PM10", T0 + H, 12.0, p1, None, 9.0),
            ("B", "PM10", T0 + H, 12.0, p2, p1, 14.0)]
    r = by_key(readings(spark, rows))
    assert {k[0]: v["status"] for k, v in r.items()} == {"A": "correction", "B": "new"}
    assert r[("A", 1)]["value"] == 30.0 and r[("A", 1)]["lag_hours"] == 9.0


def test_every_reading_ends_in_exactly_one_table_with_its_reason(spark):
    rows = [
        ("A", "PM10", T0 + H, 30.0, T0 + 2 * H, None, 0.0),        # ok
        ("A", "PM10", T0 + 2 * H, -3.0, T0 + 3 * H, None, 0.0),    # negative
        ("A", "PM10", T0 + 3 * H, 1500.0, T0 + 4 * H, None, 0.0),  # beyond the monitor range
        ("A", "PM10", T0 + 4 * H, None, T0 + 5 * H, None, 0.0),    # empty — reported once, not as out of range
        ("A", "PM10", T0 + 5 * H, None, T0 + 6 * H, None, 0.0),    # empty first ...
        ("A", "PM10", T0 + 5 * H, 40.0, T0 + 7 * H, T0 + 6 * H, 1.0),   # ... filled in later -> accepted
        ("X", "PM10", T0 + H, -1.0, T0 + 2 * H, None, 0.0),        # unknown station: its value is never judged
        ("B", "PM10", T0 + H, 30.0, T0 + 12 * H, T0 + 10 * H, 10.0),  # late
    ]
    r = readings(spark, rows)
    late = by_key(ps.late(r))
    quarantine = {k: v["quarantine_reason"] for k, v in by_key(ps.quarantine(r, DQ)).items()}
    accepted = by_key(ps.accepted(r, DQ))
    assert set(late) == {("B", 1)}
    assert quarantine == {("A", 2): "below_min", ("A", 3): "above_max", ("A", 4): "missing_value",
                          ("X", 1): "unknown_station"}
    assert set(accepted) == {("A", 1), ("A", 5)}
    assert len(late) + len(quarantine) + len(accepted) == r.count()


def drift_frame(spark, episode=False, drift=False):
    rows = []
    for i, st in enumerate("ABCD"):
        for h in range(130):
            v = 20.0 + i * 5                                   # D is always higher (traffic station)
            if episode and 70 <= h < 100:
                v *= 5                                         # all stations rise together: real smog
            if drift and st == "D" and h >= 60:
                v *= math.pow(1.05, h - 59)                    # only D creeps up: sensor drift
            rows.append(("live", st, "krakow", "PM10", T0 + h * H, v))
    df = spark.createDataFrame(rows, "source STRING, station_code STRING, city_id STRING, pollutant STRING, "
                                     "time_utc TIMESTAMP, value DOUBLE")
    return {(r["station_code"], int((r["time_utc"] - T0) / H)): r["is_drift"] for r in ps.flag_drift(df, DQ).collect()}


def test_drift_detected_only_on_the_drifting_station(spark):
    f = drift_frame(spark, drift=True)
    assert any(f[("D", h)] for h in range(80, 130))
    assert not any(f[(s, h)] for s in "ABC" for h in range(130))
    assert not any(f[("D", h)] for h in range(60))           # constantly higher is not drift


def test_real_smog_episode_is_not_drift(spark):
    f = drift_frame(spark, episode=True)
    assert not any(f.values())


def test_stream_end_to_end_with_fault_replay(ctx):
    sc = ctx.spark.createDataFrame(
        [(c, "krakow", "PL-12", c, "current") for c in "ABCD"],
        "station_code STRING, city_id STRING, jurisdiction_code STRING, current_station_code STRING, code_kind STRING")
    ctx.storage.overwrite(sc, "silver", "station_city")
    values = {(s, "PM10", T0 + h * H): 20.0 + i + (h % 3) for i, s in enumerate("ABCD") for h in range(24)}
    schedule = [
        {"type": "outage", "station_index": 0, "hour": 2, "hours": 8},     # A: hours 2..9 sent at hour 9
        {"type": "duplicate_batch", "hour": 12},
        {"type": "negative", "station_index": 1, "hour": 14},
        {"type": "missing_value", "station_index": 2, "hour": 15},
        {"type": "spike", "station_index": 3, "hour": 18, "factor": 8.0},
    ]
    out = ctx.storage.landing("fault_replay")
    for b in build_batches(values, list("ABCD"), T0, 24, schedule, 20):
        write_batch(out, "fault_replay", b.records, b.fetched_at, b.previous_fetched_at, name=b.name)

    step_pm_stream(ctx)
    st = ctx.storage
    m = {r["metric"]: r["value"] for r in st.read("ops", "dq_metrics").where("step = 'silver_pm_stream'").collect()}
    assert (m["fault_replay_accepted"], m["fault_replay_late_rejected"], m["fault_replay_quarantined"]) == (90, 4, 2)
    late = st.read("ops", "pm_late_rejected").collect()
    # A's backlog for hours 2..9 is sent after hour 9: hours 2..5 have lag > 3 h and were due earlier
    assert sorted(int((r["time_utc"] - T0) / H) for r in late) == [2, 3, 4, 5]
    reasons = sorted(r["quarantine_reason"] for r in st.read("ops", "pm_stream_quarantine").collect())
    assert reasons == ["below_min", "missing_value"]
    silver = st.read("silver", "pm_stream")
    assert silver.count() == 4 * 24 - 4 - 2                      # the re-sent batch added nothing
    assert silver.where("station_code = 'D' AND dq_flag = 'spike'").count() == 1

    tables = [("bronze", "pm_stream"), ("ops", "pm_late_rejected"), ("ops", "pm_stream_quarantine"),
              ("silver", "pm_stream")]
    before = {t: st.read(*t).count() for t in tables}
    step_pm_stream(ctx)                                          # nothing new: the checkpoint remembers the files
    assert {t: st.read(*t).count() for t in tables} == before
    assert st.read("silver", "pm_stream").where(F.col("source") != "fault_replay").count() == 0

    # Reset and clean the same files again: the same result, nothing skipped, nothing doubled.
    from smogcast.silver_clean.steps import _reset_stream_source

    _reset_stream_source(ctx, "fault_replay")
    step_pm_stream(ctx)
    assert {t: st.read(*t).count() for t in tables} == before


def test_live_reset_removes_only_live_state(ctx):
    from pathlib import Path

    from smogcast.silver_clean.steps import step_live_reset

    st = ctx.storage
    keys = "source STRING, station_code STRING, pollutant STRING, time_utc TIMESTAMP, value DOUBLE"
    rows = ctx.spark.createDataFrame([(s, "A", "PM10", T0, 10.0) for s in ("live", "fault_replay")], keys)
    tables = (("bronze", "pm_stream"), ("ops", "pm_stream_quarantine"), ("silver", "pm_stream"))
    for layer, name in tables:
        st.overwrite(rows, layer, name)
    Path(st.checkpoint("pm_stream_bronze_live")).mkdir(parents=True)
    Path(st.landing("gios_live")).mkdir(parents=True)

    step_live_reset(ctx)
    for layer, name in tables:
        assert [r["source"] for r in st.read(layer, name).collect()] == ["fault_replay"]
    assert not Path(st.checkpoint("pm_stream_bronze_live")).exists()
    assert Path(st.landing("gios_live")).exists()          # polls cannot be repeated — keep them


def test_fault_reset_leaves_pipeline_owned_stream_tables_to_the_full_refresh(ctx):
    """On Databricks (storage.stream_tables_owner = pipeline) a step may not edit the pipeline's tables: the reset
    removes the demo's files and registry rows only, the Job's full refresh of the pipeline drops the demo rows."""
    from pathlib import Path

    from smogcast.silver_clean.steps import step_fault_reset

    st = ctx.storage
    keys = "source STRING, station_code STRING, pollutant STRING, time_utc TIMESTAMP, value DOUBLE"
    st.overwrite(ctx.spark.createDataFrame([("fault_replay", "A", "PM10", T0, 10.0)], keys), "silver", "pm_stream")
    registry = "station_id INT, registry_source STRING"
    st.overwrite(ctx.spark.createDataFrame([(1, "gios_api"), (2, "fault_replay")], registry), "silver", "stations")
    for folder in ("fault_replay", "fault_replay_registry"):
        Path(st.landing(folder)).mkdir(parents=True)

    ctx.cfg["storage"]["stream_tables_owner"] = "pipeline"
    step_fault_reset(ctx)
    assert st.read("silver", "pm_stream").count() == 1                       # untouched: the pipeline's table
    assert [r["registry_source"] for r in st.read("silver", "stations").collect()] == ["gios_api"]
    assert not Path(st.landing("fault_replay")).exists() and not Path(st.landing("fault_replay_registry")).exists()

    del ctx.cfg["storage"]["stream_tables_owner"]                            # locally the steps own them
    step_fault_reset(ctx)
    assert st.read("silver", "pm_stream").count() == 0
