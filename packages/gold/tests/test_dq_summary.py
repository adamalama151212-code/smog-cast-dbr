"""gold.dq_summary: distinct readings per source, shares of the unique readings, missing tables."""

import datetime as dt

import pytest

from smogcast.gold.dq_summary import build_dq_summary
from smogcast.gold.steps import step_dq_summary

T = dt.datetime(2026, 1, 20, 10)
H = dt.timedelta(hours=1)
KEYS = "source STRING, station_code STRING, pollutant STRING, time_utc TIMESTAMP"


def test_summary_counts_distinct_readings(spark):
    # 4 unique readings, one of them re-sent twice (API overlap) -> 6 records
    bronze = spark.createDataFrame([("live", s, "PM10", T + h * H) for s, h in
                                    [("A", 0), ("A", 0), ("A", 0), ("A", 1), ("B", 0), ("B", 1)]], KEYS)
    late = spark.createDataFrame([("live", "A", "PM10", T + H)], KEYS)
    quarantine = spark.createDataFrame([("live", "B", "PM10", T, "missing_value")], f"{KEYS}, quarantine_reason STRING")
    silver = spark.createDataFrame([("live", "A", "PM10", T, "ok", True), ("live", "B", "PM10", T + H, "spike", False)],
                                   f"{KEYS}, dq_flag STRING, is_valid BOOLEAN")
    m = {r["metric"]: r for r in build_dq_summary(bronze, late, quarantine, silver).collect()}
    assert m["records_received"]["value"] == 6 and m["records_received"]["pct_of_unique"] is None
    assert m["unique_readings"]["value"] == 4 and m["resent_duplicates"]["value"] == 2
    assert m["late_rejected"]["pct_of_unique"] == pytest.approx(25.0)
    assert m["quarantined_missing_value"]["value"] == 1
    assert (m["silver_readings"]["value"], m["silver_valid"]["value"], m["flag_spike"]["value"]) == (2, 1, 1)


def test_step_works_when_tables_are_missing(ctx):
    ctx.storage.overwrite(ctx.spark.createDataFrame([("live", "A", "PM10", T)], KEYS), "bronze", "pm_stream")
    step_dq_summary(ctx)
    m = {r["metric"]: r["value"] for r in ctx.storage.read("gold", "dq_summary").collect()}
    assert m == {"records_received": 1, "unique_readings": 1, "resent_duplicates": 0}
