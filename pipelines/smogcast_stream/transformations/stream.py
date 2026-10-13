"""Declarative Lakeflow pipeline `smogcast-stream`: the live PM stream on Databricks.

    landing_live/gios_live, landing/fault_replay ──Auto Loader──► bronze.pm_stream            (streaming table)
        ──► stream_readings (one row per reading: first arrival, newest value, city)       (temporary view)
              ├──► ops.pm_late_rejected      late readings                                 (materialized view)
              ├──► ops.pm_stream_quarantine  readings failing a rule, with the reason      (materialized view)
              └──► stream_accepted ── expectations DROP late / unknown station / empty / out of range
                     └──► silver.pm_stream   + warn-only expectations: frozen, spike, drift (materialized view)

Only declarations live here; every table is defined by a function of smogcast.silver_clean.pm_stream (installed
from the bundle's wheels), the same functions the local step `smogcast-silver-clean --step pm-stream` runs and the
unit / integration tests check. Expectations use the same rule strings as the local filter (accept_rules), so the
pipeline UI counts exactly what the quarantine and the late table hold.

Configuration (pipeline settings): smogcast.env = dev | prod → conf/<env>.yaml (catalog, landing volume).
"""

import os

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from smogcast.core.config import active_cities, load_config
from smogcast.silver_clean import pm_stream as ps
from smogcast.silver_clean.station_city import cities_frame

spark = SparkSession.getActiveSession()
CFG = load_config(spark.conf.get("smogcast.env"))
DQ = CFG["pm_dq"]
CATALOG = CFG["storage"]["catalog"]
LANDING = CFG["storage"]["landing_root"]
# GIOŚ API polls land in their own zone (the Azure Function's container, external volume raw.landing_live).
LIVE_LANDING = CFG["storage"].get("live_landing_root", LANDING)


def table(layer: str, name: str) -> str:
    return f"{CATALOG}.{layer}.{name}"


# ----------------------------------------------------------------------------- bronze: every record as delivered
dp.create_streaming_table(
    name=table("bronze", "pm_stream"),
    comment="Raw stream records (GIOŚ API polls and the fault-injection replay), re-sends included",
    expect_all_or_fail=ps.BRONZE_FAIL_RULES,
    expect_all=ps.BRONZE_WARN_RULES,
)


def _ingest(source: str, folder: str) -> None:
    path = f"{LIVE_LANDING if source in ps.API_SOURCES else LANDING}/{folder}"
    # The fault demo's folder exists only after its first replay (and fault-reset removes it).
    os.makedirs(path, exist_ok=True)

    @dp.append_flow(target=table("bronze", "pm_stream"), name=f"ingest_{source}")
    def flow():
        return ps.flatten(
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", "true")
            .option("pathGlobFilter", "*.json")     # skip *.json.part files that are still being written
            .schema(ps.LANDING_SCHEMA)
            .load(path)
        )


for _source, _folder in ps.SOURCES.items():
    _ingest(_source, _folder)


# ----------------------------------------------------------------------------- readings and their verdicts
def _station_city():
    # Written by the live Job (registry CDC) before it refreshes this pipeline.
    registry = spark.read.table(table("silver", "stations")).where("registry_source = 'gios_api' AND is_current")
    return ps.stream_station_city(spark.read.table(table("silver", "station_city")), registry,
                                  cities_frame(spark, active_cities(CFG)))


@dp.temporary_view(name="stream_readings", comment="One row per reading: first arrival, newest value, city")
def stream_readings():
    return ps.readings(spark.read.table(table("bronze", "pm_stream")), _station_city(),
                       DQ["late_arrival"]["max_lag_hours"])


@dp.materialized_view(name=table("ops", "pm_late_rejected"),
                      comment="Readings new at their first arrival although due at the previous poll (> max lag)")
def pm_late_rejected():
    return ps.late(spark.read.table("stream_readings"))


@dp.materialized_view(name=table("ops", "pm_stream_quarantine"),
                      comment="On-time readings failing a rule, with failed_rules and quarantine_reason")
def pm_stream_quarantine():
    return ps.quarantine(spark.read.table("stream_readings"), DQ)


@dp.temporary_view(name="stream_accepted", comment="Readings that pass every rule")
@dp.expect_all_or_drop({**ps.ON_TIME_RULE, **ps.accept_rules(DQ)})
def stream_accepted():
    return spark.read.table("stream_readings")


# ----------------------------------------------------------------------------- silver
@dp.materialized_view(name=table("silver", "pm_stream"),
                      comment="Cleaned stream with dq_flag (ok / frozen / spike / drift); separate from the model history")
@dp.expect_all(ps.FLAG_RULES)
def pm_stream():
    return ps.silver(spark.read.table("stream_accepted"), DQ)
