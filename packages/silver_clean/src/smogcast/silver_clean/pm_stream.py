"""Cleaning of the PM stream (live GIOŚ API and the fault-injection replay) as TABLE DEFINITIONS.

Every cleaned table is a function of ``bronze.pm_stream`` (all records ever received, re-sends included),
recomputed as a whole on each run. The same functions define the tables of the declarative Lakeflow
pipeline on Databricks (pipelines/smogcast_stream) and of the local step ``silver-clean pm-stream`` —
one definition, two runners, no per-micro-batch state to keep consistent.

1. ``readings``: one row per key (source, station_code, pollutant, time_utc):
   - judged by its FIRST arrival (sent_at, previous fetch, lag) — re-sends never make a reading late,
   - carrying the NEWEST value — GIOŚ fills in empty hours later and re-publishes verified values
     (``status = 'correction'`` when the key arrived with more than one value),
   - ``is_late``: new at its first arrival although it was due at the PREVIOUS fetch already
     (hour end + max lag <= previous fetch) and more than ``max_lag_hours`` old; on a first fetch ever there
     is no previous one, so old hours are a backfill, not late,
   - with the station's city (``stream_station_city``); unknown stations keep ``city_id = NULL``.
2. ``accept_rules`` — the checks an on-time reading must pass, as SQL constraints: the pipeline declares
   them as expectations (``expect_all_or_drop``), the local step filters with the same strings. Rejected
   readings are never lost: late ones → ``ops.pm_late_rejected``, the rest → ``ops.pm_stream_quarantine``
   with the reason.
3. ``silver``: accepted readings with the pattern flags — frozen and spike (pm_clean, the same code as for
   the archive) and drift (vs the city median, ``flag_drift``) → ``silver.pm_stream``.

The quarantine is the CURRENT state: an empty value later filled in by GIOŚ leaves the quarantine and
enters silver. ``silver.pm_stream`` is separate from ``silver.pm_hourly`` (the model's history) — decision D17.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from smogcast.core.timeutil import day_cet_col, local_hour_end_to_utc_start_col
from smogcast.silver_clean.pm_clean import SERIES, flag_patterns

SOURCES = {"live": "gios_live", "fault_replay": "fault_replay"}  # source -> landing folder
# Polled from the GIOŚ API: their folder is in the LIVE landing zone (storage.live_landing_root — on Databricks the
# external volume the Azure Function writes to); the replay stays in the landing zone.
API_SOURCES = {"live"}
KEY = ["source", "station_code", "pollutant", "time_utc"]
LANDING_SCHEMA = (
    "source STRING, fetched_at STRING, previous_fetched_at STRING, records ARRAY<STRUCT<"
    "position_code: STRING, station_code: STRING, pollutant: STRING, sensor_id: LONG, "
    "time_local_end: STRING, value: DOUBLE, sent_at: STRING>>"
)


def flatten(batches: DataFrame) -> DataFrame:
    """Landing documents → one row per record with parsed times and lag (= the rows of bronze.pm_stream)."""
    r = batches.select(
        "source", "fetched_at", "previous_fetched_at", F.col("_metadata.file_path").alias("source_file"),
        F.explode("records").alias("r"),
    )
    hour_end_local = F.to_timestamp("r.time_local_end", "yyyy-MM-dd HH:mm:ss")
    return (
        r.select(
            "source", "source_file",
            F.col("r.position_code").alias("position_code"),
            F.col("r.station_code").alias("station_code"),
            F.col("r.pollutant").alias("pollutant"),
            F.col("r.sensor_id").alias("sensor_id"),
            F.col("r.value").alias("value"),
            local_hour_end_to_utc_start_col(hour_end_local).alias("time_utc"),
            F.to_timestamp("r.sent_at").alias("sent_at"),
            F.to_timestamp("previous_fetched_at").alias("previous_fetched_at"),
        )
        .withColumn("lag_hours", (F.col("sent_at").cast("long") - (F.col("time_utc").cast("long") + 3600)) / 3600.0)
    )


# Checks of the raw records, as expectations of bronze.pm_stream in the pipeline. A record of an unknown source
# means a broken poll file → the update stops; the others only count (bronze keeps everything as delivered).
BRONZE_FAIL_RULES = {"known_source": f"source IN ({', '.join(repr(s) for s in SOURCES)})"}
BRONZE_WARN_RULES = {"hour_parsed": "time_utc IS NOT NULL", "station_given": "station_code IS NOT NULL"}


def stream_station_city(station_city: DataFrame, registry_stations: DataFrame, cities: DataFrame) -> DataFrame:
    """Station → city for live data: archive metadata (historic stations) PLUS the current API
    registry (stations opened after the metadata file was published, e.g. Lublin ul. Okopowa)."""
    archive = station_city.where("code_kind = 'current'").select("station_code", "city_id", "jurisdiction_code")
    registry = (
        registry_stations.join(F.broadcast(cities), registry_stations.city_name == cities.gios_city_name)
        .select("station_code", "city_id", "jurisdiction_code")
    )
    return archive.unionByName(registry).dropDuplicates(["station_code"])


def readings(bronze: DataFrame, mapping: DataFrame, max_lag_hours: float) -> DataFrame:
    """One row per key: first arrival, newest value, ``status``, ``is_late`` and the station's city.
    ``mapping``: station_code, city_id, jurisdiction_code (see ``stream_station_city``)."""
    # Ties (the same poll stored twice, a duplicate batch) are broken by the file name — deterministic.
    first = Window.partitionBy(*KEY).orderBy(F.col("sent_at").asc(), F.col("source_file").asc())
    every = Window.partitionBy(*KEY)
    whole = first.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    as_text = F.coalesce(F.col("value").cast("string"), F.lit("null"))
    due_before_previous_fetch = (
        F.col("previous_fetched_at").isNotNull()
        & ((F.col("time_utc") + F.expr(f"INTERVAL {int(max_lag_hours * 60)} MINUTES") + F.expr("INTERVAL 1 HOUR"))
           <= F.col("previous_fetched_at"))
    )
    r = (
        bronze.withColumn("_rn", F.row_number().over(first))
        .withColumn("_newest", F.last("value").over(whole))        # the newest value, even if GIOŚ emptied it
        .withColumn("n_received", F.count("*").over(every))
        .withColumn("_n_values", F.size(F.collect_set(as_text).over(every)))
        .where("_rn = 1")
        .withColumn("value", F.col("_newest"))
        .withColumn("is_late", F.coalesce((F.col("lag_hours") > max_lag_hours) & due_before_previous_fetch,
                                          F.lit(False)))
        .withColumn("status", F.when(F.col("is_late"), "late")
                    .when(F.col("_n_values") > 1, "correction").otherwise("new"))
        .drop("_rn", "_newest", "_n_values")
    )
    return r.join(F.broadcast(mapping.select("station_code", "city_id", "jurisdiction_code")), "station_code", "left")


def accept_rules(dq: dict) -> dict[str, str]:
    """Expectation name -> SQL constraint an accepted reading satisfies (late readings are split off before).

    Each constraint tests one thing, so an empty value fails ``value_present`` alone (NULL passes the
    range checks).
    """
    hr = dq["hard_rules"]
    return {
        "known_station": "city_id IS NOT NULL",
        "value_present": "value IS NOT NULL",
        "value_not_below_min": f"value IS NULL OR value >= {hr['min_ug_m3']}",
        "value_not_above_max": f"value IS NULL OR value <= {hr['max_ug_m3']}",
    }


# Expectation name -> quarantine_reason (the names used in the ops tables, docs and the dashboard).
QUARANTINE_REASON = {
    "known_station": "unknown_station",
    "value_present": "missing_value",
    "value_not_below_min": "below_min",
    "value_not_above_max": "above_max",
}
ON_TIME_RULE = {"arrived_on_time": "NOT is_late"}


def _all(rules: dict[str, str]) -> str:
    return " AND ".join(f"({c})" for c in rules.values())


def late(r: DataFrame) -> DataFrame:
    """Readings rejected as late → ``ops.pm_late_rejected``."""
    return r.where("is_late")


def quarantine(r: DataFrame, dq: dict) -> DataFrame:
    """On-time readings failing a rule → ``ops.pm_stream_quarantine`` with ``failed_rules`` and the reason.
    An unknown station is reported alone: its value is never judged."""
    rules = accept_rules(dq)
    failed = F.array_compact(F.array(*[
        F.when(~F.coalesce(F.expr(c), F.lit(False)), F.lit(QUARANTINE_REASON[n])) for n, c in rules.items()]))
    return (
        r.where(f"NOT is_late AND NOT ({_all(rules)})")
        .withColumn("failed_rules", failed)
        .withColumn("quarantine_reason", F.when(F.col("city_id").isNull(), F.lit("unknown_station"))
                    .otherwise(F.array_join("failed_rules", ",")))
    )


def accepted(r: DataFrame, dq: dict) -> DataFrame:
    """On-time readings passing every rule (the pipeline gets the same rows from its expectations)."""
    return r.where(f"NOT is_late AND {_all(accept_rules(dq))}")


def flag_drift(df: DataFrame, dq: dict) -> DataFrame:
    """Drift = the station's recent residual vs the city median rises above its own earlier level."""
    d = dq["drift"]
    city_hour = Window.partitionBy("source", "city_id", "pollutant", "time_utc")
    df = (
        df.withColumn("_n_city", F.count("value").over(city_hour))
        .withColumn("city_median", F.percentile_approx("value", 0.5).over(city_hour))
        .withColumn("_resid", F.when(F.col("_n_city") >= d["min_city_stations"],
                                     F.log((F.col("value") + 1) / (F.col("city_median") + 1))))
    )
    t = F.col("time_utc").cast("long")
    series = Window.partitionBy("source", "station_code", "pollutant").orderBy(t)
    recent = series.rangeBetween(-(d["window_hours"] - 1) * 3600, 0)
    base = series.rangeBetween(-(d["window_hours"] + d["baseline_hours"] - 1) * 3600, -d["window_hours"] * 3600)
    return (
        df.withColumn("resid_recent", F.avg("_resid").over(recent))
        .withColumn("resid_baseline", F.avg("_resid").over(base))
        .withColumn("_n_base", F.count("_resid").over(base))
        .withColumn("is_drift", F.coalesce(
            (F.col("_n_base") >= d["min_baseline_hours"])
            & (F.col("resid_recent") - F.col("resid_baseline") > d["log_residual_increase"]), F.lit(False)))
        .drop("_n_city", "_resid", "_n_base")
    )


def recompute_flags(rows: DataFrame, dq: dict) -> DataFrame:
    """frozen / spike (shared with the archive cleaning) + drift, per source, on the accepted readings."""
    flagged = flag_drift(flag_patterns(rows, dq, ["source", *SERIES]), dq)
    return (
        flagged.withColumn("dq_flag", F.when(F.col("is_frozen"), "frozen").when(F.col("is_spike"), "spike")
                           .when(F.col("is_drift"), "drift").otherwise("ok"))
        .withColumn("is_valid", F.col("dq_flag") == "ok")
        .withColumn("day_cet", day_cet_col(F.col("time_utc")))
        .withColumn("hour_cet", F.hour(F.col("time_utc") + F.expr("INTERVAL 1 HOUR")))
    )


SILVER_COLUMNS = [
    "source", "station_code", "city_id", "jurisdiction_code", "pollutant", "time_utc", "day_cet", "hour_cet",
    "value", "dq_flag", "is_valid", "run_length", "is_frozen", "is_spike", "is_drift", "city_median",
    "resid_recent", "resid_baseline", "sent_at", "lag_hours", "status", "n_received",
]

# Pattern flags as warn-only expectations of the pipeline: the rows stay in silver (is_valid = false) and the
# pipeline UI shows how many readings each flag caught.
FLAG_RULES = {"not_frozen": "dq_flag <> 'frozen'", "not_spike": "dq_flag <> 'spike'", "not_drift": "dq_flag <> 'drift'"}


def silver(accepted_rows: DataFrame, dq: dict) -> DataFrame:
    """Accepted readings with the pattern flags → ``silver.pm_stream``."""
    return recompute_flags(accepted_rows, dq).select(*SILVER_COLUMNS)
