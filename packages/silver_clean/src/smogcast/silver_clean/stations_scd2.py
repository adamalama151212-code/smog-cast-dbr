"""Station registry history: snapshots → change feed (CDC) → SCD type 2.

Step 1 — change feed (``silver.station_changes``). The GIOŚ API has no CDC, so changes are
derived by comparing consecutive snapshots of the same registry source:
- INSERT: the station appears (first time, or again after it disappeared),
- UPDATE: it is present in both snapshots but an attribute changed (name, position, sensors),
- DELETE: it was present and is missing from the next snapshot.

Step 2 — SCD2 (``silver.stations``). On Databricks this is a single Lakeflow declaration:

    dlt.create_auto_cdc_flow(target="silver_stations", source="silver_station_changes",
        keys=["registry_source", "station_id"], sequence_by="change_ts",
        apply_as_deletes=F.expr("op = 'DELETE'"), stored_as_scd_type=2)

Locally the SAME semantics are implemented with window functions: every version has
``valid_from`` / ``valid_to``; a DELETE closes the last version and creates none.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

KEY = ["registry_source", "station_id"]
ATTRS = ["station_code", "station_name", "city_name", "voivodeship", "lat", "lon", "sensors"]


def station_states(snapshots: DataFrame) -> DataFrame:
    """One row per (source, station, snapshot) with its attributes and an attribute hash."""
    sensor = F.when(F.col("sensor_id").isNotNull(), F.concat_ws(":", "pollutant", F.col("sensor_id").cast("string")))
    states = snapshots.groupBy(*KEY, "snapshot_ts").agg(
        *[F.first(c).alias(c) for c in ATTRS if c != "sensors"],
        F.array_sort(F.collect_set(sensor)).alias("sensors"),
    )
    fingerprint = F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in ATTRS if c != "sensors"],
                              F.array_join("sensors", ","))
    return states.withColumn("attr_hash", F.sha2(fingerprint, 256))


def changes_from_snapshots(snapshots: DataFrame) -> DataFrame:
    states = station_states(snapshots)
    snaps = snapshots.select("registry_source", "snapshot_ts").distinct()
    stations = states.groupBy(*KEY).agg(F.min("snapshot_ts").alias("first_seen"))
    # Every station × every snapshot of its source since it was first seen: absence becomes visible.
    grid = stations.join(snaps, "registry_source").where(F.col("snapshot_ts") >= F.col("first_seen"))
    g = grid.join(states, [*KEY, "snapshot_ts"], "left").withColumn("present", F.col("attr_hash").isNotNull())
    w = Window.partitionBy(*KEY).orderBy("snapshot_ts")
    g = g.withColumn("prev_present", F.lag("present").over(w)).withColumn("prev_hash", F.lag("attr_hash").over(w))
    op = (
        F.when(F.col("present") & ~F.coalesce(F.col("prev_present"), F.lit(False)), "INSERT")
        .when(F.col("present") & (F.col("attr_hash") != F.col("prev_hash")), "UPDATE")
        .when(~F.col("present") & F.coalesce(F.col("prev_present"), F.lit(False)), "DELETE")
    )
    return (
        g.withColumn("op", op)
        .where(F.col("op").isNotNull())
        .select(*KEY, "op", F.col("snapshot_ts").alias("change_ts"), *ATTRS, "attr_hash")
    )


def apply_scd2(changes: DataFrame) -> DataFrame:
    w = Window.partitionBy(*KEY).orderBy("change_ts")
    versions = changes.dropDuplicates([*KEY, "change_ts"]).withColumn("valid_to", F.lead("change_ts").over(w))
    return (
        versions.where(F.col("op") != "DELETE")
        .withColumnRenamed("change_ts", "valid_from")
        .withColumn("is_current", F.col("valid_to").isNull())
        .select(*KEY, *ATTRS, "valid_from", "valid_to", "is_current")
    )
