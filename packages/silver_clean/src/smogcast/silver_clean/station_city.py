"""Which measurement station belongs to which forecast city.

Matched by the town name in the GIOŚ metadata (``Miejscowość``) against ``gios_city_name`` in
conf/cities.yaml — no hard-coded station lists, so new stations are picked up automatically.
Stations renamed over the years appear in the archive under their OLD code; the metadata
lists old codes ("Stary Kod stacji", possibly several per station), so every old code gets
its own mapping row pointing to the same city.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def cities_frame(spark: SparkSession, cities: dict[str, dict]) -> DataFrame:
    rows = [(cid, c["gios_city_name"], c["jurisdiction_code"]) for cid, c in cities.items()]
    return spark.createDataFrame(rows, "city_id STRING, gios_city_name STRING, jurisdiction_code STRING")


def build_station_city(stations: DataFrame, cities: DataFrame) -> DataFrame:
    """Rows: (station_code, city_id, jurisdiction_code, current_station_code, lat, lon, code_kind)."""
    matched = stations.join(F.broadcast(cities), stations.town == cities.gios_city_name).select(
        "station_code", "city_id", "jurisdiction_code", "lat", "lon", "old_station_codes"
    )
    current = matched.select(
        "station_code", "city_id", "jurisdiction_code",
        F.col("station_code").alias("current_station_code"), "lat", "lon", F.lit("current").alias("code_kind"),
    )
    old = (
        matched.where(F.col("old_station_codes").isNotNull())
        .withColumn("old_code", F.explode(F.split(F.regexp_replace("old_station_codes", r"\s", ""), "[,;]")))
        .where(F.col("old_code") != "")
        .select(
            F.col("old_code").alias("station_code"), "city_id", "jurisdiction_code",
            F.col("station_code").alias("current_station_code"), "lat", "lon", F.lit("old").alias("code_kind"),
        )
    )
    # An old code identical to a current one would duplicate the key — keep the current row.
    return current.unionByName(old.join(current.select("station_code"), "station_code", "left_anti"))
