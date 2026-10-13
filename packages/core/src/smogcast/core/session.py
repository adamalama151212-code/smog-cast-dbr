"""Spark session: a local one with Delta Lake, or the existing Databricks cluster session.

This is the ONLY place in the code that knows whether we run locally or in the cloud.
Everything else receives a ready ``SparkSession`` and does not care.
"""

from __future__ import annotations

import logging
import os
import time

from pyspark.sql import SparkSession

log = logging.getLogger(__name__)


def is_databricks() -> bool:
    """Databricks sets this variable on every cluster and on serverless compute."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _force_process_utc() -> None:
    """Make the Python process time zone UTC.

    PySpark converts timestamps in ``collect()`` and ``createDataFrame`` using the
    time zone of the PYTHON PROCESS (not the Spark session). On a laptop set to
    Polish time a datetime would differ by 1–2 h from the one in Docker/on the cluster.
    """
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):  # not available on Windows — Docker is used there anyway
        time.tzset()


def get_spark(cfg: dict) -> SparkSession:
    _force_process_utc()
    if is_databricks():
        # The cluster already has a session (with Delta, Unity Catalog, Photon).
        # Building our own would ignore the cluster configuration.
        spark = SparkSession.builder.getOrCreate()
    else:
        spark = _build_local_session(cfg["spark"])

    # UTC everywhere. Without it Spark interprets timestamps in the OS time zone and
    # the same code would give different hours on a laptop, in Docker and on Azure.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def _build_local_session(spark_cfg: dict) -> SparkSession:
    # Imported here, not at module top: on Databricks the pip package `delta-spark`
    # is not needed because Delta is built into the runtime.
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.master(spark_cfg.get("master", "local[*]"))
        .appName("smogcast-local")
        # These two settings turn plain Spark into "Spark with Delta Lake":
        # SQL extensions (MERGE, DESCRIBE HISTORY) and a Delta-aware catalog.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", str(spark_cfg.get("shuffle_partitions", 8)))
        .config("spark.databricks.delta.snapshotPartitions", str(spark_cfg.get("delta_snapshot_partitions", 2)))
        .config("spark.driver.memory", spark_cfg.get("driver_memory", "4g"))
        # Adaptive Query Execution coalesces small post-shuffle partitions. It is on by
        # default on Databricks — enable it locally so query plans look alike.
        .config("spark.sql.adaptive.enabled", "true")
        # The progress bar clutters CLI and test output.
        .config("spark.ui.showConsoleProgress", "false")
        # A Derby metastore would create files in the working directory. Locally we use
        # path-based Delta tables (see storage.py), so no metastore is needed.
        .config("spark.sql.catalogImplementation", "in-memory")
    )
    # configure_spark_with_delta_pip adds spark.jars.packages with the matching Delta
    # jars. The first run downloads them from Maven Central (then cached in ~/.ivy2).
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("Local Spark session %s ready", spark.version)
    return spark
