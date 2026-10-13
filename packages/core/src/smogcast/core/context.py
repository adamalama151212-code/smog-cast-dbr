"""Run context: configuration + Spark session + storage layer.

Every pipeline step receives this one object instead of three arguments. Steps never
create a session or read configuration themselves — so the same step runs from the
local CLI, from a Databricks Job task (wheel entry point) and from pytest (which passes
a small configuration and a temporary data directory).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field

from pyspark.sql import SparkSession

from smogcast.core.config import OFFLINE_OVERRIDES, load_config
from smogcast.core.storage import Storage


def local_run_id() -> str:
    """Run id when no orchestrator gives one (local CLI, tests): one per context, i.e. per `smogcast run-…`."""
    return "local-" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


@dataclass
class Context:
    cfg: dict
    spark: SparkSession
    storage: Storage
    # Identifies one run of a pipeline order. On Databricks = the Job run id ({{job.run_id}}), shared by every
    # task of the run and by the RETRIES of a task — so a retried step can recognise what its first attempt wrote.
    run_id: str = field(default_factory=local_run_id)

    @property
    def run(self) -> dict:
        return self.cfg["run"]


def build_context(env: str | None = None, offline: bool = False, overrides: dict | None = None,
                  run_id: str | None = None) -> Context:
    # Spark is imported lazily inside get_spark's module, so `--help` stays instant.
    from smogcast.core.config import deep_merge
    from smogcast.core.session import get_spark

    extra = deep_merge(OFFLINE_OVERRIDES, overrides or {}) if offline else overrides
    cfg = load_config(env, extra)
    spark = get_spark(cfg)
    ctx = Context(cfg, spark, Storage(spark, cfg))
    if run_id:
        ctx.run_id = run_id
    return ctx
