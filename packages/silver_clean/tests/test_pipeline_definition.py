"""The declarative pipeline file (pipelines/smogcast_stream) declares the expected graph with the wheel's rules.

Local Spark 4.0 has no ``pyspark.pipelines``, so the file runs against a recorder that stands in for it: what is
checked is what the file DECLARES (tables, flows, expectations); what the tables CONTAIN is the job of the
pm_stream tests, because every table is a function of smogcast.silver_clean.pm_stream.
"""

import os
import sys
import types
from pathlib import Path

import pyspark
import pytest

from smogcast.core.config import load_config
from smogcast.silver_clean import pm_stream as ps

PIPELINE = Path(__file__).resolve().parents[3] / "pipelines" / "smogcast_stream" / "transformations" / "stream.py"


class Recorder(types.ModuleType):
    def __init__(self):
        super().__init__("pyspark.pipelines")
        self.datasets: dict[str, dict] = {}
        self.flows: list[tuple[str, str]] = []

    def create_streaming_table(self, name, **kw):
        self.datasets[name] = {"kind": "streaming_table", **kw}

    def append_flow(self, target, name):
        def deco(fn):
            self.flows.append((name, target))
            return fn
        return deco

    def _dataset(self, kind):
        def factory(name, **kw):
            def deco(fn):
                self.datasets[name] = {"kind": kind, "drop": getattr(fn, "_drop", {}), "warn": getattr(fn, "_warn", {})}
                return fn
            return deco
        return factory

    def __getattr__(self, attr):
        if attr in ("temporary_view", "materialized_view"):
            return self._dataset(attr)
        if attr in ("expect_all_or_drop", "expect_all"):
            key = "_drop" if attr == "expect_all_or_drop" else "_warn"

            def expectation(rules):
                def deco(fn):
                    setattr(fn, key, {**getattr(fn, key, {}), **rules})
                    return fn
                return deco
            return expectation
        raise AttributeError(attr)


@pytest.mark.parametrize("env", ["dev", "prod"])   # prod: prefixed schemas in a shared catalog
def test_pipeline_declares_the_stream_graph_with_the_wheels_rules(spark, monkeypatch, env):
    dp = Recorder()
    monkeypatch.setitem(sys.modules, "pyspark.pipelines", dp)
    monkeypatch.setattr(pyspark, "pipelines", dp, raising=False)
    monkeypatch.setattr(os, "makedirs", lambda *a, **k: None)   # /Volumes does not exist here
    spark.conf.set("smogcast.env", env)
    try:
        exec(compile(PIPELINE.read_text(encoding="utf-8"), str(PIPELINE), "exec"), {"__name__": "stream"})
    finally:
        spark.conf.unset("smogcast.env")

    storage = load_config(env)["storage"]
    cat = f"{storage['catalog']}.{storage.get('schema_prefix', '')}"   # e.g. "smogcast_dev." or "dbr_dev.smogcast_"
    dq = load_config(env)["pm_dq"]
    d = dp.datasets
    assert d[f"{cat}bronze.pm_stream"]["kind"] == "streaming_table"
    assert d[f"{cat}bronze.pm_stream"]["expect_all_or_fail"] == ps.BRONZE_FAIL_RULES
    assert sorted(dp.flows) == sorted((f"ingest_{s}", f"{cat}bronze.pm_stream") for s in ps.SOURCES)
    assert {n: x["kind"] for n, x in d.items() if x["kind"] == "materialized_view"} == {
        f"{cat}ops.pm_late_rejected": "materialized_view",
        f"{cat}ops.pm_stream_quarantine": "materialized_view",
        f"{cat}silver.pm_stream": "materialized_view",
    }
    # The pipeline drops exactly what the local step filters out — the same rule strings.
    assert d["stream_accepted"]["drop"] == {**ps.ON_TIME_RULE, **ps.accept_rules(dq)}
    assert d[f"{cat}silver.pm_stream"]["warn"] == ps.FLAG_RULES
