"""Integration test: the whole pipeline on synthetic inputs (smogcast.ingest.synthetic) — the same steps in
the same order as the Lakeflow Jobs and `smogcast run-batch / run-live / run-fault-demo`.

Beyond the unit tests it shows that the steps fit together (each finds what the previous one wrote), that a
re-run duplicates nothing, that the injected faults end where they should and that tomorrow's forecast has a
row for every city and pollutant. No internet needed, so it runs in CI. Skip it locally with
`pytest -m "not integration"`.
"""

from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import functions as F

from smogcast.cli.main import BATCH_ORDER, FAULT_DEMO_ORDER, LIVE_ORDER, PACKAGES
from smogcast.core.config import load_config
from smogcast.core.context import Context
from smogcast.core.storage import DATA_DIR_VAR, Storage
from smogcast.core.timeutil import issue_day_cet
from smogcast.model import steps as model_steps

pytestmark = pytest.mark.integration

# Small but complete: 2 cities, January–February of 2021–2025 (train, validation and test years), tiny grids.
OVERRIDES = {
    "sources": {"pm": "synthetic", "weather": "synthetic"},
    "run": {"cities": ["krakow", "gdansk"]},
    "synthetic": {"months": [1, 2]},
    "model": {"grids": {"logistic": {"reg_param": [0.01]}, "gbt": {"max_depth": [2], "max_iter": [5]}}},
}
DATA_STEPS = [s for s in BATCH_ORDER if s[0] != "model"]  # history up to the features (no training)


def run(ctx: Context, order: list[tuple[str, str]]) -> None:
    for package, step in order:
        PACKAGES[package][step](ctx)


def counts(ctx: Context, tables: list[tuple[str, str]]) -> dict[str, int]:
    return {f"{layer}.{name}": ctx.storage.read(layer, name).count() for layer, name in tables}


@pytest.fixture(scope="module")
def pipeline(spark, tmp_path_factory):
    """Run batch → live → fault demo once for the whole module; tests only read the results."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv(DATA_DIR_VAR, str(tmp_path_factory.mktemp("integration") / "data"))
        cfg = load_config("local", OVERRIDES)
        ctx = Context(cfg, spark, Storage(spark, cfg))
        run(ctx, BATCH_ORDER)
        history = [("bronze", "pm_hourly"), ("silver", "pm_hourly"), ("silver", "pm_daily_city"),
                   ("silver", "features"), ("bronze", "weather_forecast")]
        first = counts(ctx, history)
        run(ctx, DATA_STEPS)  # re-run of the history must change nothing
        second = counts(ctx, history)
        # Two live runs of the same issue day with a fixed CET clock (the run kind depends on the time of day):
        # 12:00 → the official forecast, 18:00 → an update; then a RETRY of the second run's forecast task.
        day = issue_day_cet()
        for run_id, hour in (("run-1", 12), ("run-2", 18)):
            ctx.run_id = run_id
            mp.setattr(model_steps, "now_cet", lambda h=hour: dt.datetime.combine(day, dt.time(h)))
            run(ctx, LIVE_ORDER)  # the second poll: the API re-sends the last days
        mp.setattr(model_steps, "now_cet", lambda: dt.datetime.combine(day, dt.time(18, 10)))
        PACKAGES["model"]["forecast"](ctx)
        run(ctx, FAULT_DEMO_ORDER)
        yield ctx, first, second


def test_history_path_builds_every_layer(pipeline):
    ctx, first, _ = pipeline
    assert all(n > 0 for n in first.values()), first
    feats = ctx.storage.read("silver", "features")
    assert {r["split"] for r in feats.where("is_usable").select("split").distinct().collect()} >= {
        "train", "validation", "test"}


def test_rerun_of_the_history_duplicates_nothing(pipeline):
    _, first, second = pipeline
    assert first == second


def test_model_is_chosen_and_judged_for_each_pollutant(pipeline):
    ctx, _, _ = pipeline
    sel = ctx.storage.read("gold", "model_selection").where("operational").collect()
    assert sorted(r["pollutant"] for r in sel) == ["PM10", "PM2.5"]
    acc = ctx.storage.read("gold", "acceptance")
    assert {r["pollutant"] for r in acc.select("pollutant").distinct().collect()} == {"PM10", "PM2.5"}
    assert acc.where("criterion = 'K4a'").count() == 1  # K4 applies to PM10 only


def test_tomorrow_has_a_forecast_for_every_city_and_pollutant(pipeline):
    ctx, _, _ = pipeline
    rows = ctx.storage.read("gold", "forecast_tomorrow").collect()
    assert len(rows) == 4
    assert all(r["status"] == "ok" and 0 <= r["probability"] <= 1 for r in rows), rows
    assert {r["run_kind"] for r in rows} == {"update"}   # the current forecast = the newest run (18:00)


def test_forecast_history_keeps_every_run_once_and_one_official(pipeline):
    ctx, _, _ = pipeline
    hist = ctx.storage.read("gold", "forecast_history").collect()
    runs = {}
    for r in hist:
        runs.setdefault(r["run_id"], set()).add(r["run_kind"])
    # 4 rows (2 cities × 2 pollutants) per run; the retry of run-2 replaced its own rows instead of adding more
    assert len(hist) == 8 and runs == {"run-1": {"official"}, "run-2": {"update"}}, runs


def test_repeated_live_polls_keep_one_row_per_reading(pipeline):
    ctx, _, _ = pipeline
    live = ctx.storage.read("silver", "pm_stream").where("source = 'live'")
    keys = ["station_code", "pollutant", "time_utc"]
    assert live.count() > 0 and live.count() == live.select(*keys).distinct().count()


def test_injected_faults_end_where_they_should(pipeline):
    ctx, _, _ = pipeline
    st = ctx.storage
    reasons = {r["quarantine_reason"] for r in
               st.read("ops", "pm_stream_quarantine").where("source = 'fault_replay'").select("quarantine_reason").collect()}
    assert {"below_min", "above_max", "missing_value"} <= reasons
    assert st.read("ops", "pm_late_rejected").where("source = 'fault_replay'").count() > 0
    flags = {r["dq_flag"] for r in st.read("silver", "pm_stream").where("source = 'fault_replay'")
             .select("dq_flag").distinct().collect()}
    assert {"frozen", "spike", "drift"} <= flags
    # the registry change of the fault schedule (a station removed, a sensor added) reaches the CDC feed
    ops = {r["op"] for r in st.read("silver", "station_changes").where(F.col("registry_source") == "fault_replay")
           .select("op").distinct().collect()}
    assert {"INSERT", "UPDATE", "DELETE"} <= ops
