"""Model package: metrics on hand-computed examples, baselines without leakage, acceptance
logic, and an end-to-end train + backtest + live forecast on synthetic features with a known signal."""

import datetime as dt
import math
import random

import pytest
from pyspark.sql import functions as F

from smogcast.core.schema import CALENDAR_FEATURES, PM_FEATURES, WEATHER_FEATURES
from smogcast.model import baselines, evaluate
from smogcast.model.prepare import prepare
from smogcast.model.steps import step_backtest, step_forecast, step_train

PRED = "model STRING, split STRING, pollutant STRING, city_id STRING, issue_day DATE, probability DOUBLE, label DOUBLE"
D = dt.date(2025, 1, 1)


def test_metrics_on_a_hand_computed_example(spark):
    # warning at p >= 0.5: hits=1 (0.9,1), miss=1 (0.2,1), false alarm=1 (0.6,0), quiet=1 (0.1,0)
    rows = [("m", "test", "PM10", "krakow", D, p, y) for p, y in [(0.9, 1.0), (0.2, 1.0), (0.6, 0.0), (0.1, 0.0)]]
    clim = [("climatology", "test", "PM10", "krakow", D, 0.5, y) for y in (1.0, 1.0, 0.0, 0.0)]
    pred, cp = spark.createDataFrame(rows, PRED), spark.createDataFrame(clim, PRED)
    m = {r["city_id"]: r for r in evaluate.metrics(pred, cp, 0.5).collect()}["krakow"]
    assert m["brier"] == pytest.approx((0.01 + 0.64 + 0.36 + 0.01) / 4)
    assert m["brier_climatology"] == pytest.approx(0.25)
    assert m["bss"] == pytest.approx(1 - 0.255 / 0.25)
    assert (m["pod"], m["far"], m["csi"]) == (pytest.approx(0.5), pytest.approx(0.5), pytest.approx(1 / 3))
    # positives 0.9, 0.2 vs negatives 0.6, 0.1: 3 of 4 pairs ordered correctly
    assert m["auc"] == pytest.approx(0.75)


def test_reliability_bins(spark):
    rows = [("m", "test", "PM10", "krakow", D, p, y) for p, y in [(0.72, 1.0), (0.78, 0.0), (0.05, 0.0), (1.0, 1.0)]]
    r = {x["bin"]: x for x in evaluate.reliability(spark.createDataFrame(rows, PRED)).collect()}
    assert r[7]["n_days"] == 2 and r[7]["observed_frequency"] == 0.5 and r[7]["gap"] == pytest.approx(0.25)
    assert r[9]["n_days"] == 1  # probability 1.0 lands in the top bin, not an 11th one


CRIT = {"k1_brier_skill_vs_climatology_min": 0.0, "k2_must_beat_persistence_brier": True,
        "k3_calibration_max_abs_gap": 0.15, "k3_calibration_min_bin_days": 30,
        "k4_pm10_min_pod": 0.6, "k4_pm10_max_far": 0.5}


def test_acceptance_logic():
    m = [{"bss": 0.2, "brier": 0.05, "pod": 0.7, "far": 0.4}]
    rel = [{"n_days": 100, "gap": 0.05}, {"n_days": 10, "gap": 0.9}]  # tiny bin ignored
    res = {r[2]: r[6] for r in evaluate.acceptance(m, rel, 0.08, "PM10", "logistic", CRIT)}
    assert res == {"K1": True, "K2": True, "K3": True, "K4a": True, "K4b": True}
    res = {r[2]: r[6] for r in evaluate.acceptance([{**m[0], "far": None}], rel, 0.04, "PM10", "x", CRIT)}
    assert res["K2"] is False and res["K4b"] is False   # worse than persistence; no warnings at all
    assert "K4a" not in {r[2] for r in evaluate.acceptance(m, rel, 0.08, "PM2.5", "x", CRIT)}


def synthetic_features(spark, n_days=1826, seed=1):
    """Daily features 2021-01-02 … with a known signal: tomorrow exceeds when yesterday was
    high AND the forecast wind is weak (plus noise)."""
    rng = random.Random(seed)
    rows = []
    for city, level in (("krakow", 45.0), ("gdansk", 25.0)):
        for i in range(n_days):
            issue = dt.date(2021, 1, 1) + dt.timedelta(days=i)
            target = issue + dt.timedelta(days=1)
            split = {2021: "train", 2022: "train", 2023: "train", 2024: "validation", 2025: "test"}.get(target.year, "none")
            winter = target.month in (1, 2, 11, 12)
            d1 = level * (1.6 if winter else 0.6) * math.exp(rng.gauss(0, 0.5))
            wind = abs(rng.gauss(3.0, 1.5))
            label = (math.log(d1) - 0.4 * wind + rng.gauss(0, 0.3)) > math.log(50) - 1.0
            weather = {c: 1.0 for c in WEATHER_FEATURES}
            weather.update(wind_mean_ms=wind, wind_from_deg_mean=270.0, t_mean_c=0.0 if winter else 15.0)
            pm = {"pm_d1_max_ug_m3": d1, "pm_d1_mean_ug_m3": d1 * 0.8, "pm_d1_exceeded": d1 > 50,
                  "pm_d2_max_ug_m3": d1, "pm_d_morning_max_ug_m3": d1, "pm_d_morning_mean_ug_m3": d1 * 0.9}
            cal = {"target_month": target.month, "target_dow": target.isoweekday() % 7 + 1,
                   "target_is_weekend": target.weekday() >= 5, "target_is_heating_season": winter}
            for pollutant in ("PM10", "PM2.5"):
                rows.append((city, pollutant, issue, target, split, "x", *[pm[c] for c in PM_FEATURES],
                             *[weather[c] for c in WEATHER_FEATURES], *[cal[c] for c in CALENDAR_FEATURES],
                             label, d1, True))
    types = {"pm_d1_exceeded": "BOOLEAN", "target_month": "INT", "target_dow": "INT",
             "target_is_weekend": "BOOLEAN", "target_is_heating_season": "BOOLEAN"}
    cols = [*PM_FEATURES, *WEATHER_FEATURES, *CALENDAR_FEATURES]
    schema = ("city_id STRING, pollutant STRING, issue_day DATE, target_day DATE, split STRING, weather_source STRING, "
              + ", ".join(f"{c} {types.get(c, 'DOUBLE')}" for c in cols)
              + ", exceeded_next_day BOOLEAN, next_day_max_ug_m3 DOUBLE, is_usable BOOLEAN")
    return spark.createDataFrame(rows, schema)


def test_climatology_uses_training_years_only(spark):
    p = prepare(synthetic_features(spark, n_days=1200))
    table = baselines.climatology_table(p)
    train_rate = p.where("split = 'train' AND city_id = 'krakow' AND pollutant = 'PM10' AND target_month = 1") \
        .agg({"label": "avg"}).first()[0]
    got = table.where("city_id = 'krakow' AND pollutant = 'PM10' AND target_month = 1").first()["clim_month"]
    assert got == pytest.approx(train_rate)


def test_train_backtest_and_forecast_end_to_end(ctx, monkeypatch):
    ctx.cfg["model"]["grids"] = {"logistic": {"reg_param": [0.01]}, "gbt": {"max_depth": [3], "max_iter": [20]}}
    ctx.storage.overwrite(synthetic_features(ctx.spark), "silver", "features")
    step_train(ctx)
    step_backtest(ctx)

    sel = ctx.storage.read("gold", "model_selection").where("operational").collect()
    assert {r["pollutant"] for r in sel} == {"PM10", "PM2.5"}
    m = {(r["model"], r["pollutant"]): r for r in
         ctx.storage.read("gold", "backtest_metrics").where("split = 'test' AND city_id = 'ALL'").collect()}
    op = {r["pollutant"]: r["model"] for r in sel}
    # The known signal must be found: clearly better than the calendar and than persistence.
    assert m[(op["PM10"], "PM10")]["bss"] > 0.2
    assert m[(op["PM10"], "PM10")]["brier"] < m[("persistence", "PM10")]["brier"]
    verdict = ctx.storage.read("gold", "acceptance").collect()
    assert {r["criterion"] for r in verdict if r["pollutant"] == "PM10"} == {"K1", "K2", "K3", "K4a", "K4b"}

    # Live forecast with the saved final models: one day of features, one row not usable.
    d = dt.date(2026, 1, 20)
    monkeypatch.setenv("SMOGCAST_ISSUE_DATE", d.isoformat())
    live = (
        synthetic_features(ctx.spark, n_days=30).where(F.col("issue_day") == dt.date(2021, 1, 20))
        .withColumn("issue_day", F.lit(d)).withColumn("target_day", F.lit(d + dt.timedelta(days=1)))
        .withColumn("split", F.lit("live")).withColumn("exceeded_next_day", F.lit(None).cast("boolean"))
        .withColumn("is_usable", ~((F.col("city_id") == "gdansk") & (F.col("pollutant") == "PM10")))
        .withColumn("unusable_reason", F.when(~F.col("is_usable"), "no_valid_pm_yesterday"))
    )
    ctx.storage.overwrite(live, "silver", "features_live")
    step_forecast(ctx)
    step_forecast(ctx)                                     # MERGE: same day replaces, no duplicates
    fc = {(r["city_id"], r["pollutant"]): r for r in ctx.storage.read("gold", "forecast_tomorrow").collect()}
    assert len(fc) == 4
    ok = fc[("krakow", "PM10")]
    assert ok["status"] == "ok" and 0.0 <= ok["probability"] <= 1.0 and ok["model"] == op["PM10"]
    assert ok["target_day"] == d + dt.timedelta(days=1) and ok["jurisdiction_code"] == "PL-12"
    assert ok["warned"] == (ok["probability"] >= 0.5)
    missing = fc[("gdansk", "PM10")]
    assert missing["status"] == "no_forecast" and missing["probability"] is None
    assert missing["unusable_reason"] == "no_valid_pm_yesterday"


def test_gbt_is_the_same_whatever_the_partitioning(spark):
    # Regression (2026-10-06): the same GBT settings scored differently in Docker and on Databricks because
    # GBT samples split thresholds per partition; fit_input fixes one partition and one row order.
    from smogcast.model import train

    p = prepare(synthetic_features(spark, n_days=400)).where("pollutant = 'PM10'")
    rows = p.where("split = 'train'")
    probs = []
    for parts in (1, 7):
        data = train.fit_input(rows.repartition(parts, "issue_day"))  # different input layouts
        model = train.build_pipeline("gbt", {"max_depth": 3, "max_iter": 10}, seed=42).fit(data)
        probs.append([r["probability"] for r in train.predict(model, p).orderBy("city_id", "issue_day").collect()])
    assert probs[0] == probs[1]


def test_float_noise_in_inputs_does_not_reach_the_model(spark):
    # Regression (2026-10-06): last-bit differences between Docker and Databricks changed the GBT trees.
    from smogcast.model.prepare import NUMERIC_FEATURES

    raw = synthetic_features(spark, n_days=60)
    noisy = raw.withColumns({c: F.col(c) * F.lit(1.0 + 2e-16) for c in ("pm_d1_max_ug_m3", "pm_d1_mean_ug_m3",
                                                                        "wind_mean_ms", "t_mean_c")})
    def rows(df):
        return [tuple(r) for r in prepare(df).select(*NUMERIC_FEATURES).orderBy(*NUMERIC_FEATURES).collect()]
    assert rows(raw) == rows(noisy)
