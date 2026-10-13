"""Model steps: ``train`` (tune on validation, refit), ``backtest`` (score, metrics, verdict) and
``forecast`` (tomorrow's probability from live features with the saved final model)."""

from __future__ import annotations

import datetime as dt
import json
import logging

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from smogcast.core.context import Context
from smogcast.core.dq_metrics import log_metrics
from smogcast.core.timeutil import issue_day_cet, now_cet
from smogcast.model import baselines, evaluate, forecast_runs, train
from smogcast.model.prepare import prepare

log = logging.getLogger(__name__)

PRED_COLS = ["model", "split", "city_id", "pollutant", "issue_day", "target_day", "probability", "label"]


def model_dir(ctx: Context, pollutant: str, model: str, kind: str) -> str:
    return ctx.storage.models(pollutant.replace(".", ""), model, kind)


def _prepared(ctx: Context) -> DataFrame:
    return prepare(ctx.storage.read("silver", "features")).cache()


def step_train(ctx: Context) -> None:
    """Tune every model type on the validation year, save tuned and final (refit) models."""
    mcfg, seed = ctx.cfg["model"], ctx.run["seed"]
    prepared = _prepared(ctx)
    rows = []
    for pollutant in ctx.cfg["pollutants"]:
        p = prepared.where(F.col("pollutant") == pollutant)
        results = train.tune(p, mcfg, seed)
        best, operational = train.choose(results)
        rows += train.selection_rows(pollutant, results, best, operational)
        for model_type, r in best.items():
            tuned = train.build_pipeline(model_type, r["params"], seed).fit(train.fit_input(p.where("split = 'train'")))
            tuned.write().overwrite().save(model_dir(ctx, pollutant, model_type, "tuned"))
            refit_split = "split IN ('train', 'validation')" if mcfg["refit_on_train_and_validation"] else "split = 'train'"
            final = train.build_pipeline(model_type, r["params"], seed).fit(train.fit_input(p.where(refit_split)))
            final.write().overwrite().save(model_dir(ctx, pollutant, model_type, "final"))
        log.info("%s: operational model = %s (validation Brier %.5f)", pollutant, operational,
                 best[operational]["validation_brier"])
    ctx.storage.overwrite(ctx.spark.createDataFrame(rows, train.SELECTION_SCHEMA), "gold", "model_selection")
    prepared.unpersist()


def step_backtest(ctx: Context) -> None:
    """Score validation (tuned models) and test (final models) plus baselines; metrics and K1–K4."""
    st, mcfg = ctx.storage, ctx.cfg["model"]
    warning_p = ctx.cfg["thresholds"]["warning_probability"]
    prepared = _prepared(ctx)
    scored = prepared.where("split IN ('validation', 'test')")
    clim_table = baselines.climatology_table(prepared)

    preds = [
        baselines.persistence(scored).withColumn("model", F.lit("persistence")),
        baselines.climatology(scored, clim_table).withColumn("model", F.lit("climatology")),
    ]
    selection = st.read("gold", "model_selection").where("best_of_type").collect()
    operational = {r["pollutant"]: r["model"] for r in selection if r["operational"]}
    for r in selection:
        p = scored.where(F.col("pollutant") == r["pollutant"])
        for split, kind in (("validation", "tuned"), ("test", "final")):
            model = PipelineModel.load(model_dir(ctx, r["pollutant"], r["model"], kind))
            preds.append(train.predict(model, p.where(F.col("split") == split)).withColumn("model", F.lit(r["model"])))

    all_preds = preds[0].select(*PRED_COLS)
    for d in preds[1:]:
        all_preds = all_preds.unionByName(d.select(*PRED_COLS))
    st.overwrite(all_preds.withColumn("warned", F.col("probability") >= warning_p), "gold", "backtest_predictions")
    pred = st.read("gold", "backtest_predictions").cache()

    m = evaluate.metrics(pred, pred.where("model = 'climatology'"), warning_p)
    st.overwrite(m, "gold", "backtest_metrics")
    st.overwrite(evaluate.reliability(pred), "gold", "reliability")

    metrics_tbl, rel_tbl = st.read("gold", "backtest_metrics"), st.read("gold", "reliability")
    if metrics_tbl.where("split = 'test'").isEmpty():
        raise ValueError(f"No usable rows in the test year {mcfg['split']['test_year']} — cannot judge the model")
    verdict_rows = []
    for pollutant, model in operational.items():
        sel = (F.col("pollutant") == pollutant) & (F.col("split") == "test")
        mrows = [r.asDict() for r in metrics_tbl.where(sel & (F.col("model") == model) & (F.col("city_id") == "ALL")).collect()]
        persistence_brier = metrics_tbl.where(
            sel & (F.col("model") == "persistence") & (F.col("city_id") == "ALL")).first()["brier"]
        rrows = [r.asDict() for r in rel_tbl.where(sel & (F.col("model") == model)).collect()]
        verdict_rows += evaluate.acceptance(mrows, rrows, persistence_brier, pollutant, model, mcfg["acceptance"])
    st.overwrite(ctx.spark.createDataFrame(verdict_rows, evaluate.ACCEPTANCE_SCHEMA), "gold", "acceptance")

    summary = {}
    for r in metrics_tbl.where("city_id = 'ALL'").collect():
        key = f"{r['split']}_{r['pollutant']}_{r['model']}"
        summary[f"brier_{key}"], summary[f"bss_{key}"] = r["brier"], r["bss"] if r["bss"] is not None else float("nan")
    for pollutant, model, k, _desc, value, threshold, passed in verdict_rows:
        log.info("ACCEPTANCE %s/%s %s: value=%s threshold=%s -> %s", pollutant, model, k, value, threshold,
                 "PASS" if passed else "FAIL")
        summary[f"accept_{pollutant}_{k}"] = 1.0 if passed else 0.0
    log_metrics(st, "model_backtest", summary)
    log.info("operational models: %s", json.dumps(operational))
    pred.unpersist()
    prepared.unpersist()


# Columns shown next to the probability, so a reader sees WHY the model expects smog tomorrow.
EXPLAIN_COLS = ["pm_d1_max_ug_m3", "pm_d_morning_max_ug_m3", "t_mean_c", "wind_mean_ms", "calm_hours", "precip_sum_mm"]
FORECAST_KEYS = ["city_id", "pollutant", "target_day"]
HISTORY_KEYS = ["run_id", *FORECAST_KEYS]


def _run_kind(ctx: Context, day: dt.date) -> str:
    """preliminary / official / update for this run (forecast_runs) — needs the runs already in the history."""
    st = ctx.storage
    earlier = ({r["run_id"]: r["run_kind"] for r in st.read("gold", "forecast_history")
                .where(F.col("issue_day") == F.lit(day)).select("run_id", "run_kind").distinct().collect()}
               if st.exists("gold", "forecast_history") else {})
    return forecast_runs.run_kind(now_cet().time(), forecast_runs.official_from(ctx.cfg["model"]), earlier, ctx.run_id)


def step_forecast(ctx: Context) -> None:
    """silver.features_live (issue day D) → gold.forecast_tomorrow + gold.forecast_history: P(exceedance on D+1).

    Uses each pollutant's OPERATIONAL model (chosen on the validation year, gold.model_selection)
    in its ``final`` version — the same saved model that was judged on the test year. A city whose
    features are not usable gets a row with NULL probability and the reason, never a guess.

    Every run is labelled ``run_kind`` = preliminary / official / update (forecast_runs) and
    - kept in gold.forecast_history — MERGE on (run_id, city, pollutant, target day): every run adds its rows, a
      retried task of the same run replaces its own;
    - shown as the CURRENT forecast in gold.forecast_tomorrow — MERGE on (city, pollutant, target day): the newest
      run replaces the day's forecast, with its kind and issue time (decision of the author: always the newest).
    """
    st, cfg = ctx.storage, ctx.cfg
    warning_p = cfg["thresholds"]["warning_probability"]
    day = issue_day_cet()
    kind = _run_kind(ctx, day)
    if not st.exists("gold", "model_selection"):
        raise FileNotFoundError("gold.model_selection missing — run `smogcast-model --step train` first")
    operational = {r["pollutant"]: r["model"] for r in st.read("gold", "model_selection").where("operational").collect()}
    feats = st.read("silver", "features_live").where(F.col("issue_day") == F.lit(day)).cache()
    if feats.isEmpty():
        raise ValueError(f"No live features for issue day {day} — run `smogcast-silver-transform --step features-live`")

    scored = []
    prepared = prepare(feats)  # keeps usable rows only
    for pollutant, model in operational.items():
        rows = prepared.where(F.col("pollutant") == pollutant)
        fitted = PipelineModel.load(model_dir(ctx, pollutant, model, "final"))
        scored.append(train.predict(fitted, rows).select(*FORECAST_KEYS, "probability", F.lit(model).alias("model")))
    probs = scored[0]
    for s in scored[1:]:
        probs = probs.unionByName(s)

    cities = cfg["cities"]
    city_info = ctx.spark.createDataFrame(
        [(c, cities[c]["name"], cities[c]["jurisdiction_code"]) for c in cities],
        "city_id STRING, city_name STRING, jurisdiction_code STRING")
    limits = cfg["thresholds"]["daily_limit_ug_m3"]
    limit = F.create_map(*[x for p, lim in limits.items() for x in (F.lit(p), F.lit(float(lim)))])
    out = (
        feats.select(*FORECAST_KEYS, "issue_day", "weather_source", "is_usable", "unusable_reason", *EXPLAIN_COLS)
        .join(probs, FORECAST_KEYS, "left")
        .join(F.broadcast(city_info), "city_id", "left")
        .withColumn("limit_ug_m3", limit[F.col("pollutant")])
        .withColumn("warned", F.col("probability") >= warning_p)
        .withColumn("status", F.when(F.col("probability").isNotNull(), "ok").otherwise("no_forecast"))
        .withColumn("model_version", F.lit("final"))
        .withColumn("run_kind", F.lit(kind))
        .withColumn("run_id", F.lit(ctx.run_id))
        .withColumn("issued_at", F.current_timestamp())
        .select("city_id", "city_name", "jurisdiction_code", "pollutant", "issue_day", "target_day", "probability",
                "warned", "limit_ug_m3", "status", "unusable_reason", "model", "model_version", "weather_source",
                *EXPLAIN_COLS, "run_kind", "run_id", "issued_at")
    ).localCheckpoint()   # one issued_at for both tables (current_timestamp would be evaluated per write)
    st.merge(out, "gold", "forecast_history", HISTORY_KEYS)
    st.merge(out, "gold", "forecast_tomorrow", FORECAST_KEYS)
    feats.unpersist()

    target = day + dt.timedelta(days=1)
    written = st.read("gold", "forecast_tomorrow").where(F.col("target_day") == F.lit(target)).collect()
    log.info("forecast run %s (%s) for %s", ctx.run_id, kind, target)
    for r in sorted(written, key=lambda r: (r["pollutant"], r["city_id"])):
        log.info("forecast %s %s %s: %s", r["target_day"], r["pollutant"], r["city_id"],
                 f"P={r['probability']:.2f}" if r["probability"] is not None else f"none ({r['unusable_reason']})")
    log_metrics(st, "model_forecast", {
        "rows": len(written), "with_probability": sum(r["probability"] is not None for r in written),
        "warnings": sum(bool(r["warned"]) for r in written),
        **{f"run_{k}": float(k == kind) for k in forecast_runs.RUN_KINDS},
    })
