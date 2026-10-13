"""Backtest metrics, calibration and the acceptance verdict (ARCHITECTURE.md, section 11.2).

Input: one row per (model, split, city, pollutant, issue day) with ``probability`` and the
observed ``label`` (1 = the limit was exceeded the next day). Metrics, in plain words:
- Brier: mean squared error of the probability (0 = perfect, lower is better);
- BSS: 1 - Brier / Brier of climatology on the SAME days (> 0 = better than the calendar);
- POD: share of exceedance days that got a warning (probability >= warning threshold);
- FAR: share of warnings that were false alarms;
- CSI: hits / (hits + misses + false alarms) — ignores the many quiet days;
- AUC: how well probabilities rank exceedance days above quiet days (0.5 = random).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

GROUP = ["model", "split", "pollutant", "city_id"]
ALL_CITIES = "ALL"


def with_all_cities(pred: DataFrame) -> DataFrame:
    """Duplicate rows under city_id = 'ALL' so every metric also exists for the whole country."""
    return pred.unionByName(pred.withColumn("city_id", F.lit(ALL_CITIES)))


def _auc(pred: DataFrame) -> DataFrame:
    """Mann–Whitney AUC per group with average ranks for ties (exact, no binning)."""
    w = Window.partitionBy(*GROUP).orderBy("probability")
    tie = Window.partitionBy(*GROUP, "probability")
    ranked = pred.withColumn("_rank", F.rank().over(w) + (F.count("*").over(tie) - 1) / 2.0)
    return ranked.groupBy(*GROUP).agg(
        F.sum(F.when(F.col("label") == 1, F.col("_rank"))).alias("_rank_pos"),
        F.sum("label").alias("_n_pos"),
        F.count("*").alias("_n"),
    ).select(
        *GROUP,
        # try_divide: a group without exceedances (or without quiet days) has no AUC -> NULL.
        F.try_divide(F.col("_rank_pos") - F.col("_n_pos") * (F.col("_n_pos") + 1) / 2,
                     F.col("_n_pos") * (F.col("_n") - F.col("_n_pos"))).alias("auc"),
    )


def metrics(pred: DataFrame, climatology_pred: DataFrame, warning_p: float) -> DataFrame:
    """``pred`` and ``climatology_pred`` share keys (split, pollutant, city_id, issue_day)."""
    p = with_all_cities(pred)
    warned = F.col("probability") >= warning_p
    hit, miss = warned & (F.col("label") == 1), ~warned & (F.col("label") == 1)
    false_alarm = warned & (F.col("label") == 0)
    base = p.groupBy(*GROUP).agg(
        F.count("*").alias("n_days"),
        F.sum("label").cast("long").alias("n_exceed"),
        F.avg("label").alias("base_rate"),
        F.avg((F.col("probability") - F.col("label")) ** 2).alias("brier"),
        F.sum(hit.cast("int")).alias("hits"),
        F.sum(miss.cast("int")).alias("misses"),
        F.sum(false_alarm.cast("int")).alias("false_alarms"),
    )
    clim = (
        with_all_cities(climatology_pred)
        .groupBy("split", "pollutant", "city_id")
        .agg(F.avg((F.col("probability") - F.col("label")) ** 2).alias("brier_climatology"))
    )
    hits, misses, fa = F.col("hits"), F.col("misses"), F.col("false_alarms")
    return (
        base.join(clim, ["split", "pollutant", "city_id"], "left")
        .join(_auc(p), GROUP, "left")
        # Undefined ratios (e.g. POD in a city without exceedances) become NULL, not an error.
        .withColumn("bss", 1 - F.try_divide(F.col("brier"), F.col("brier_climatology")))
        .withColumn("pod", F.try_divide(hits, hits + misses))
        .withColumn("far", F.try_divide(fa, hits + fa))
        .withColumn("csi", F.try_divide(hits, hits + misses + fa))
    )


def reliability(pred: DataFrame, n_bins: int = 10) -> DataFrame:
    """Calibration table: forecast probability bin → how often the exceedance really happened."""
    b = F.least(F.floor(F.col("probability") * n_bins), F.lit(n_bins - 1))
    return (
        pred.withColumn("bin", b.cast("int"))
        .groupBy("model", "split", "pollutant", "bin")
        .agg(F.count("*").alias("n_days"), F.avg("probability").alias("mean_probability"),
             F.avg("label").alias("observed_frequency"))
        .withColumn("bin_from", F.col("bin") / n_bins)
        .withColumn("bin_to", (F.col("bin") + 1) / n_bins)
        .withColumn("gap", F.abs(F.col("mean_probability") - F.col("observed_frequency")))
    )


def acceptance(metrics_rows: list[dict], reliability_rows: list[dict], persistence_brier: float,
               pollutant: str, model: str, crit: dict) -> list[tuple]:
    """Check K1–K4 for one pollutant's operational model on the test year (city = ALL)."""
    m = metrics_rows[0]
    out = [
        ("K1", "BSS vs climatology > threshold", m["bss"], crit["k1_brier_skill_vs_climatology_min"],
         m["bss"] is not None and m["bss"] > crit["k1_brier_skill_vs_climatology_min"]),
        ("K2", "Brier below persistence", m["brier"], persistence_brier,
         (not crit["k2_must_beat_persistence_brier"]) or m["brier"] < persistence_brier),
    ]
    bins = [r for r in reliability_rows if r["n_days"] >= crit["k3_calibration_min_bin_days"]]
    worst = max((r["gap"] for r in bins), default=0.0)
    out.append(("K3", "max calibration gap (bins >= min days)", worst, crit["k3_calibration_max_abs_gap"],
                worst <= crit["k3_calibration_max_abs_gap"]))
    if pollutant == "PM10":
        pod, far = m["pod"], m["far"]
        out.append(("K4a", "POD (PM10)", pod, crit["k4_pm10_min_pod"], pod is not None and pod >= crit["k4_pm10_min_pod"]))
        # No warnings at all → FAR undefined; that cannot count as a pass for a warning system.
        out.append(("K4b", "FAR (PM10)", far, crit["k4_pm10_max_far"], far is not None and far <= crit["k4_pm10_max_far"]))
    return [(pollutant, model, k, desc, None if v is None else float(v), float(t), bool(ok)) for k, desc, v, t, ok in out]


ACCEPTANCE_SCHEMA = ("pollutant STRING, model STRING, criterion STRING, description STRING, "
                     "value DOUBLE, threshold DOUBLE, passed BOOLEAN")
