"""Data-quality rules for hourly PM measurements (history from the archive; reused by the
live stream in stage E6).

Two kinds of problems, handled differently on purpose:
- HARD rule violations (negative, beyond the monitor range): the value is physically
  impossible → the row goes to ``ops.pm_quarantine`` with the broken rule and never reaches silver;
- SUSPICIOUS patterns (frozen sensor, single-hour spike): the row stays in silver with a
  ``dq_flag`` and ``is_valid = false`` — it is excluded from daily means but kept for analysis.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

SERIES = ["station_code", "pollutant"]


def hard_rule_failures(dq: dict) -> dict[str, Column]:
    """Rule name -> condition that is TRUE when the rule is broken."""
    hr = dq["hard_rules"]
    return {
        "below_min": F.col("value") < hr["min_ug_m3"],
        "above_max": F.col("value") > hr["max_ug_m3"],
        "missing_value": F.col("value").isNull(),
    }


def split_hard_rules(df: DataFrame, dq: dict) -> tuple[DataFrame, DataFrame]:
    """(passing rows, quarantined rows with ``failed_rules`` and ``quarantine_reason``)."""
    failed = F.array_compact(F.array(*[F.when(cond, F.lit(name)) for name, cond in hard_rule_failures(dq).items()]))
    flagged = df.withColumn("failed_rules", failed)
    ok = flagged.where(F.size("failed_rules") == 0).drop("failed_rules")
    bad = flagged.where(F.size("failed_rules") > 0).withColumn("quarantine_reason", F.array_join("failed_rules", ","))
    return ok, bad


def flag_patterns(df: DataFrame, dq: dict, series: list[str] = SERIES) -> DataFrame:
    """Add ``is_frozen``, ``is_spike``, ``dq_flag`` and ``is_valid`` (one series per ``series`` key; the
    stream adds ``source``, so a live series and the replayed demo of the same station never touch)."""
    w = Window.partitionBy(*series).orderBy("time_utc")
    one_hour = F.expr("INTERVAL 1 HOUR")

    # --- frozen: identical value in consecutive hours. A new run starts when the value changes
    # OR an hour is missing (a gap must not glue two runs together).
    prev_val, prev_t = F.lag("value").over(w), F.lag("time_utc").over(w)
    same = (F.col("value") == prev_val) & (F.col("time_utc") == prev_t + one_hour)
    df = (
        df.withColumn("_new_run", F.when(F.coalesce(same, F.lit(False)), 0).otherwise(1))
        .withColumn("_run_id", F.sum("_new_run").over(w.rowsBetween(Window.unboundedPreceding, 0)))
        .withColumn("run_length", F.count("*").over(Window.partitionBy(*series, "_run_id")))
        .withColumn("is_frozen", F.col("run_length") >= dq["frozen"]["repeat_hours"])
    )

    # --- spike: far above the HIGHEST of two neighbours on each side (only neighbours exactly
    # 1–2 hours away count; a gap leaves the neighbour out). A real episode lifts the neighbours too.
    def neighbour(offset: int) -> Column:
        lag_v = F.lag("value", offset).over(w) if offset > 0 else F.lead("value", -offset).over(w)
        lag_t = F.lag("time_utc", offset).over(w) if offset > 0 else F.lead("time_utc", -offset).over(w)
        expected = F.col("time_utc") - F.expr(f"INTERVAL {offset} HOURS")
        return F.when(lag_t == expected, lag_v)

    neighbours = F.greatest(*[neighbour(o) for o in (1, 2, -1, -2)])
    sp = dq["spike"]
    df = df.withColumn(
        "is_spike",
        F.coalesce(
            (F.col("value") > sp["min_ug_m3"]) & (F.col("value") > F.lit(sp["factor"]) * neighbours), F.lit(False)
        ),
    )
    return (
        df.withColumn("dq_flag", F.when(F.col("is_frozen"), "frozen").when(F.col("is_spike"), "spike").otherwise("ok"))
        .withColumn("is_valid", F.col("dq_flag") == "ok")
        .drop("_new_run", "_run_id")
    )
