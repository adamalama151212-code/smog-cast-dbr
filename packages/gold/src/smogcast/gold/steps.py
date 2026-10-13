"""Gold steps: each is ``(Context) -> None``.

Tomorrow's forecast (gold.forecast_tomorrow) is written by ``smogcast-model --step forecast``:
it needs the saved model and the model's feature preparation, and step wheels never import
each other (ARCHITECTURE.md, section 2).
"""

from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import functions as F

from smogcast.core.config import active_cities
from smogcast.core.context import Context
from smogcast.core.dq_metrics import log_metrics
from smogcast.core.timeutil import issue_day_cet
from smogcast.gold import governance, measured
from smogcast.gold.dq_summary import build_dq_summary

log = logging.getLogger(__name__)

STREAM_SOURCES = ["live", "fault_replay"]
EMPTY = {
    ("bronze", "pm_stream"): "source STRING, station_code STRING, pollutant STRING, time_utc TIMESTAMP",
    ("ops", "pm_late_rejected"): "source STRING, station_code STRING, pollutant STRING, time_utc TIMESTAMP",
    ("ops", "pm_stream_quarantine"): ("source STRING, station_code STRING, pollutant STRING, time_utc TIMESTAMP, "
                                      "quarantine_reason STRING"),
    ("silver", "pm_stream"): ("source STRING, station_code STRING, pollutant STRING, time_utc TIMESTAMP, "
                              "dq_flag STRING, is_valid BOOLEAN"),
}


def step_dq_summary(ctx: Context) -> None:
    """Stream tables → gold.dq_summary (full overwrite: a few dozen rows recomputed from the sources)."""
    st = ctx.storage

    def stream_table(layer: str, name: str):
        # A table can be missing legitimately, e.g. no reading was ever late.
        df = st.read(layer, name) if st.exists(layer, name) else ctx.spark.createDataFrame([], EMPTY[(layer, name)])
        return df.where(F.col("source").isin(STREAM_SOURCES))

    summary = build_dq_summary(*(stream_table(layer, name) for layer, name in EMPTY))
    st.overwrite(summary, "gold", "dq_summary")
    for r in st.read("gold", "dq_summary").orderBy("source", "metric").collect():
        log.info("dq-summary %s %s = %s (%s%%)", r["source"], r["metric"], r["value"], r["pct_of_unique"])


def step_measured_daily(ctx: Context) -> None:
    """silver.pm_daily_city (archive) + silver.pm_daily_city_live → gold.measured_daily, then official forecasts vs
    measured → gold.forecast_verification (both full overwrite; measured.py).

    Runs in the batch Job (archive only, no live table yet on a fresh environment) and in the live Job (after the
    forecast and the live daily means). Verification needs gold.forecast_history — skipped until the first live run.
    """
    st, cfg, spark = ctx.storage, ctx.cfg, ctx.spark
    city_cols = "city_id STRING, pollutant STRING, day_cet DATE, max_station_mean_ug_m3 DOUBLE, " \
                "mean_station_mean_ug_m3 DOUBLE, n_stations BIGINT, limit_ug_m3 DOUBLE, exceeded BOOLEAN"

    def city_daily(name: str):
        return st.read("silver", name) if st.exists("silver", name) else spark.createDataFrame([], city_cols)

    city_info = spark.createDataFrame([(c["city_id"], c["name"], c["jurisdiction_code"]) for c in active_cities(cfg).values()],
                                      "city_id STRING, city_name STRING, jurisdiction_code STRING")
    st.overwrite(measured.build_measured_daily(city_daily("pm_daily_city"), city_daily("pm_daily_city_live"), city_info),
                 "gold", "measured_daily")
    m = st.read("gold", "measured_daily")
    metrics = {f"days_{r['source']}": r["count"] for r in m.groupBy("source").count().collect()}

    if st.exists("gold", "forecast_history"):
        v = measured.build_verification(st.read("gold", "forecast_history"), m, issue_day_cet())
        st.overwrite(v, "gold", "forecast_verification")
        for r in st.read("gold", "forecast_verification").groupBy("outcome").count().collect():
            metrics[f"verification_{r['outcome']}"] = r["count"]
    else:
        log.info("measured-daily: no gold.forecast_history yet — forecast verification skipped")
    log.info("measured-daily: %s", metrics)
    log_metrics(st, "gold_measured_daily", metrics)


def _gold_tables(ctx: Context) -> list[str]:
    st = ctx.storage
    if st.mode == "path":
        root = Path(st.table_path("gold", "_")).parent
        return sorted(p.name for p in root.iterdir() if p.is_dir()) if root.exists() else []
    return sorted(r["tableName"] for r in ctx.spark.sql(f"SHOW TABLES IN {st.schema_name('gold')}").collect())


def step_governance(ctx: Context) -> None:
    """Gold data dictionary → column comments and tags + row/column-secured views in ``gold_secure`` (governance.py).

    Runs at the end of the batch and live Jobs, so comments and views follow every write. First it checks that the
    dictionary documents every column of every gold table — an undocumented column fails the step (and the Job e-mails).
    Locally (no Unity Catalog, no account groups) only that check runs.
    """
    st, cfg, spark = ctx.storage, ctx.cfg, ctx.spark
    dictionary = cfg["gold_dictionary"]
    problems = [f"gold.{t}: table not in conf/gold_dictionary.yaml" for t in _gold_tables(ctx) if t not in dictionary]
    columns = {}
    for table, entry in dictionary.items():
        if st.exists("gold", table):   # e.g. forecast_history before the first live run
            columns[table] = st.read("gold", table).columns
            problems += governance.check_columns(table, entry, columns[table])
    if problems:
        raise ValueError("Gold data dictionary out of date:\n" + "\n".join(problems))
    metrics = {"tables_documented": len(columns), "columns_documented": sum(map(len, columns.values()))}
    if st.mode != "catalog":
        log.info("governance: dictionary covers %d gold tables (comments, tags and secure views: Unity Catalog only)",
                 len(columns))
        log_metrics(st, "gold_governance", metrics)
        return

    g = cfg["governance"]
    for table, cols in columns.items():
        for stmt in governance.comment_statements(st.table_name("gold", table), dictionary[table], cols):
            spark.sql(stmt)
    access = st.table_name("ops", g["access_table"])
    for stmt in governance.access_table_statements(access, g.get("seed_access", []), g.get("access_editors", [])):
        spark.sql(stmt)
    secure = st.schema_name(g["secure_schema"])
    if not spark.catalog.databaseExists(secure):
        # The schema and its grants come from Terraform (module unity_catalog / governance) — never created here.
        log.warning("governance: schema %s missing — secure views skipped (terraform apply first)", secure)
        metrics["secure_views"] = 0
    else:
        for table, cols in columns.items():
            spark.sql(governance.secure_view(cfg, f"{secure}.{table}", st.table_name("gold", table), access,
                                             dictionary[table], cols))
        metrics["secure_views"] = len(columns)
    log.info("governance: %s", metrics)
    log_metrics(st, "gold_governance", metrics)
