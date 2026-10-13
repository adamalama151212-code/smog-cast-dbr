"""Gold governance (E9 point 5): the data dictionary check and the secure views' row / column rules.

The views' query runs on local Spark: Unity Catalog's is_account_group_member() is stood in for by a test function
reading a set of groups, and the access table is a local view — the same SQL text as on Databricks otherwise.
"""

import datetime as dt

import pytest

from smogcast.core.config import load_config
from smogcast.gold import governance as gov

CFG = load_config("local")
DICT = CFG["gold_dictionary"]


@pytest.fixture
def env(spark):
    """Local stand-ins: gold tables as temp views, the access table, and the group-membership function."""
    me = spark.sql("SELECT current_user() AS u").first()["u"]
    cols = list(DICT["forecast_tomorrow"]["columns"])
    rows = []
    for city, code in (("krakow", "PL-12"), ("wroclaw", "PL-02"), ("gdansk", "PL-22")):
        r = dict.fromkeys(cols)
        r.update(city_id=city, jurisdiction_code=code, pollutant="PM10", probability=0.3, run_id="run-7",
                 model_version="final", issued_at=dt.datetime(2026, 10, 8, 10, 42))
        rows.append(tuple(r[c] for c in cols))
    schema = ", ".join(f"{c} {'TIMESTAMP' if c == 'issued_at' else 'DOUBLE' if c == 'probability' else 'STRING'}"
                       for c in cols)
    spark.createDataFrame(rows, schema).createOrReplaceTempView("t_forecast")
    spark.createDataFrame([("krakow", "PM10"), ("ALL", "PM10")], "city_id STRING, pollutant STRING") \
        .createOrReplaceTempView("t_metrics")

    def query(access: list[tuple], table="forecast_tomorrow", groups=()):
        member_of = frozenset(groups)   # registered per query: a Python UDF ships a COPY of what it reads
        spark.udf.register("is_account_group_member", lambda g: g in member_of, "boolean")
        spark.createDataFrame([(me, *a) for a in access], "principal STRING, role STRING, region STRING") \
            .createOrReplaceTempView("t_access")
        entry, base = DICT[table], ("t_forecast" if table.startswith("forecast") else "t_metrics")
        columns = cols if base == "t_forecast" else ["city_id", "pollutant"]
        if base == "t_metrics":
            entry = {**entry, "columns": {c: entry["columns"][c] for c in columns}}
        return spark.sql(gov.secure_select(CFG, base, "t_access", entry, columns)).collect()

    return query


def test_admin_sees_every_row_and_column(env):
    rows = env([("admin", None)])
    assert len(rows) == 3 and {r["run_id"] for r in rows} == {"run-7"}


def test_analyst_sees_every_row_but_technical_columns_are_masked(env):
    rows = env([("analyst", None)])
    assert len(rows) == 3
    assert all(r["run_id"] is None and r["model_version"] is None for r in rows)   # technical → NULL
    assert all(r["probability"] == 0.3 for r in rows)                             # business → as is


def test_region_sees_only_its_voivodeship(env):
    rows = env([("region", "PL-12")])
    assert [r["city_id"] for r in rows] == ["krakow"] and rows[0]["run_id"] is None
    assert sorted(r["city_id"] for r in env([("region", "PL-12"), ("region", "PL-02")])) == ["krakow", "wroclaw"]


def test_city_tables_are_mapped_to_voivodeships_and_aggregates_stay_visible(env):
    # backtest_metrics has city_id, not jurisdiction_code: the city is mapped through conf/cities.yaml
    assert sorted(r["city_id"] for r in env([("region", "PL-02")], table="backtest_metrics")) == ["ALL"]
    assert sorted(r["city_id"] for r in env([("region", "PL-12")], table="backtest_metrics")) == ["ALL", "krakow"]


def test_without_a_table_row_the_account_groups_decide(env):
    assert len(env([], groups={"smogcast-admins"})) == 3
    assert [r["city_id"] for r in env([], groups={"smogcast-region-PL-02"})] == ["wroclaw"]


def test_an_unknown_viewer_sees_nothing(env):
    assert env([]) == []
    assert env([], table="backtest_metrics") == []


def test_the_access_table_wins_over_the_groups(env):
    # the demo: the author is in smogcast-admins, but his own row says "region PL-12"
    assert [r["city_id"] for r in env([("region", "PL-12")], groups={"smogcast-admins"})] == ["krakow"]


def test_undocumented_columns_are_reported():
    entry = DICT["dq_summary"]
    assert gov.check_columns("dq_summary", entry, list(entry["columns"])) == []
    assert gov.check_columns("dq_summary", entry, [*entry["columns"], "new_col"]) == [
        "gold.dq_summary.new_col: column not in conf/gold_dictionary.yaml"]
    # a documented column the table does not have yet is fine (tables evolve forward)
    assert gov.check_columns("dq_summary", entry, ["source", "metric"]) == []


def test_every_dictionary_entry_has_a_class_and_a_description():
    for table, entry in DICT.items():
        assert entry["description"], table
        assert gov.check_columns(table, entry, list(entry["columns"])) == [], table


def test_editors_get_write_access_to_the_access_table_only():
    # The table belongs to the Jobs' principal; without the grant an admin cannot switch a role (found on DEV).
    stmts = gov.access_table_statements("c.ops.access_policy", [], ["smogcast-admins"])
    assert [s for s in stmts if s.startswith("GRANT")] == [
        "GRANT SELECT, MODIFY ON TABLE c.ops.access_policy TO `smogcast-admins`"]
    assert not any("GRANT" in s for s in gov.access_table_statements("c.ops.access_policy", []))
    assert load_config("dev")["governance"]["access_editors"] == [CFG["governance"]["groups"]["admin"]]


def test_view_and_comments_carry_the_descriptions():
    entry = DICT["forecast_tomorrow"]
    cols = list(entry["columns"])
    view = gov.secure_view(CFG, "c.gold_secure.forecast_tomorrow", "c.gold.forecast_tomorrow", "c.ops.access_policy",
                           entry, cols)
    assert view.startswith("CREATE OR REPLACE VIEW c.gold_secure.forecast_tomorrow (")
    assert "`calm_hours` COMMENT 'Forecast hours tomorrow with wind below 1.5 m/s" in view
    stmts = gov.comment_statements("c.gold.forecast_tomorrow", entry, cols)
    assert "ALTER TABLE c.gold.forecast_tomorrow ALTER COLUMN `run_id` SET TAGS ('class' = 'technical')" in stmts
    assert not any("—" in s for s in stmts)   # app texts use the en dash (author's rule A3)
