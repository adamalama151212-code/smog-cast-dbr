"""gold.measured_daily and gold.forecast_verification (E9 point 6): archive + live days, official forecasts vs facts."""

import datetime as dt

import pytest

from smogcast.gold import measured
from smogcast.gold.steps import step_governance, step_measured_daily

D = dt.date
CITY = ("city_id STRING, pollutant STRING, day_cet DATE, max_station_mean_ug_m3 DOUBLE, "
        "mean_station_mean_ug_m3 DOUBLE, n_stations BIGINT, limit_ug_m3 DOUBLE, exceeded BOOLEAN")
INFO = "city_id STRING, city_name STRING, jurisdiction_code STRING"
HISTORY = ("city_id STRING, city_name STRING, jurisdiction_code STRING, pollutant STRING, issue_day DATE, "
           "target_day DATE, probability DOUBLE, warned BOOLEAN, limit_ug_m3 DOUBLE, status STRING, run_kind STRING, "
           "issued_at TIMESTAMP")


def day(city, d, worst, limit=50.0):
    return (city, "PM10", d, worst, None if worst is None else worst - 5, None if worst is None else 3, limit,
            None if worst is None else worst > limit)


@pytest.fixture
def city_info(spark):
    return spark.createDataFrame([("krakow", "Kraków", "PL-12"), ("wroclaw", "Wrocław", "PL-02")], INFO)


def test_archive_and_live_in_one_table_archive_wins_on_overlap(spark, city_info):
    archive = spark.createDataFrame([day("krakow", D(2025, 1, 20), 109.0), day("krakow", D(2025, 6, 1), 20.0)], CITY)
    live = spark.createDataFrame([
        day("krakow", D(2025, 1, 20), 999.0),     # same day as the archive -> the verified archive value stays
        day("krakow", D(2026, 10, 7), 55.0),
        day("wroclaw", D(2026, 10, 7), None),     # morning-only row: no valid full day -> no row
        day("gdansk", D(2026, 10, 7), 30.0),      # not a configured city here -> no row
    ], CITY)
    rows = {(r["city_id"], r["day_cet"]): r for r in measured.build_measured_daily(archive, live, city_info).collect()}
    assert set(rows) == {("krakow", D(2025, 1, 20)), ("krakow", D(2025, 6, 1)), ("krakow", D(2026, 10, 7))}
    jan = rows[("krakow", D(2025, 1, 20))]
    assert (jan["source"], jan["max_station_mean_ug_m3"], jan["exceeded"]) == ("archive", 109.0, True)
    oct_ = rows[("krakow", D(2026, 10, 7))]
    assert (oct_["source"], oct_["city_name"], oct_["jurisdiction_code"]) == ("live", "Kraków", "PL-12")
    assert (jan["heating_season"], rows[("krakow", D(2025, 6, 1))]["heating_season"]) == ("2024/25", None)


@pytest.mark.parametrize("d, season", [(D(2024, 10, 1), "2024/25"), (D(2025, 4, 30), "2024/25"), (D(2025, 5, 1), None),
                                       (D(2025, 9, 30), None), (D(2099, 12, 31), "2099/00")])
def test_heating_season_matches_the_model_feature_months(spark, d, season):
    from pyspark.sql import functions as F

    df = spark.createDataFrame([(d,)], "d DATE")
    assert df.select(measured.heating_season(F.col("d")).alias("s")).first()["s"] == season


def test_verification_judges_only_official_forecasts(spark, city_info):
    today = D(2026, 10, 10)
    t = dt.datetime(2026, 10, 8, 10, 42)

    def fc(city, target, p, kind="official", status="ok"):
        return (city, city.title(), "PL-12", "PM10", target - dt.timedelta(days=1), target, p,
                None if p is None else p >= 0.5, 50.0, status, kind, t)

    history = spark.createDataFrame([
        fc("krakow", D(2026, 10, 5), 0.8),                       # warned, exceeded -> hit
        fc("krakow", D(2026, 10, 5), 0.1, kind="update"),        # not official -> not judged
        fc("krakow", D(2026, 10, 6), 0.2),                       # not warned, exceeded -> miss
        fc("krakow", D(2026, 10, 7), 0.6),                       # warned, clean -> false alarm
        fc("krakow", D(2026, 10, 8), 0.1),                       # quiet, clean -> correct
        fc("wroclaw", D(2026, 10, 8), None, status="no_forecast"),
        fc("krakow", D(2026, 10, 9), 0.3),                       # day over, no valid measurement -> no_data
        fc("krakow", D(2026, 10, 10), 0.3),                      # today: not over yet -> pending
    ], HISTORY)
    m = measured.build_measured_daily(spark.createDataFrame([], CITY), spark.createDataFrame(
        [day("krakow", D(2026, 10, d), v) for d, v in ((5, 80.0), (6, 60.0), (7, 30.0), (8, 20.0))]
        + [day("wroclaw", D(2026, 10, 8), 40.0)], CITY), city_info)
    rows = {(r["city_id"], r["target_day"]): r for r in measured.build_verification(history, m, today).collect()}
    assert len(rows) == 7
    got = {k: r["outcome"] for k, r in rows.items()}
    assert got == {("krakow", D(2026, 10, 5)): "hit", ("krakow", D(2026, 10, 6)): "miss",
                   ("krakow", D(2026, 10, 7)): "false_alarm", ("krakow", D(2026, 10, 8)): "correct_no_warning",
                   ("wroclaw", D(2026, 10, 8)): "no_forecast", ("krakow", D(2026, 10, 9)): "no_data",
                   ("krakow", D(2026, 10, 10)): "pending"}
    hit = rows[("krakow", D(2026, 10, 5))]
    assert hit["probability"] == 0.8 and hit["squared_error"] == pytest.approx(0.04)
    assert hit["measured_max_ug_m3"] == 80.0 and hit["measured_source"] == "live"
    assert rows[("krakow", D(2026, 10, 9))]["squared_error"] is None


def test_step_writes_both_tables_and_the_dictionary_covers_them(ctx, monkeypatch):
    monkeypatch.setenv("SMOGCAST_ISSUE_DATE", "2026-10-09")
    st, spark = ctx.storage, ctx.spark
    st.overwrite(spark.createDataFrame([day("krakow", D(2025, 1, 20), 109.0)], CITY), "silver", "pm_daily_city")
    st.overwrite(spark.createDataFrame([day("krakow", D(2026, 10, 8), 20.0)], CITY), "silver", "pm_daily_city_live")
    step_measured_daily(ctx)                                     # no forecast history yet: verification skipped
    assert st.read("gold", "measured_daily").count() == 2 and not st.exists("gold", "forecast_verification")

    st.overwrite(spark.createDataFrame([("krakow", "Kraków", "PL-12", "PM10", D(2026, 10, 7), D(2026, 10, 8), 0.1,
                                         False, 50.0, "ok", "official", dt.datetime(2026, 10, 7, 10, 42))], HISTORY),
                 "gold", "forecast_history")
    step_measured_daily(ctx)
    (v,) = st.read("gold", "forecast_verification").collect()
    assert v["outcome"] == "correct_no_warning"
    step_governance(ctx)   # every column of the new tables is in conf/gold_dictionary.yaml (else it raises)
