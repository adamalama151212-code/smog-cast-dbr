"""Daily means (18 h rule, morning window, city = max station) and leakage-free features."""

import datetime as dt

from smogcast.core.config import load_config
from smogcast.core.schema import PM_FEATURES, WEATHER_FEATURES
from smogcast.silver_transform.features import build_features
from smogcast.silver_transform.pm_daily import build_city_daily, build_station_daily

CFG = load_config("local")
LIMITS = CFG["thresholds"]["daily_limit_ug_m3"]
HOURLY = ("station_code STRING, city_id STRING, pollutant STRING, day_cet DATE, hour_cet INT, "
          "value DOUBLE, is_valid BOOLEAN")


def day_rows(station, day, value, hours=range(24), valid=True):
    return [(station, "krakow", "PM10", day, h, float(value), valid) for h in hours]


def test_station_day_validity_and_morning_window(spark):
    d = dt.date(2024, 1, 10)
    rows = (day_rows("A", d, 40, hours=range(18))            # 18 h -> valid
            + day_rows("B", d, 90, hours=range(17))          # 17 h -> invalid
            + day_rows("C", d, 30, hours=range(11, 24)))     # no morning hours
    st = {r["station_code"]: r for r in build_station_daily(spark.createDataFrame(rows, HOURLY), 18, 10, 8).collect()}
    assert st["A"]["is_valid"] and not st["B"]["is_valid"]
    assert st["A"]["n_morning_hours"] == 11 and st["A"]["is_morning_valid"]
    assert not st["C"]["is_morning_valid"]


def test_city_exceedance_uses_the_worst_valid_station(spark):
    d = dt.date(2024, 1, 10)
    rows = day_rows("A", d, 40) + day_rows("B", d, 60) + day_rows("C", d, 500, hours=range(5))  # C invalid
    station = build_station_daily(spark.createDataFrame(rows, HOURLY), 18, 10, 8)
    city = build_city_daily(station, LIMITS).first()
    assert city["n_stations"] == 2 and city["max_station_mean_ug_m3"] == 60.0 and city["exceeded"]


CITY = ("city_id STRING, pollutant STRING, day_cet DATE, max_station_mean_ug_m3 DOUBLE, mean_station_mean_ug_m3 DOUBLE, "
        "exceeded BOOLEAN, morning_max_station_ug_m3 DOUBLE, morning_mean_ug_m3 DOUBLE")


def city_days(start, maxes):
    return [("krakow", "PM10", start + dt.timedelta(days=i), float(m), float(m) * 0.8, m > 50, float(m) + 1, float(m))
            for i, m in enumerate(maxes)]


def weather(spark, days, source):
    cols = ", ".join(f"{c} DOUBLE" for c in WEATHER_FEATURES)
    rows = [("krakow", source, d, True, *[1.0] * len(WEATHER_FEATURES)) for d in days]
    return spark.createDataFrame(rows, f"city_id STRING, source STRING, day_cet DATE, is_complete BOOLEAN, {cols}")


def features(spark, maxes, start=dt.date(2024, 3, 1)):
    days = [start + dt.timedelta(days=i) for i in range(len(maxes) + 1)]
    w = weather(spark, days, "day_ahead").unionByName(weather(spark, days, "short_range"))
    f = build_features(spark.createDataFrame(city_days(start, maxes), CITY), w, CFG["model"])
    return {r["issue_day"]: r for r in f.collect()}


def test_features_align_d_minus_1_d_and_label_d_plus_1(spark):
    f = features(spark, [10, 20, 80, 30])  # 1–4 March 2024
    r = f[dt.date(2024, 3, 2)]  # issue day D = 2 March, predicting 3 March
    assert r["pm_d1_max_ug_m3"] == 10.0                   # D-1 = 1 March
    assert r["pm_d_morning_max_ug_m3"] == 21.0            # D morning = 2 March
    assert r["exceeded_next_day"] is True and r["next_day_max_ug_m3"] == 80.0  # D+1 = 3 March
    assert r["target_day"] == dt.date(2024, 3, 3) and r["split"] == "validation"
    assert r["weather_source"] == "day_ahead" and r["is_usable"]
    assert not f[dt.date(2024, 3, 1)]["is_usable"]       # no D-1 available


def test_training_rows_use_short_range_weather(spark):
    r = features(spark, [10, 20, 30], start=dt.date(2022, 3, 1))[dt.date(2022, 3, 2)]
    assert r["split"] == "train" and r["weather_source"] == "short_range" and r["is_usable"]


def test_no_leakage_changing_tomorrow_changes_only_the_label(spark):
    a = features(spark, [10, 20, 30, 40])
    b = features(spark, [10, 20, 300, 40])  # only 3 March differs
    issue = dt.date(2024, 3, 2)              # predicting 3 March
    for col in PM_FEATURES + WEATHER_FEATURES:
        assert a[issue][col] == b[issue][col], col
    assert a[issue]["exceeded_next_day"] != b[issue]["exceeded_next_day"]


STREAM = ("source STRING, station_code STRING, city_id STRING, pollutant STRING, day_cet DATE, hour_cet INT, "
          "value DOUBLE, is_valid BOOLEAN")


def test_features_live_one_row_per_city_and_pollutant(ctx, monkeypatch):
    from smogcast.silver_transform.steps import step_features_live

    d = dt.date(2026, 1, 20)
    monkeypatch.setenv("SMOGCAST_ISSUE_DATE", d.isoformat())
    yday = d - dt.timedelta(days=1)
    rows = ([("live", "K1", "krakow", "PM10", yday, h, 60.0, True) for h in range(24)]
            + [("live", "K1", "krakow", "PM10", d, h, 40.0, True) for h in range(11)]
            + [("fault_replay", "K9", "krakow", "PM10", yday, h, 500.0, True) for h in range(24)]  # not live
            + [("live", "G1", "gdansk", "PM10", yday, h, 20.0, True) for h in range(24)])
    ctx.storage.overwrite(ctx.spark.createDataFrame(rows, STREAM), "silver", "pm_stream")
    tomorrow = [d + dt.timedelta(days=1)]
    w = weather(ctx.spark, tomorrow, "live").unionByName(weather(ctx.spark, tomorrow, "day_ahead"))
    w = w.where("source = 'day_ahead' OR city_id = 'krakow'")   # gdansk: no live forecast
    ctx.storage.overwrite(w, "silver", "weather_daily")

    step_features_live(ctx)
    step_features_live(ctx)                                    # MERGE: a re-run adds no rows
    f = {(r["city_id"], r["pollutant"]): r for r in ctx.storage.read("silver", "features_live").collect()}
    assert len(f) == 4                                         # 2 test cities x 2 pollutants
    k = f[("krakow", "PM10")]
    assert k["is_usable"] and k["unusable_reason"] is None and k["split"] == "live"
    assert k["weather_source"] == "live" and k["exceeded_next_day"] is None
    assert k["pm_d1_max_ug_m3"] == 60.0 and k["pm_d1_exceeded"] and k["pm_d_morning_max_ug_m3"] == 40.0
    assert f[("krakow", "PM2.5")]["unusable_reason"] == "no_valid_pm_yesterday"
    assert f[("gdansk", "PM10")]["unusable_reason"] == "no_complete_weather_forecast"


def test_live_daily_means_cover_only_finished_days_of_the_live_source(ctx, monkeypatch):
    from smogcast.silver_transform.steps import step_pm_daily_live

    monkeypatch.setenv("SMOGCAST_ISSUE_DATE", "2026-10-09")   # today in CET
    y, t = dt.date(2026, 10, 8), dt.date(2026, 10, 9)
    rows = ([("live", *r) for r in day_rows("A", y, 60)]           # yesterday, full day -> exceedance
            + [("live", *r) for r in day_rows("A", t, 90, hours=range(20))]   # today: not over -> left out
            + [("fault_replay", *r) for r in day_rows("B", y, 900)])          # demo faults never count
    ctx.storage.overwrite(ctx.spark.createDataFrame(rows, f"source STRING, {HOURLY}"), "silver", "pm_stream")
    step_pm_daily_live(ctx)
    (r,) = ctx.storage.read("silver", "pm_daily_city_live").collect()
    assert (r["day_cet"], r["n_stations"], r["max_station_mean_ug_m3"], r["exceeded"]) == (y, 1, 60.0, True)
