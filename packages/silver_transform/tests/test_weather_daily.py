"""Daily weather aggregates: CET day boundaries, wind direction averaging, calm hours."""

import datetime as dt

import pytest

from smogcast.silver_transform.weather_daily import build_weather_daily

SCHEMA = ("city_id STRING, source STRING, time_utc TIMESTAMP, temperature_2m DOUBLE, relative_humidity_2m DOUBLE, "
          "wind_speed_10m DOUBLE, wind_direction_10m DOUBLE, precipitation DOUBLE, surface_pressure DOUBLE, "
          "cloud_cover DOUBLE, shortwave_radiation DOUBLE")


def row(t, temp=0.0, ws=1.0, wd=0.0, p=0.0):
    return ("krakow", "short_range", t, temp, 80.0, ws, wd, p, 1000.0, 50.0, 0.0)


def test_cet_day_boundaries(spark):
    rows = [
        row(dt.datetime(2024, 1, 1, 22)),   # 23:00 CET on 1 Jan
        row(dt.datetime(2024, 1, 1, 23)),   # 00:00 CET on 2 Jan -> next CET day
    ]
    days = {r["day_cet"]: r["n_hours"] for r in build_weather_daily(spark.createDataFrame(rows, SCHEMA)).collect()}
    assert days == {dt.date(2024, 1, 1): 1, dt.date(2024, 1, 2): 1}


def test_aggregates_for_one_day(spark):
    base = dt.datetime(2024, 1, 1, 23)  # 00:00 CET 2 Jan
    rows = [row(base + dt.timedelta(hours=i), temp=float(i), ws=1.0 if i < 12 else 4.0,
                wd=350.0 if i % 2 else 10.0, p=0.5 if i < 2 else 0.0) for i in range(24)]
    d = build_weather_daily(spark.createDataFrame(rows, SCHEMA)).first()
    assert d["n_hours"] == 24 and d["is_complete"]
    assert (d["t_min_c"], d["t_max_c"], d["t_range_c"]) == (0.0, 23.0, 23.0)
    assert d["calm_hours"] == 12                       # wind < 1.5 m/s in the first 12 hours
    assert d["precip_sum_mm"] == pytest.approx(1.0) and d["precip_hours"] == 2
    # 350° and 10° average to north, not to 180° (vector mean)
    assert min(d["wind_from_deg_mean"], 360 - d["wind_from_deg_mean"]) < 1.0


def test_null_hours_are_ignored_and_day_flagged_incomplete(spark):
    rows = [row(dt.datetime(2024, 1, 1, 23) + dt.timedelta(hours=i)) for i in range(10)]
    rows.append(("krakow", "short_range", dt.datetime(2024, 1, 2, 12), None, None, None, None, None, None, None, None))
    d = build_weather_daily(spark.createDataFrame(rows, SCHEMA)).first()
    assert d["n_hours"] == 10 and not d["is_complete"]
