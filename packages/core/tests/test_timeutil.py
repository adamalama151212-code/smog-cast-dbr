"""Time conventions (decision D13): archive end-of-hour CET → start-of-hour UTC; day in CET."""

import datetime as dt

import pytest
from pyspark.sql import functions as F

from smogcast.core.timeutil import (
    cet_hour_end_to_utc_start,
    day_cet_col,
    issue_day_cet,
    local_hour_end_to_utc_start,
    local_hour_end_to_utc_start_col,
    now_cet,
    round_to_hour,
)


@pytest.mark.parametrize(
    "raw, rounded",
    [
        (dt.datetime(2025, 1, 1, 3, 0, 0, 5000), dt.datetime(2025, 1, 1, 3)),        # +5 ms noise
        (dt.datetime(2025, 12, 31, 23, 59, 16), dt.datetime(2026, 1, 1, 0)),         # noise just below the hour
        (dt.datetime(2025, 6, 1, 12, 0, 43), dt.datetime(2025, 6, 1, 12)),           # max noise seen (~44 s)
    ],
)
def test_round_to_nearest_hour(raw, rounded):
    assert round_to_hour(raw) == rounded


def test_first_hour_of_year_starts_on_new_years_eve_utc():
    # "2025-01-01 01:00 CET" = value for 00:00–01:00 CET = 23:00–24:00 UTC on 31 Dec.
    assert cet_hour_end_to_utc_start(dt.datetime(2025, 1, 1, 1)) == dt.datetime(2024, 12, 31, 23)


def test_summer_is_still_cet_not_cest():
    # The archive uses CET all year: no daylight-saving shift in July.
    assert cet_hour_end_to_utc_start(dt.datetime(2025, 7, 1, 13)) == dt.datetime(2025, 7, 1, 11)


def test_day_cet_of_utc_hours(spark):
    rows = [(dt.datetime(2024, 12, 31, 23),), (dt.datetime(2025, 1, 1, 22),), (dt.datetime(2025, 1, 1, 23),)]
    df = spark.createDataFrame(rows, "time_utc TIMESTAMP").withColumn("day", day_cet_col(F.col("time_utc")))
    days = [r["day"] for r in df.orderBy("time_utc").collect()]
    assert days == [dt.date(2025, 1, 1), dt.date(2025, 1, 1), dt.date(2025, 1, 2)]


def test_api_local_time_handles_daylight_saving(spark):
    # winter: local = CET (UTC+1); summer: local = CEST (UTC+2)
    assert local_hour_end_to_utc_start(dt.datetime(2026, 1, 15, 13)) == dt.datetime(2026, 1, 15, 11)
    assert local_hour_end_to_utc_start(dt.datetime(2026, 7, 1, 13)) == dt.datetime(2026, 7, 1, 10)
    df = spark.createDataFrame([(dt.datetime(2026, 7, 1, 13),), (dt.datetime(2026, 1, 15, 13),)], "t TIMESTAMP")
    got = [r["u"] for r in df.select(local_hour_end_to_utc_start_col(F.col("t")).alias("u")).collect()]
    assert got == [dt.datetime(2026, 7, 1, 10), dt.datetime(2026, 1, 15, 11)]


def test_issue_day_is_the_cet_day_unless_overridden(monkeypatch):
    monkeypatch.delenv("SMOGCAST_ISSUE_DATE", raising=False)
    assert issue_day_cet(dt.datetime(2026, 1, 9, 22, 59)) == dt.date(2026, 1, 9)
    assert issue_day_cet(dt.datetime(2026, 1, 9, 23, 0)) == dt.date(2026, 1, 10)   # 00:00 CET
    monkeypatch.setenv("SMOGCAST_ISSUE_DATE", "2025-01-19")
    assert issue_day_cet(dt.datetime(2026, 1, 9, 12, 0)) == dt.date(2025, 1, 19)


def test_now_cet_is_utc_plus_one_all_year():
    # summer (CEST in Poland) and winter alike: the project's clock is CET, never daylight saving time
    assert now_cet(dt.datetime(2026, 7, 1, 10, 30)) == dt.datetime(2026, 7, 1, 11, 30)
    assert now_cet(dt.datetime(2026, 12, 31, 23, 30)) == dt.datetime(2027, 1, 1, 0, 30)
