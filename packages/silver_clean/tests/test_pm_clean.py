"""PM cleaning: station→city mapping (old codes), hard rules → quarantine, frozen runs, spikes."""

import datetime as dt

from smogcast.core.config import load_config
from smogcast.silver_clean.pm_clean import flag_patterns, split_hard_rules
from smogcast.silver_clean.station_city import build_station_city, cities_frame

DQ = load_config("local")["pm_dq"]
T0 = dt.datetime(2024, 1, 1)
SCHEMA = "station_code STRING, pollutant STRING, time_utc TIMESTAMP, value DOUBLE"


def series(spark, values, start=T0, skip=()):
    rows = [("S1", "PM10", start + dt.timedelta(hours=i), float(v)) for i, v in enumerate(values) if i not in skip]
    return spark.createDataFrame(rows, SCHEMA)


def flags(spark, values, skip=()):
    out = flag_patterns(series(spark, values, skip=skip), DQ).orderBy("time_utc").collect()
    return [r["dq_flag"] for r in out]


def test_station_city_maps_current_and_old_codes(spark):
    stations = spark.createDataFrame(
        [("MpKrakAlKras", "Kraków", "OldKrak1, OldKrak2", 50.0, 19.9),
         ("PmGdaLeczk08", "Gdańsk", None, 54.3, 18.6),
         ("XxOther", "Katowice", "OldKat", 50.2, 19.0)],
        "station_code STRING, town STRING, old_station_codes STRING, lat DOUBLE, lon DOUBLE",
    )
    cfg = load_config("local")
    cities = cities_frame(spark, {k: cfg["cities"][k] for k in ("krakow", "gdansk")})
    m = {r["station_code"]: (r["city_id"], r["current_station_code"], r["code_kind"])
         for r in build_station_city(stations, cities).collect()}
    assert m["MpKrakAlKras"] == ("krakow", "MpKrakAlKras", "current")
    assert m["OldKrak2"] == ("krakow", "MpKrakAlKras", "old")   # old code → today's station
    assert "XxOther" not in m and "OldKat" not in m             # city not configured


def test_hard_rules_go_to_quarantine_with_reason(spark):
    df = spark.createDataFrame(
        [("S1", "PM10", T0, 10.0), ("S1", "PM10", T0, -3.0), ("S1", "PM10", T0, 1500.0), ("S1", "PM10", T0, None)],
        SCHEMA,
    )
    ok, bad = split_hard_rules(df, DQ)
    assert ok.count() == 1
    assert sorted(r["quarantine_reason"] for r in bad.collect()) == ["above_max", "below_min", "missing_value"]


def test_frozen_run_flags_whole_run_only_when_long_enough(spark):
    assert flags(spark, [5, 7, 7, 7, 7, 7, 7, 9]) == ["ok"] + ["frozen"] * 6 + ["ok"]
    assert flags(spark, [5, 7, 7, 7, 7, 7, 9]) == ["ok"] * 7          # 5 repeats < 6
    # a missing hour splits the run: 3 + 3 identical values are not frozen
    assert "frozen" not in flags(spark, [7, 7, 7, 7, 7, 7, 7], skip=(3,))


def test_single_hour_spike_is_flagged_but_smog_episode_is_not(spark):
    assert flags(spark, [20, 22, 400, 21, 20]) == ["ok", "ok", "spike", "ok", "ok"]
    # a real episode rises over hours — neighbours are high too
    assert "spike" not in flags(spark, [20, 60, 150, 300, 400, 380, 250])
    # big relative jump but at clean-air level (below min_ug_m3) is not a spike
    assert "spike" not in flags(spark, [5, 5, 60, 5, 5])
