"""Open-Meteo ingest: data contract (m/s, UTC, array lengths) and caching rules."""

import copy
import datetime as dt
import json

import pytest

from smogcast.core.config import load_config
from smogcast.ingest import weather
from smogcast.ingest.weather import WeatherValidationError, build_params, landing_file, validate_payload

VARS = ["temperature_2m", "wind_speed_10m"]


def payload(suffix="", n=3):
    return {
        "latitude": 50.06, "longitude": 19.94, "utc_offset_seconds": 0,
        "hourly_units": {"time": "iso8601", "temperature_2m" + suffix: "°C", "wind_speed_10m" + suffix: "m/s"},
        "hourly": {"time": [f"2024-01-01T0{i}:00" for i in range(n)],
                   "temperature_2m" + suffix: [1.0] * n, "wind_speed_10m" + suffix: [2.0] * n},
    }


def test_request_forces_ms_utc_and_suffix():
    p = build_params(50.0, 19.9, "2024-01-01", "2024-12-31", VARS, "_previous_day1")
    assert p["wind_speed_unit"] == "ms" and p["timezone"] == "GMT"
    assert p["hourly"] == "temperature_2m_previous_day1,wind_speed_10m_previous_day1"


def test_valid_payload_passes():
    validate_payload(payload("_previous_day1"), VARS, "_previous_day1")


@pytest.mark.parametrize("breakage, message", [
    (lambda p: p["hourly_units"].update({"wind_speed_10m": "km/h"}), "m/s"),
    (lambda p: p.update({"utc_offset_seconds": 3600}), "UTC"),
    (lambda p: p["hourly"]["wind_speed_10m"].pop(), "length"),
    (lambda p: p["hourly"].update({"time": []}), "Empty"),
])
def test_broken_payload_is_rejected(breakage, message):
    bad = copy.deepcopy(payload())
    breakage(bad)
    with pytest.raises(WeatherValidationError, match=message):
        validate_payload(bad, VARS, "")


def test_download_caches_past_years_and_refreshes_current(tmp_path, monkeypatch):
    cfg = load_config("local", {"run": {"cities": ["krakow"], "archive_years": [2024, 2025]},
                                "weather": {"hourly_variables": VARS}})
    calls = []

    def fake_fetch(url, params):
        calls.append((url, params["start_date"], params["end_date"]))
        suffix = "_previous_day1" if "previous-runs" in url else ""
        return payload(suffix)

    monkeypatch.setattr(weather, "fetch", fake_fetch)
    today = dt.date(2025, 6, 15)
    weather.download_history(cfg, str(tmp_path), today=today)
    # short_range: 2024, 2025; day_ahead: 2024, 2025 (first_year 2024) → 4 requests
    assert len(calls) == 4
    assert ("2025-01-01", "2025-06-14") in {(c[1], c[2]) for c in calls}  # current year only up to yesterday
    stored = json.loads(landing_file(tmp_path, "day_ahead", "krakow", 2024).read_text())
    assert stored["variable_suffix"] == "_previous_day1" and stored["requested_lat"] == 50.0647

    calls.clear()
    weather.download_history(cfg, str(tmp_path), today=today)
    assert sorted(c[1] for c in calls) == ["2025-01-01", "2025-01-01"]  # only the growing current year again


class FakeResponse:
    def __init__(self, status, body):
        self.status_code, self.text = status, body

    def raise_for_status(self):
        pass

    def json(self):
        return json.loads(self.text)


def test_fetch_retries_a_body_that_is_not_json(monkeypatch):
    # Regression (Databricks, 2026-10-06): a 200 with a non-JSON body failed the whole archive download.
    answers = [FakeResponse(200, "<html>busy</html>"), FakeResponse(200, json.dumps({"ok": 1}))]
    monkeypatch.setattr(weather.requests, "get", lambda url, params, timeout: answers.pop(0))
    assert weather.fetch("https://x", {}, delay=0) == {"ok": 1} and not answers


def test_fetch_gives_up_after_the_last_attempt(monkeypatch):
    monkeypatch.setattr(weather.requests, "get", lambda url, params, timeout: FakeResponse(200, ""))
    with pytest.raises(weather.NotJsonResponse, match="not JSON"):
        weather.fetch("https://x", {}, retries=2, delay=0)
