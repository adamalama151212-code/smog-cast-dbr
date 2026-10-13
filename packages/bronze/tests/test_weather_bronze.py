"""Weather landing JSON → bronze.weather_forecast: both archives share one schema."""

import datetime as dt
import json
from pathlib import Path

from smogcast.bronze.steps import step_weather_forecast


def write_landing(ctx, source: str, suffix: str, values: list) -> None:
    variables = ctx.cfg["weather"]["hourly_variables"]
    n = len(values)
    hourly = {"time": [f"2024-01-19T{10 + i:02d}:00" for i in range(n)]}
    hourly.update({v + suffix: values for v in variables})
    record = {
        "city_id": "krakow", "source": source, "variable_suffix": suffix,
        "requested_lat": 50.0647, "requested_lon": 19.945, "fetched_at": "2026-10-01T00:00:00+00:00",
        "payload": {"latitude": 50.06, "longitude": 19.94, "utc_offset_seconds": 0, "hourly": hourly},
    }
    path = Path(ctx.storage.landing("weather", source, "krakow", "2024.json"))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record))


def test_both_sources_land_in_one_table(ctx):
    write_landing(ctx, "short_range", "", [1.0, 2.0, 3.0])
    # day_ahead archive starts 2024-01-19 12:00 UTC: earlier hours are null
    write_landing(ctx, "day_ahead", "_previous_day1", [None, None, 3.5])
    step_weather_forecast(ctx)
    step_weather_forecast(ctx)  # idempotent full rebuild

    df = ctx.storage.read("bronze", "weather_forecast")
    assert df.count() == 6
    assert "temperature_2m" in df.columns and not any(c.endswith("_previous_day1") for c in df.columns)
    da = {r["time_utc"]: r["temperature_2m"] for r in df.where("source = 'day_ahead'").collect()}
    assert da[dt.datetime(2024, 1, 19, 10)] is None and da[dt.datetime(2024, 1, 19, 12)] == 3.5
