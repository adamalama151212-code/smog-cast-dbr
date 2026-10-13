"""Entry point of the smogcast-bronze wheel: ``smogcast-bronze --step <name> --env <env>``.

Each step is a function ``(Context) -> None``; one Lakeflow Job task runs one step.
"""

from __future__ import annotations

from smogcast.bronze.steps import step_pm_hourly, step_station_meta, step_station_snapshots, step_weather_forecast
from smogcast.core.runner import make_main, run

STEPS = {
    "pm-hourly": step_pm_hourly,
    "station-meta": step_station_meta,
    "weather-forecast": step_weather_forecast,
    "station-snapshots": step_station_snapshots,
}

main = make_main("bronze", STEPS, "Landing files into bronze Delta tables")

if __name__ == "__main__":
    run(main)
