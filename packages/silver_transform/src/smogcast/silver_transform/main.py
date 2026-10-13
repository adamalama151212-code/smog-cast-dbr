"""Entry point of the smogcast-silver-transform wheel: ``smogcast-silver-transform --step <name> --env <env>``.

Each step is a function ``(Context) -> None``; one Lakeflow Job task runs one step.
"""

from __future__ import annotations

from smogcast.core.runner import make_main, run
from smogcast.silver_transform.steps import (
    step_features,
    step_features_live,
    step_pm_daily,
    step_pm_daily_live,
    step_weather_daily,
)

STEPS = {
    "pm-daily": step_pm_daily,
    "weather-daily": step_weather_daily,
    "features": step_features,
    "features-live": step_features_live,
    "pm-daily-live": step_pm_daily_live,
}

main = make_main("silver-transform", STEPS, "Daily aggregates and model features")

if __name__ == "__main__":
    run(main)
