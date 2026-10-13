"""Entry point of the smogcast-silver-clean wheel: ``smogcast-silver-clean --step <name> --env <env>``.

Each step is a function ``(Context) -> None``; one Lakeflow Job task runs one step.
"""

from __future__ import annotations

from smogcast.core.runner import make_main, run
from smogcast.silver_clean.steps import (
    step_fault_reset,
    step_live_reset,
    step_pm_hourly,
    step_pm_stream,
    step_stations_scd2,
)

STEPS = {
    "pm-hourly": step_pm_hourly,
    "pm-stream": step_pm_stream,
    "fault-reset": step_fault_reset,
    "live-reset": step_live_reset,
    "stations-scd2": step_stations_scd2,
}

main = make_main("silver-clean", STEPS, "Data-quality cleaning of PM measurements")

if __name__ == "__main__":
    run(main)
