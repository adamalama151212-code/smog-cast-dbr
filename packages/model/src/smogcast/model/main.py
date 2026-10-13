"""Entry point of the smogcast-model wheel: ``smogcast-model --step <name> --env <env>``.

Each step is a function ``(Context) -> None``; one Lakeflow Job task runs one step.
"""

from __future__ import annotations

from smogcast.core.runner import make_main, run
from smogcast.model.steps import step_backtest, step_forecast, step_train

STEPS = {
    "train": step_train,
    "backtest": step_backtest,
    "forecast": step_forecast,
}

main = make_main("model", STEPS, "Forecast model: training, backtest, tomorrow's forecast")

if __name__ == "__main__":
    run(main)
