"""Uniform command-line entry point for every step wheel.

Each step package (bronze, silver_clean, …) exposes a dict ``STEPS`` mapping a step name
to a function ``(Context) -> None`` and builds its console script with ``make_main``.
A Databricks Job task then runs exactly one step of one wheel:

    python_wheel_task:
      package_name: smogcast_bronze
      entry_point: smogcast-bronze
      parameters: ["--step", "pm-hourly", "--env", "dev"]

The same function is called by the local ``smogcast`` CLI and by tests, so there is
one code path for all three.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING

from smogcast.core.config import ENVIRONMENTS

if TYPE_CHECKING:  # Context pulls in pyspark/delta; keep `--help` fast
    from smogcast.core.context import Context

StepFn = Callable[["Context"], None]


def configure_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # py4j logs every JVM call at INFO level — silence it.
    logging.getLogger("py4j").setLevel(logging.WARNING)


def make_main(package: str, steps: dict[str, StepFn], description: str = "") -> Callable[[list[str] | None], int]:
    def main(argv: list[str] | None = None) -> int:
        parser = argparse.ArgumentParser(prog=f"smogcast-{package}", description=description)
        parser.add_argument("--step", required=True, choices=sorted(steps), help="step to run")
        parser.add_argument("--env", default=None, choices=ENVIRONMENTS,
                            help="configuration environment (default: SMOGCAST_ENV or local)")
        parser.add_argument("--offline", action="store_true",
                            help="synthetic inputs, separate data root (no internet; results are NOT real)")
        parser.add_argument("--run-id", default=None,
                            help="id of the orchestrator run (Databricks Job: {{job.run_id}}); a task's retries share it")
        parser.add_argument("-v", "--verbose", action="store_true")
        args = parser.parse_args(argv)
        configure_logging(args.verbose)

        # Spark starts only after argument parsing, so `--help` and typos fail instantly.
        from smogcast.core.context import build_context

        ctx = build_context(args.env, offline=args.offline, run_id=args.run_id)
        logging.getLogger("smogcast").info("===== %s: %s =====", package, args.step)
        steps[args.step](ctx)
        return 0

    return main


def run(main: Callable[[list[str] | None], int]) -> None:
    sys.exit(main(None))
