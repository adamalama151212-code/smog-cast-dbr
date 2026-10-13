"""``smogcast`` — local command line over all step wheels.

Examples:
    smogcast steps                                # list every step of every wheel
    smogcast run bronze pm-hourly                 # one step (same as `smogcast-bronze --step pm-hourly`)
    smogcast --offline run-batch                  # history → model, synthetic inputs
    smogcast show gold forecast_tomorrow -n 20    # peek at a table

The ordered step lists below mirror the task graph of the Lakeflow Job in
resources/job_smogcast.yml (batch) and resources/job_smogcast_live.yml (live) — keep them in sync.
"""

from __future__ import annotations

import argparse
import logging

from smogcast.bronze.main import STEPS as BRONZE
from smogcast.core.config import ENVIRONMENTS
from smogcast.core.runner import configure_logging, run
from smogcast.gold.main import STEPS as GOLD
from smogcast.ingest.main import STEPS as INGEST
from smogcast.model.main import STEPS as MODEL
from smogcast.silver_clean.main import STEPS as SILVER_CLEAN
from smogcast.silver_transform.main import STEPS as SILVER_TRANSFORM

PACKAGES = {
    "ingest": INGEST,
    "bronze": BRONZE,
    "silver-clean": SILVER_CLEAN,
    "silver-transform": SILVER_TRANSFORM,
    "model": MODEL,
    "gold": GOLD,
}

# Path A — history and model (batch).
BATCH_ORDER = [
    ("ingest", "gios-archive"),
    ("ingest", "weather-forecast-history"),
    ("bronze", "station-meta"),
    ("bronze", "pm-hourly"),
    ("bronze", "weather-forecast"),
    ("silver-clean", "pm-hourly"),
    ("silver-transform", "pm-daily"),
    ("silver-transform", "weather-daily"),
    ("silver-transform", "features"),
    ("model", "train"),
    ("model", "backtest"),
    ("gold", "measured-daily"),
    ("gold", "governance"),
]

# Path B/C — live data, station registry (CDC) and tomorrow's forecast (needs a trained model
# from run-batch). The batch weather steps re-run here because the live forecast joins
# bronze/silver weather tables shared with the history.
LIVE_ORDER = [
    ("ingest", "gios-registry"),
    ("bronze", "station-snapshots"),
    ("silver-clean", "stations-scd2"),
    ("ingest", "gios-live"),
    ("silver-clean", "pm-stream"),
    ("ingest", "weather-forecast-live"),
    ("bronze", "weather-forecast"),
    ("silver-transform", "weather-daily"),
    ("silver-transform", "features-live"),
    ("model", "forecast"),
    ("gold", "dq-summary"),
    ("silver-transform", "pm-daily-live"),
    ("gold", "measured-daily"),
    ("gold", "governance"),
]

# Data-quality demo: real measurements replayed with injected faults, then cleaned. Starts with
# a reset, so every run replays the same faults from scratch (the live source is untouched).
FAULT_DEMO_ORDER = [
    ("silver-clean", "fault-reset"),
    ("ingest", "fault-replay"),
    ("bronze", "station-snapshots"),
    ("silver-clean", "stations-scd2"),
    ("silver-clean", "pm-stream"),
    ("gold", "dq-summary"),
]

ORDERS = {"run-batch": BATCH_ORDER, "run-live": LIVE_ORDER, "run-fault-demo": FAULT_DEMO_ORDER}


def _check_orders() -> None:
    for name, order in ORDERS.items():
        for pkg, step in order:
            if step not in PACKAGES[pkg]:
                raise RuntimeError(f"{name}: unknown step {pkg}/{step}")


def main(argv: list[str] | None = None) -> int:
    _check_orders()
    parser = argparse.ArgumentParser(prog="smogcast", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", default=None, choices=ENVIRONMENTS)
    parser.add_argument("--offline", action="store_true",
                        help="synthetic inputs, separate data root (no internet; results are NOT real)")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("steps", help="list all steps of all wheels")
    one = sub.add_parser("run", help="run one step of one wheel")
    one.add_argument("package", choices=sorted(PACKAGES))
    one.add_argument("step")
    for name in ORDERS:
        sub.add_parser(name, help=f"run {len(ORDERS[name])} steps in order")
    show = sub.add_parser("show", help="print a table, e.g. `show gold forecast_tomorrow`")
    show.add_argument("layer")
    show.add_argument("table")
    show.add_argument("-n", type=int, default=20)

    args = parser.parse_args(argv)
    configure_logging(args.verbose)

    if args.command == "steps":
        for pkg, steps in PACKAGES.items():
            print(f"smogcast-{pkg}: {', '.join(steps)}")
        return 0
    if args.command == "run" and args.step not in PACKAGES[args.package]:
        parser.error(f"unknown step {args.step!r} for {args.package}; available: {', '.join(PACKAGES[args.package])}")

    # Spark starts only after argument parsing, so `--help` and typos fail instantly.
    from smogcast.core.context import build_context

    ctx = build_context(args.env, offline=args.offline)
    log = logging.getLogger("smogcast")
    if args.command == "run":
        PACKAGES[args.package][args.step](ctx)
    elif args.command in ORDERS:
        for pkg, step in ORDERS[args.command]:
            log.info("===== %s: %s =====", pkg, step)
            PACKAGES[pkg][step](ctx)
    elif args.command == "show":
        ctx.storage.read(args.layer, args.table).show(args.n, truncate=False)
    return 0


if __name__ == "__main__":
    run(main)
