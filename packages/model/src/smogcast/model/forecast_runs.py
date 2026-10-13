"""Which kind of forecast a live run issues: preliminary, official or update (ARCHITECTURE.md, section 7).

The live Job computes tomorrow's forecast several times on issue day D (resources/job_smogcast_live.yml). The
model was judged on the test year with the inputs available at the 12:00 CET issue time (decision D6), so only a
run that has them is the OFFICIAL forecast — the one to verify against what happened:

- before ``official_from_cet`` (11:30): ``preliminary`` — this morning's readings are not complete; the model fills
  the gap, a situation never evaluated in the backtest;
- the FIRST run from that time on: ``official`` — also when the scheduled run failed and a later one (a manual
  re-run at 13:00) is the first complete one, so a day does not end without an official forecast;
- every later run that day: ``update`` — the same readings, a newer weather forecast.

A retried task belongs to the same orchestrator run (same ``run_id``) and keeps the kind its first attempt wrote,
so a retry can neither duplicate the history nor turn the official forecast into an update.
"""

from __future__ import annotations

import datetime as dt

PRELIMINARY, OFFICIAL, UPDATE = "preliminary", "official", "update"
RUN_KINDS = (PRELIMINARY, OFFICIAL, UPDATE)


def official_from(model_cfg: dict) -> dt.time:
    return dt.time.fromisoformat(model_cfg["official_from_cet"])


def run_kind(now_cet: dt.time, official_from_cet: dt.time, earlier_runs: dict[str, str], run_id: str) -> str:
    """The kind of the run ``run_id`` at CET time ``now_cet``.

    ``earlier_runs`` maps the run ids already in gold.forecast_history for this issue day to their kinds.
    """
    if run_id in earlier_runs:   # a retry of a task of the same run
        return earlier_runs[run_id]
    if now_cet < official_from_cet:
        return PRELIMINARY
    return UPDATE if OFFICIAL in earlier_runs.values() else OFFICIAL
