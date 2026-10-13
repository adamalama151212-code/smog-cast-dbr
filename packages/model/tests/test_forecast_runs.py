"""Run kinds of the live forecast: preliminary / official / update (ARCHITECTURE.md, section 7) — no Spark needed."""

import datetime as dt

from smogcast.core.config import load_config
from smogcast.model.forecast_runs import OFFICIAL, PRELIMINARY, UPDATE, official_from, run_kind

T = dt.time
FROM = T(11, 30)


def day_of(runs: list[tuple[str, dt.time]]) -> dict[str, str]:
    """Classify the runs of one issue day in order, as the forecast step does run after run."""
    history: dict[str, str] = {}
    for run_id, at in runs:
        history[run_id] = run_kind(at, FROM, history, run_id)
    return history


def test_the_threshold_comes_from_the_configuration():
    assert official_from(load_config("local")["model"]) == FROM


def test_a_normal_day_preliminary_official_update():
    assert day_of([("a", T(0, 30)), ("b", T(11, 30)), ("c", T(17, 30))]) == {"a": PRELIMINARY, "b": OFFICIAL, "c": UPDATE}


def test_a_failed_official_run_makes_the_next_complete_run_official():
    # 11:30 failed and wrote nothing; the manual re-run at 13:00 is the first run with the full morning
    assert day_of([("a", T(0, 30)), ("manual", T(13, 0)), ("c", T(17, 30))]) == {"a": PRELIMINARY, "manual": OFFICIAL,
                                                                                    "c": UPDATE}


def test_extra_morning_runs_stay_preliminary():
    assert day_of([("a", T(0, 30)), ("test", T(9, 0)), ("b", T(11, 30))]) == {"a": PRELIMINARY, "test": PRELIMINARY,
                                                                                "b": OFFICIAL}


def test_exactly_at_the_threshold_is_official():
    assert run_kind(T(11, 30), FROM, {}, "a") == OFFICIAL
    assert run_kind(T(11, 29, 59), FROM, {}, "a") == PRELIMINARY


def test_a_retry_of_the_same_run_keeps_its_kind():
    # the official run's forecast task failed after writing, and its retry runs 10 minutes later
    assert run_kind(T(11, 53), FROM, {"b": OFFICIAL}, "b") == OFFICIAL
    # without the run id it would have become an update — the reason for passing {{job.run_id}}
    assert run_kind(T(11, 53), FROM, {"b": OFFICIAL}, "other") == UPDATE
