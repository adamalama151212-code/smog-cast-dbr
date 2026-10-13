"""The ONE place where time conventions are converted (ARCHITECTURE.md section 5, decision D13).

Conventions:
- tables store ``time_utc`` = START of the measurement hour, in UTC;
- the GIOŚ archive stamps each hourly value with the END of the hour in CET
  (UTC+1 all year, no daylight saving): "2025-01-01 01:00" covers 00:00–01:00 CET;
- the GIOŚ API (live ``getData`` and ``archivalData``) stamps each value with the END of the hour
  in LOCAL time (Europe/Warsaw, with daylight saving) — verified 2026-10-01: archivalData returns
  exactly the archive values and stamps for a winter day (when local time = CET);
- the "day" of a daily mean is the calendar day in CET (``day_cet``), as GIOŚ computes it.
"""

from __future__ import annotations

import datetime as dt
import os
from zoneinfo import ZoneInfo

from pyspark.sql import Column
from pyspark.sql import functions as F

CET_OFFSET = dt.timedelta(hours=1)
LOCAL_TZ = "Europe/Warsaw"
HOUR = dt.timedelta(hours=1)
ISSUE_DATE_VAR = "SMOGCAST_ISSUE_DATE"


def now_cet(now_utc: dt.datetime | None = None) -> dt.datetime:
    """Current wall-clock time in CET (UTC+1 all year, naive) — the project's day and issue time are CET."""
    now_utc = now_utc or dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    return now_utc + CET_OFFSET


def issue_day_cet(now_utc: dt.datetime | None = None) -> dt.date:
    """Day D on which tomorrow's forecast is issued: today in CET, or ``SMOGCAST_ISSUE_DATE``
    (YYYY-MM-DD) — the override lets tests and demos replay a chosen day. Both live steps
    (features and forecast) call this, so they always agree on D."""
    override = os.environ.get(ISSUE_DATE_VAR)
    if override:
        return dt.date.fromisoformat(override)
    return now_cet(now_utc).date()


def round_to_hour(ts: dt.datetime) -> dt.datetime:
    """Round to the NEAREST full hour.

    Archive xlsx timestamps carry accumulating spreadsheet noise (+5 ms per row, up to
    ~44 s by the end of a year); successive rows are still exactly one hour apart.
    """
    return (ts + dt.timedelta(minutes=30)).replace(minute=0, second=0, microsecond=0)


def cet_hour_end_to_utc_start(ts_end_cet: dt.datetime) -> dt.datetime:
    """'01:00 CET' (end of hour) -> '23:00 UTC of the previous day' (start of hour)."""
    return round_to_hour(ts_end_cet) - CET_OFFSET - HOUR


def cet_hour_end_to_utc_start_col(ts_end_cet: Column) -> Column:
    """Spark version of :func:`cet_hour_end_to_utc_start` for an already rounded timestamp column."""
    return ts_end_cet - F.expr("INTERVAL 2 HOURS")


def day_cet_col(time_utc: Column) -> Column:
    """Calendar day in CET of an hour that STARTS at ``time_utc``."""
    return F.to_date(time_utc + F.expr("INTERVAL 1 HOUR"))


def local_hour_end_to_utc_start(ts_end_local: dt.datetime) -> dt.datetime:
    """API value stamped '2026-07-01 13:00' (local, CEST) -> hour starting 2026-07-01 10:00 UTC.

    At the autumn clock change one local hour occurs twice; zoneinfo resolves it to the first
    occurrence (fold=0) — one ambiguous hour a year, accepted.
    """
    aware = round_to_hour(ts_end_local).replace(tzinfo=ZoneInfo(LOCAL_TZ))
    return aware.astimezone(dt.timezone.utc).replace(tzinfo=None) - HOUR


def local_hour_end_to_utc_start_col(ts_end_local: Column) -> Column:
    """Spark version of :func:`local_hour_end_to_utc_start` (session time zone must be UTC)."""
    return F.to_utc_timestamp(ts_end_local, LOCAL_TZ) - F.expr("INTERVAL 1 HOUR")
