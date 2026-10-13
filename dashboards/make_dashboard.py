"""Generate dashboards/smogcast.lvdash.json (AI/BI dashboard, serialized Lakeview format)."""
import json
import sys

OUT = sys.argv[1]

# --------------------------------------------------------------------------------------------- datasets
# Unqualified table names: the bundle sets dataset_catalog = ${var.catalog}, dataset_schema = gold_secure.
MODEL_NAME = ("CASE model WHEN 'gbt' THEN 'Model (gradient-boosted trees)' WHEN 'logistic' THEN 'Logistic regression' "
              "WHEN 'persistence' THEN 'Tomorrow like today' WHEN 'climatology' THEN 'Calendar average' ELSE model END")
TEST_NAME = ("CASE criterion WHEN 'K1' THEN 'Better than the calendar average?' "
             "WHEN 'K2' THEN 'Better than \"tomorrow like today\"?' "
             "WHEN 'K3' THEN 'Does \"70%\" really mean about 70%?' "
             "WHEN 'K4a' THEN 'Does it catch smog days?' "
             "WHEN 'K4b' THEN 'Are its warnings rarely false?' END")
REQUIRED = ("CASE criterion WHEN 'K1' THEN 'above 0' "
            "WHEN 'K2' THEN concat('below ', format_number(threshold, 4)) "
            "WHEN 'K3' THEN 'gap at most 15 pp' "
            "WHEN 'K4a' THEN 'at least 60%' WHEN 'K4b' THEN 'at most 50%' END")
REACHED = ("CASE WHEN criterion IN ('K4a', 'K4b') THEN concat(format_number(100 * value, 0), '%') "
           "WHEN criterion = 'K3' THEN concat(format_number(100 * value, 1), ' pp') "
           "ELSE format_number(value, 4) END")

# Every run (preliminary → official → update) of the newest target day, all cities, one pollutant.
UPDATES = """
SELECT city_name, run_kind, ROUND(100 * probability, 1) AS probability_pct
FROM forecast_history
WHERE target_day = (SELECT max(target_day) FROM forecast_history) AND pollutant = '{pollutant}'
  AND probability IS NOT NULL
ORDER BY city_name, CASE run_kind WHEN 'preliminary' THEN 1 WHEN 'official' THEN 2 ELSE 3 END, issued_at"""

# Test year 2025, one row per city and day: the operational model's warning vs what happened, and what "tomorrow
# like today" (persistence) would have said. backtest_predictions has city_id only -> names from conf/cities.yaml.
CITY_NAMES = {"warszawa": "Warszawa", "krakow": "Kraków", "wroclaw": "Wrocław", "lodz": "Łódź", "poznan": "Poznań",
              "gdansk": "Gdańsk", "szczecin": "Szczecin", "bydgoszcz": "Bydgoszcz", "lublin": "Lublin",
              "bialystok": "Białystok"}
CITY_CASE = "CASE p.city_id " + " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in CITY_NAMES.items()) + " END"
TEST_BASE = f"""days AS (
  SELECT {CITY_CASE} AS city_name, p.pollutant, date_format(p.target_day, 'yyyy-MM') AS month, p.label, p.warned,
         b.warned AS persistence_warned
  FROM backtest_predictions p
  JOIN (SELECT DISTINCT pollutant, model FROM model_selection WHERE operational) s
    ON s.pollutant = p.pollutant AND s.model = p.model
  LEFT JOIN backtest_predictions b
    ON b.model = 'persistence' AND b.split = p.split AND b.city_id = p.city_id AND b.pollutant = p.pollutant
   AND b.target_day = p.target_day
  WHERE p.split = 'test'
)"""
TEST_DAYS = """
WITH """ + TEST_BASE.replace("{", "{{").replace("}", "}}") + """
SELECT city_name, month,
       CASE WHEN warned AND label = 1 THEN '✓ smog, warned in advance'
            WHEN label = 1 THEN '✗ smog missed'
            ELSE '✗ false alarm' END AS outcome
FROM days
WHERE pollutant = '{pollutant}' AND (label = 1 OR warned)
ORDER BY month, CASE WHEN warned AND label = 1 THEN 1 WHEN label = 1 THEN 2 ELSE 3 END"""

# Month by month, computed in SQL (the table widget does not evaluate formulas): "16 of 19 (84%)" – both parts of a
# percentage visible; of the SUMS, not an average of city percentages; en dash when there is nothing to count.
def _of(num, den):
    return (f"CASE WHEN {den} = 0 THEN '–' ELSE concat(CAST({num} AS INT), ' of ', CAST({den} AS INT), ' (', "
            f"CAST(ROUND(100 * {num} / {den}) AS INT), '%)') END")


HIT, P_HIT = "count_if(warned AND label = 1)", "count_if(persistence_warned AND label = 1)"
MONTHS = ("WITH " + TEST_BASE.replace("{", "{{").replace("}", "}}") + """
SELECT {group}month, pollutant, CAST(sum(label) AS INT) AS smog_days, count_if(warned) AS warnings,
       """ + _of(HIT, "sum(label)") + """ AS caught,
       """ + _of(HIT, "count_if(warned)") + """ AS warnings_right,
       """ + _of(P_HIT, "sum(label)") + """ AS persistence_caught,
       """ + _of(P_HIT, "count_if(persistence_warned)") + """ AS persistence_right
FROM days
GROUP BY ALL
HAVING sum(label) > 0 OR count_if(warned) > 0 OR count_if(persistence_warned) > 0   -- quiet months: nothing to show
ORDER BY {order}month, pollutant""")

DATASETS = {
    "tomorrow": ("Tomorrow's forecast", """
SELECT city_name, pollutant, target_day, ROUND(100 * probability, 1) AS probability_pct,
       -- table text: the widget would print 20.00 and null, so numbers and "–" are formatted here
       coalesce(format_number(100 * probability, 1), '–') AS probability_txt,
       CASE WHEN warned THEN 'warning' WHEN status = 'ok' THEN 'no warning' ELSE 'no forecast' END AS verdict,
       run_kind, date_format(issued_at + INTERVAL 1 HOUR, 'yyyy-MM-dd HH:mm') AS issued_cet,
       coalesce(format_number(pm_d1_max_ug_m3, 0), '–') AS yesterday_ug_m3,
       coalesce(format_number(pm_d_morning_max_ug_m3, 0), '–') AS this_morning_ug_m3,
       coalesce(format_number(t_mean_c, 1), '–') AS temperature_c, coalesce(format_number(wind_mean_ms, 1), '–') AS wind_ms,
       coalesce(CAST(calm_hours AS STRING), '–') AS calm_hours, coalesce(format_number(precip_sum_mm, 1), '–') AS rain_mm,
       coalesce(unusable_reason, '–') AS unusable_reason
FROM forecast_tomorrow
WHERE target_day = (SELECT max(target_day) FROM forecast_tomorrow)
ORDER BY probability DESC"""),
    # The limits come from the data (conf/thresholds.yaml → limit_ug_m3), not a second copy in the dashboard.
    "tomorrow_limits": ("Daily limits", """
SELECT CAST(max(CASE WHEN pollutant = 'PM10' THEN limit_ug_m3 END) AS INT) AS pm10_limit,
       CAST(max(CASE WHEN pollutant = 'PM2.5' THEN limit_ug_m3 END) AS INT) AS pm25_limit
FROM forecast_tomorrow
WHERE target_day = (SELECT max(target_day) FROM forecast_tomorrow)"""),
    "tomorrow_summary": ("Tomorrow – summary", """
SELECT max(target_day) AS target_day, count_if(warned) AS warnings, count_if(status = 'ok') AS forecasts,
       max(run_kind) AS run_kind
FROM forecast_tomorrow
WHERE target_day = (SELECT max(target_day) FROM forecast_tomorrow)"""),
    "updates_pm10": ("Tomorrow's forecast run by run – PM10", UPDATES.format(pollutant="PM10")),
    "updates_pm25": ("Tomorrow's forecast run by run – PM2.5", UPDATES.format(pollutant="PM2.5")),
    "updates_table": ("Tomorrow's forecast runs side by side", """
SELECT target_day, city_name, pollutant,
       coalesce(format_number(100 * max(CASE WHEN run_kind = 'preliminary' THEN probability END), 1), '–')
         AS preliminary_pct,
       coalesce(format_number(100 * max(CASE WHEN run_kind = 'official' THEN probability END), 1), '–')
         AS official_pct,
       coalesce(format_number(100 * max_by(CASE WHEN run_kind = 'update' THEN probability END,
                                           CASE WHEN run_kind = 'update' THEN issued_at END), 1), '–')
         AS latest_update_pct
FROM forecast_history
WHERE target_day = (SELECT max(target_day) FROM forecast_history)
GROUP BY ALL
ORDER BY city_name, pollutant"""),
    "season": ("Exceedance days per heating season", """
SELECT heating_season, city_name, pollutant, count_if(exceeded) AS exceedance_days, count(*) AS measured_days
FROM measured_daily
WHERE heating_season IS NOT NULL
GROUP BY ALL"""),
    "recent": ("Measured – last 60 days", """
SELECT day_cet, city_name, pollutant, ROUND(max_station_mean_ug_m3, 1) AS worst_station_ug_m3, limit_ug_m3, source
FROM measured_daily
WHERE day_cet >= date_sub(current_date(), 60)"""),
    "verification": ("Official forecasts vs measurements", """
WITH v AS (
  SELECT target_day, city_name, pollutant, concat(format_number(limit_ug_m3, 0), ' µg/m³') AS daily_limit,
         concat(format_number(100 * probability, 0), '% chance') AS p,
         concat('measured ', format_number(measured_max_ug_m3, 0), ' µg/m³') AS m, outcome
  FROM forecast_verification
), verdicts AS (
  SELECT target_day, city_name, pollutant, daily_limit,
         CASE outcome
           WHEN 'hit' THEN concat('✓ smog, warned in advance (', p, ' → ', m, ')')
           WHEN 'correct_no_warning' THEN concat('✓ no smog, as forecast (', p, ' → ', m, ')')
           WHEN 'miss' THEN concat('✗ smog missed (', p, ' → ', m, ')')
           WHEN 'false_alarm' THEN concat('✗ false alarm (', p, ' → ', m, ')')
           WHEN 'pending' THEN concat('⏳ waiting for the day to end (', p, ')')
           WHEN 'no_data' THEN concat('– no valid measurement (', p, ')')
           WHEN 'no_forecast' THEN '– no forecast issued'
         END AS verdict
  FROM v
)
SELECT target_day, city_name,
       max(CASE WHEN pollutant = 'PM10' THEN daily_limit END) AS pm10_limit,
       max(CASE WHEN pollutant = 'PM10' THEN verdict END) AS pm10,
       max(CASE WHEN pollutant = 'PM2.5' THEN daily_limit END) AS pm25_limit,
       max(CASE WHEN pollutant = 'PM2.5' THEN verdict END) AS pm25
FROM verdicts
GROUP BY ALL
ORDER BY target_day DESC, city_name"""),
    "verification_summary": ("Live track record", """
SELECT pollutant,
       count_if(outcome IN ('hit', 'miss', 'false_alarm', 'correct_no_warning')) AS judged_forecasts,
       count_if(outcome = 'hit') AS hits, count_if(outcome = 'miss') AS misses,
       count_if(outcome = 'false_alarm') AS false_alarms, count_if(outcome = 'correct_no_warning') AS correct_quiet,
       ROUND(avg(squared_error), 4) AS live_brier, count_if(outcome = 'pending') AS pending
FROM forecast_verification
GROUP BY pollutant
ORDER BY pollutant"""),
    "test_days_pm10": ("Test year 2025 – PM10 warnings per month", TEST_DAYS.format(pollutant="PM10")),
    "test_days_pm25": ("Test year 2025 – PM2.5 warnings per month", TEST_DAYS.format(pollutant="PM2.5")),
    "test_months": ("Test year 2025 – month by month, per city", MONTHS.format(
        group="city_name, ", order="city_name, ")),
    "test_months_all": ("Test year 2025 – month by month, all cities together", MONTHS.format(group="", order="")),
    "brier_pm10": ("Brier score PM10 (test year 2025)", f"""
SELECT {MODEL_NAME} AS model_name, ROUND(brier, 4) AS brier
FROM backtest_metrics
WHERE split = 'test' AND city_id = 'ALL' AND pollutant = 'PM10'
ORDER BY brier"""),
    "brier_pm25": ("Brier score PM2.5 (test year 2025)", f"""
SELECT {MODEL_NAME} AS model_name, ROUND(brier, 4) AS brier
FROM backtest_metrics
WHERE split = 'test' AND city_id = 'ALL' AND pollutant = 'PM2.5'
ORDER BY brier"""),
    "reliability": ("Calibration (test year 2025)", """
SELECT r.pollutant, ROUND(100 * r.mean_probability, 1) AS forecast_pct,
       ROUND(100 * r.observed_frequency, 1) AS observed_pct, r.n_days
FROM reliability r
JOIN (SELECT DISTINCT pollutant, model FROM model_selection WHERE operational) s
  ON s.pollutant = r.pollutant AND s.model = r.model
WHERE r.split = 'test' AND r.n_days >= 30"""),
    "acceptance": ("Reliability tests", f"""
SELECT pollutant, {TEST_NAME} AS test, criterion AS code, {REQUIRED} AS required, {REACHED} AS reached,
       CASE WHEN passed THEN '✓ passed' ELSE '✗ not passed' END AS result
FROM acceptance
UNION ALL
SELECT 'PM2.5', t.test, t.code, '–', '–', '– (PM10 only)'
FROM VALUES ('Does it catch smog days?', 'K4a'), ('Are its warnings rarely false?', 'K4b') AS t(test, code)
ORDER BY pollutant, code"""),
    "dq": ("Live data quality", """
SELECT source,
       CASE metric WHEN 'records_received' THEN 'Records received' WHEN 'unique_readings' THEN 'Unique readings'
            WHEN 'resent_duplicates' THEN 'Re-sent duplicates (collapsed)' WHEN 'late_rejected' THEN 'Late (rejected)'
            WHEN 'quarantined' THEN 'Quarantined (total)' WHEN 'silver_readings' THEN 'Accepted readings'
            WHEN 'silver_valid' THEN 'Accepted and valid' ELSE metric END AS measure,
       metric, value, ROUND(pct_of_unique, 1) AS pct_of_unique
FROM dq_summary
ORDER BY source, metric"""),
    "dq_reasons": ("Why readings were set aside", """
SELECT source,
       CASE metric WHEN 'late_rejected' THEN 'late'
                   WHEN 'quarantined_missing_value' THEN 'empty value' WHEN 'quarantined_below_min' THEN 'below 0'
                   WHEN 'quarantined_above_max' THEN 'above 1000 µg/m³' WHEN 'quarantined_unknown_station' THEN 'unknown station'
                   WHEN 'flag_frozen' THEN 'flag: frozen sensor' WHEN 'flag_spike' THEN 'flag: spike'
                   WHEN 'flag_drift' THEN 'flag: drift' END AS reason,
       value
FROM dq_summary
WHERE metric IN ('late_rejected', 'quarantined_missing_value', 'quarantined_below_min', 'quarantined_above_max',
                 'quarantined_unknown_station', 'flag_frozen', 'flag_spike', 'flag_drift')"""),
}
# The inputs table has its own copy of the dataset: a page filter acts on every widget of its dataset, and the city
# filter must narrow the table only (the chart keeps all cities). Rows grouped by city: PM10 and PM2.5 together.
DATASETS["tomorrow_inputs"] = ("Tomorrow – inputs per city",
                               DATASETS["tomorrow"][1].replace("ORDER BY probability DESC", "ORDER BY city_name, pollutant"))


def dataset(name, display, sql):
    lines = sql.strip().split("\n")
    return {"name": name, "displayName": display, "queryLines": [ln + "\n" for ln in lines[:-1]] + [lines[-1]]}


# --------------------------------------------------------------------------------------------- widgets
_n = 0


def wid(prefix):
    global _n
    _n += 1
    return f"{prefix}_{_n:02d}"


def pos(x, y, w, h):
    return {"x": x, "y": y, "width": w, "height": h}


def text(lines, x, y, w, h):
    return {"widget": {"name": wid("text"), "multilineTextboxSpec": {"lines": lines}}, "position": pos(x, y, w, h)}


def fields(cols):
    return [{"name": c, "expression": f"`{c}`"} for c in cols]


def query(ds, cols, disaggregated=True):
    return [{"name": "main_query", "query": {"datasetName": ds, "fields": fields(cols), "disaggregated": disaggregated}}]


def table(ds, cols, title, x, y, w, h):
    return {"widget": {"name": wid("table"), "queries": query(ds, [c for c, _ in cols]),
                       "spec": {"version": 2, "widgetType": "table",
                                "encodings": {"columns": [{"fieldName": c, "displayName": d} for c, d in cols]},
                                "frame": {"showTitle": True, "title": title}}},
            "position": pos(x, y, w, h)}


def counter(ds, col, title, x, y, w, h, description=None):
    frame = {"showTitle": True, "title": title}
    if description:
        frame.update(showDescription=True, description=description)
    return {"widget": {"name": wid("counter"),
                       "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": [
                           {"name": f"sum({col})", "expression": f"SUM(`{col}`)"}], "disaggregated": False}}],
                       "spec": {"version": 2, "widgetType": "counter",
                                "encodings": {"value": {"fieldName": f"sum({col})", "displayName": title}},
                                "frame": frame}},
            "position": pos(x, y, w, h)}


def chart(kind, ds, x_col, x_type, x_name, y_col, y_name, title, x, y, w, h, color=None, group=False,
          description=None, natural_order=False, y_domain=None):
    cols = [x_col, y_col] + ([color[0]] if color else [])
    enc = {"x": {"fieldName": x_col, "scale": {"type": x_type}, "displayName": x_name},
           "y": {"fieldName": y_col, "scale": {"type": "quantitative"}, "displayName": y_name}}
    if y_domain:   # a fixed range: 6% must look small, not like the top of the chart
        enc["y"]["scale"]["domain"] = {"min": y_domain[0], "max": y_domain[1]}
    if natural_order:
        enc["x"]["scale"]["sort"] = {"by": "natural-order"}
    if color:
        enc["color"] = {"fieldName": color[0], "scale": {"type": "categorical"}, "displayName": color[1]}
        if natural_order:
            enc["color"]["scale"]["sort"] = {"by": "natural-order"}
    spec = {"version": 3, "widgetType": kind, "encodings": enc, "frame": {"showTitle": True, "title": title}}
    if description:
        spec["frame"].update(showDescription=True, description=description)
    if group:
        spec["mark"] = {"layout": "group"}
    return {"widget": {"name": wid(kind), "queries": query(ds, cols), "spec": spec}, "position": pos(x, y, w, h)}


def count_chart(ds, x_col, x_name, color_col, color_name, y_name, title, x, y, w, h, description=None):
    """Stacked bars: rows counted per x and colour (the dataset has one row per city-day)."""
    q = [{"name": "main_query", "query": {"datasetName": ds, "fields": [
        {"name": x_col, "expression": f"`{x_col}`"}, {"name": color_col, "expression": f"`{color_col}`"},
        {"name": "count(*)", "expression": "COUNT(*)"}], "disaggregated": False}}]
    enc = {"x": {"fieldName": x_col, "scale": {"type": "categorical", "sort": {"by": "natural-order"}},
                 "displayName": x_name},
           "y": {"fieldName": "count(*)", "scale": {"type": "quantitative"}, "displayName": y_name},
           "color": {"fieldName": color_col, "scale": {"type": "categorical", "sort": {"by": "natural-order"}},
                     "displayName": color_name}}
    frame = {"showTitle": True, "title": title}
    if description:
        frame.update(showDescription=True, description=description)
    return {"widget": {"name": wid("bar"), "queries": q,
                       "spec": {"version": 3, "widgetType": "bar", "encodings": enc, "frame": frame}},
            "position": pos(x, y, w, h)}


def sum_table(ds, keys, sums, title, x, y, w, h):
    """Table summed over the rows the filters leave (e.g. City = All -> all cities added up).
    keys: [(column, header)]; sums: [(field name, SQL aggregate expression, header)]."""
    fields_ = [{"name": c, "expression": f"`{c}`"} for c, _ in keys] + [
        {"name": n, "expression": e} for n, e, _ in sums]
    cols = [{"fieldName": c, "displayName": d} for c, d in keys] + [{"fieldName": n, "displayName": d}
                                                                     for n, _, d in sums]
    return {"widget": {"name": wid("table"),
                       "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": fields_,
                                                                    "disaggregated": False}}],
                       "spec": {"version": 2, "widgetType": "table", "encodings": {"columns": cols},
                                "frame": {"showTitle": True, "title": title}}},
            "position": pos(x, y, w, h)}


def select_filter(field, title, datasets, x, y, w, h, default=None, multi=False):
    """default: one value, or a list of values for a multi-select filter."""
    qs, fs = [], []
    for ds in datasets:
        qn = f"filter_{field}_{ds}"
        qs.append({"name": qn, "query": {"datasetName": ds, "fields": [
            {"name": field, "expression": f"`{field}`"},
            {"name": f"{field}_associativity", "expression": "COUNT_IF(`associative_filter_predicate_group`)"}],
            "disaggregated": False}})
        fs.append({"fieldName": field, "displayName": title, "queryName": qn})
    spec = {"version": 2, "widgetType": "filter-multi-select" if multi else "filter-single-select",
            "encodings": {"fields": fs}, "frame": {"showTitle": True, "title": title}}
    if default:
        values = [{"value": v} for v in ([default] if isinstance(default, str) else default)]
        spec["selection"] = {"defaultSelection": {"values": {"dataType": "STRING", "values": values}}}
    return {"widget": {"name": wid("filter"), "queries": qs, "spec": spec}, "position": pos(x, y, w, h)}


# --------------------------------------------------------------------------------------------- pages
CHANCE = "Chance of exceeding the daily limit (%)"
MONTH_COLS = [("month", "Month"), ("pollutant", "Pollutant"), ("smog_days", "Smog days"),
              ("warnings", "Warnings issued"), ("caught", "Model: smog days that got a warning"),
              ("warnings_right", "Model: warnings followed by smog"),
              ("persistence_caught", "\"Tomorrow like today\": smog days that got a warning"),
              ("persistence_right", "\"Tomorrow like today\": warnings followed by smog")]
NOTE_RLS = ("*What you see depends on who you are: rows by voivodeship and technical columns are filtered by "
            "Unity Catalog (schema `gold_secure`).*")

pages = [
    ("tomorrow", "Tomorrow", [
        text(["# Will the daily limit be exceeded tomorrow?\n",
              "Probability that the **daily mean of the worst station** in the city exceeds the limit "
              "(PM10 50 µg/m³, PM2.5 25 µg/m³). A warning is issued at 50% or more. ", NOTE_RLS], 0, 0, 6, 2),
        counter("tomorrow_summary", "warnings", "Warnings for tomorrow", 0, 2, 2, 3),
        counter("tomorrow_summary", "forecasts", "Cities × pollutants with a forecast", 2, 2, 2, 3),
        text(["**Run kinds**\n", "- *preliminary* – before 11:30 CET, this morning's readings are not in yet\n",
              "- *official* – first run from 11:30 CET, judged against measurements\n",
              "- *update* – later run, newer weather forecast"], 4, 2, 2, 3),
        chart("bar", "tomorrow", "city_name", "categorical", "City", "probability_pct", CHANCE,
              "Chance of an exceedance tomorrow", 0, 5, 6, 7, color=("pollutant", "Pollutant"), group=True,
              y_domain=(0, 100), description="A warning is issued at 50% or more."),
        counter("tomorrow_limits", "pm10_limit", "PM10 daily limit (µg/m³)", 0, 12, 3, 3),
        counter("tomorrow_limits", "pm25_limit", "PM2.5 daily limit (µg/m³)", 3, 12, 3, 3),
        select_filter("city_name", "Cities in the table below (empty = all)", ["tomorrow_inputs"], 0, 15, 2, 2,
                      default=["Kraków", "Warszawa", "Wrocław"], multi=True),
        table("tomorrow_inputs", [("city_name", "City"), ("pollutant", "Pollutant"), ("probability_txt", "Chance of exceedance (%)"),
                           ("verdict", "Verdict"), ("run_kind", "Run"), ("issued_cet", "Issued (CET)"),
                           ("yesterday_ug_m3", "Yesterday – worst station, daily mean (µg/m³)"),
                           ("this_morning_ug_m3", "This morning – worst station, 00–10 CET (µg/m³)"),
                           ("temperature_c", "Temp. tomorrow (°C)"), ("wind_ms", "Wind (m/s)"),
                           ("calm_hours", "Calm hours"), ("rain_mm", "Rain (mm)"), ("unusable_reason", "Why no forecast")],
              "Why this forecast – the main inputs", 0, 17, 6, 8),
    ]),
    ("forecast_updates", "Forecast updates", [
        text(["# How today's forecast for tomorrow changed\n",
              "Tomorrow's forecast is computed three times today: preliminary (00:30 CET, this morning's readings "
              "not in yet), official (11:30 CET) and update (17:30 CET, newer weather forecast). Only the "
              "**official** one is later judged against measurements (page *Did it come true?*). "
              "A missing bar = that run has not happened yet."], 0, 0, 6, 2),
        chart("bar", "updates_pm10", "city_name", "categorical", "City", "probability_pct", CHANCE,
              "PM10 – tomorrow's forecast, run by run", 0, 2, 6, 7, color=("run_kind", "Run"), group=True,
              natural_order=True, y_domain=(0, 100), description="A warning is issued at 50% or more."),
        chart("bar", "updates_pm25", "city_name", "categorical", "City", "probability_pct", CHANCE,
              "PM2.5 – tomorrow's forecast, run by run", 0, 9, 6, 7, color=("run_kind", "Run"), group=True,
              natural_order=True, y_domain=(0, 100), description="A warning is issued at 50% or more."),
        table("updates_table", [("target_day", "Forecast for (day)"), ("city_name", "City"),
                                ("pollutant", "Pollutant"), ("preliminary_pct", "Preliminary (%)"),
                                ("official_pct", "Official (%)"), ("latest_update_pct", "Latest update (%)")],
              "The same in numbers", 0, 16, 6, 8),
    ]),
    ("measured", "Measured", [
        text(["# What was really measured\n",
              "An **exceedance day**: the daily mean of the city's worst station is above the limit. "
              "Heating season = October–April. Data: GIOŚ archive 2021–2025 (verified) and the live GIOŚ feed "
              "(finished days only, not yet verified by GIOŚ). PM10 limit: at most 35 exceedance days a year."],
             0, 0, 6, 2),
        select_filter("pollutant", "Pollutant", ["season", "recent"], 0, 2, 2, 2),
        chart("bar", "season", "city_name", "categorical", "City", "exceedance_days", "Exceedance days",
              "Exceedance days per heating season", 0, 4, 6, 7, color=("heating_season", "Heating season"),
              group=True),
        chart("line", "recent", "day_cet", "temporal", "Day (CET)", "worst_station_ug_m3", "µg/m³",
              "Worst station's daily mean – last 60 days", 0, 11, 6, 7, color=("city_name", "City")),
    ]),
    ("came_true", "Did it come true?", [
        text(["# Did the official forecasts come true?\n",
              "Each official forecast (issued at 11:30 CET the day before) next to what was measured on that day: "
              "the daily mean of the city's worst station compared with the daily limit. A warning is issued at "
              "50% or more.\n",
              "- ✓ *no smog, as forecast* – no warning, limit not exceeded\n",
              "- ✓ *smog, warned in advance* – warning, limit exceeded\n",
              "- ✗ *smog missed* – limit exceeded without a warning\n",
              "- ✗ *false alarm* – warning, but the limit was not exceeded\n",
              "- ⏳ *waiting* – the day is not over yet (judged after midnight CET)\n\n",
              "The live record is short (the live feed started in October 2026); the test year 2025 on the Model "
              "page is the main evidence – this page shows the model keeps working on new data."], 0, 0, 6, 4),
        table("verification_summary", [("pollutant", "Pollutant"), ("judged_forecasts", "Judged forecasts"),
                                       ("correct_quiet", "✓ No smog, as forecast"),
                                       ("hits", "✓ Smog, warned in advance"), ("misses", "✗ Smog missed"),
                                       ("false_alarms", "✗ False alarms"),
                                       ("live_brier", "Live Brier score (lower = better)"),
                                       ("pending", "⏳ Waiting for the day to end")],
              "Live track record", 0, 4, 6, 4),
        table("verification", [("target_day", "Day"), ("city_name", "City"),
                               ("pm10_limit", "PM10 daily limit"), ("pm10", "PM10 – did it come true?"),
                               ("pm25_limit", "PM2.5 daily limit"), ("pm25", "PM2.5 – did it come true?")],
              "Every official forecast", 0, 8, 6, 9),
    ]),
    ("test_year", "Test year 2025", [
        text(["# How the model did in 2025, month by month\n",
              "2025 is the test year: the model never saw it while learning, and the tests on the Model page were "
              "fixed before it was checked. Each bar counts **smog days** – days when the daily limit was exceeded "
              "(the city's worst station: PM10 above 50 µg/m³, PM2.5 above 25 µg/m³) – and **false alarms**. "
              "A warning means a forecast of 50% or more.\n",
              "**All 10 cities together** (charts with City = All, and the last table): days are summed over the "
              "cities – one smog day = one city on one day (February: 28 days × 10 cities = 280 city-days)."], 0, 0, 6, 3),
        select_filter("city_name", "City", ["test_days_pm10", "test_days_pm25", "test_months"], 0, 3, 2, 2,
                      default="Kraków"),
        count_chart("test_days_pm10", "month", "Month", "outcome", "Outcome", "Days",
                    "PM10 – smog days caught, missed, and false alarms", 0, 5, 6, 7),
        count_chart("test_days_pm25", "month", "Month", "outcome", "Outcome", "Days",
                    "PM2.5 – smog days caught, missed, and false alarms", 0, 12, 6, 7),
        text(["**How to read the tables** – two questions, both answered as \"X of Y\":\n",
              "- **Smog days that got a warning** – did we miss smog? Example: 16 of 19 = of 19 smog days, 16 were "
              "warned the day before. Higher is better.\n",
              "- **Warnings followed by smog** – can you trust a warning? Example: 16 of 22 = of 22 warnings, 16 were "
              "followed by a real smog day (the other 6 were false alarms). Higher is better.\n",
              "- A good forecast needs **both** to be high: warning every day would catch every smog day, but most "
              "warnings would be wrong.\n",
              "- **\"Tomorrow like today\"** – the simplest possible forecast: warn if the last full day was a smog "
              "day (when the forecast is issued at 11:30, today is not over yet, so that is yesterday). "
              "The model should do better than that.\n",
              "- Why \"tomorrow like today\" is not enough: it is always **two days late**. It misses the first two "
              "days of every smog episode and keeps warning for two days after the smog is gone. It looks fine in "
              "long episodes (a frosty February), but when smog comes in short bursts of one or two days it never "
              "catches it. The model also looks at this morning's readings and the weather forecast, so it can warn "
              "before an episode starts.\n",
              "- Our goal, set before testing: at least 60% of smog days warned, at least half of the warnings "
              "followed by smog.\n",
              "- Months not shown had no smog day and no warning – the model correctly stayed quiet "
              "(e.g. May–September). A dash (–) = nothing to count."],
             0, 19, 6, 6),
        table("test_months", [("city_name", "City")] + MONTH_COLS,
              "Month by month in the chosen city – the model vs \"tomorrow like today\"", 0, 25, 6, 8),
        table("test_months_all", MONTH_COLS,
              "All 10 cities together (not affected by the City filter)", 0, 33, 6, 8),
    ]),
    ("model", "Model", [
        text(["# How good is the model?\n",
              "Five tests were fixed **before training**. The model is trusted only if it passes all of them. "
              "Judged on 2025 – a year the model never saw."], 0, 0, 6, 2),
        table("acceptance", [("pollutant", "Pollutant"), ("test", "Test"), ("required", "Required"),
                             ("reached", "Reached"), ("result", "Result"), ("code", "Code")],
              "Reliability tests (test year 2025)", 0, 2, 6, 6),
        chart("bar", "brier_pm10", "model_name", "categorical", "Forecast", "brier", "Brier score",
              "PM10 – Brier score, lower is better", 0, 8, 3, 6),
        chart("bar", "brier_pm25", "model_name", "categorical", "Forecast", "brier", "Brier score",
              "PM2.5 – Brier score, lower is better", 3, 8, 3, 6,
              description="Scales differ – compare forecasts within one pollutant, not PM10 with PM2.5."),
        chart("scatter", "reliability", "forecast_pct", "quantitative", "Forecast probability (%)", "observed_pct",
              "Exceedance really happened (%)", "Does \"70%\" mean 70%? (bands with at least 30 days)", 0, 14, 6, 7,
              color=("pollutant", "Pollutant"),
              description="A well-calibrated model puts the points near the diagonal: forecast % ≈ observed %."),
    ]),
    ("data_quality", "Data quality", [
        text(["# Data quality of the live feed\n",
              "*live* = GIOŚ API, *fault_replay* = demonstration with injected faults (never used by the model). "
              "Nothing is dropped silently: every set-aside reading has a reason."], 0, 0, 6, 2),
        select_filter("source", "Source", ["dq", "dq_reasons"], 0, 2, 2, 2),
        chart("bar", "dq_reasons", "reason", "categorical", "Reason", "value", "Readings",
              "Readings set aside or flagged", 0, 4, 6, 6),
        table("dq", [("source", "Source"), ("measure", "Measure"), ("value", "Count"),
                     ("pct_of_unique", "% of unique readings")], "All measures", 0, 10, 6, 8),
    ]),
]

dash = {
    "datasets": [dataset(k, d, s) for k, (d, s) in DATASETS.items()],
    "pages": [{"name": n, "displayName": d, "layout": layout, "pageType": "PAGE_TYPE_CANVAS"} for n, d, layout in pages],
}
text_all = json.dumps(dash, ensure_ascii=False)
assert "—" not in text_all, "no em dash in the app/dashboard (author's rule)"
with open(OUT, "w", encoding="utf-8", newline="\n") as f:
    json.dump(dash, f, ensure_ascii=False, indent=2)
    f.write("\n")
print(len(dash["datasets"]), "datasets,", sum(len(p[2]) for p in pages), "widgets")
