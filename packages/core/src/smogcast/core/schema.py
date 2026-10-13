"""Shared table contracts. Step wheels never import each other, so column lists that two
steps must agree on (the producer and the consumer of a table) live here, in smogcast-core.
"""

# silver.features — produced by smogcast-silver-transform, consumed by smogcast-model.
FEATURE_KEYS = ["city_id", "pollutant", "issue_day", "target_day", "split", "weather_source"]
PM_FEATURES = [
    "pm_d1_max_ug_m3", "pm_d1_mean_ug_m3", "pm_d1_exceeded", "pm_d2_max_ug_m3",
    "pm_d_morning_max_ug_m3", "pm_d_morning_mean_ug_m3",
]
WEATHER_FEATURES = [
    "t_mean_c", "t_min_c", "t_max_c", "t_range_c", "rh_mean_pct", "wind_mean_ms", "wind_min_ms",
    "wind_max_ms", "calm_hours", "wind_from_deg_mean", "precip_sum_mm", "precip_hours",
    "pressure_mean_hpa", "cloud_mean_pct", "radiation_sum_wh_m2",
]
CALENDAR_FEATURES = ["target_month", "target_dow", "target_is_weekend", "target_is_heating_season"]
LABEL = "exceeded_next_day"

# GIOŚ archive files — read by smogcast-bronze, and written by the synthetic generator in smogcast-ingest
# (offline runs, integration tests), so both must use exactly these labels.
# The Polish strings are the labels INSIDE the files published by GIOŚ and are matched character by
# character — do not translate them; the English names are the bronze columns they map to.
# Hourly sheets <year>_<token>_1g.xlsx: header rows are found by the label in the first column.
GIOS_POSITION_ROW_LABEL = "Kod stanowiska"
GIOS_HOURLY_HEADER_LABELS = ["Nr", "Kod stacji", "Wskaźnik", "Czas uśredniania", "Jednostka", GIOS_POSITION_ROW_LABEL]
# Metadata xlsx: sheet name and Polish column → English bronze column (columns not listed are dropped).
GIOS_STATIONS_SHEET = "STACJE"
GIOS_POSITIONS_SHEET = "STANOWISKA"
GIOS_STATION_COLUMNS = {
    "Kod stacji": "station_code",
    "Kod międzynarodowy": "international_code",
    "Nazwa stacji": "station_name",
    "Stary Kod stacji \n(o ile inny od aktualnego)": "old_station_codes",
    "Data uruchomienia": "opened_on",
    "Data zamknięcia": "closed_on",
    "Typ stacji": "station_type",
    "Typ obszaru": "area_type",
    "Rodzaj stacji": "station_kind",
    "Województwo": "voivodeship",
    "Miejscowość": "town",
    "Adres": "address",
    "WGS84 φ N": "lat",
    "WGS84 λ E": "lon",
}
GIOS_POSITION_COLUMNS = {
    "Kod stanowiska": "position_code",
    "Kod stacji": "station_code",
    "Wskaźnik - kod": "pollutant",
    "Czas uśredniania": "averaging",
    "Typ pomiaru": "measurement_type",
    "Data uruchomienia": "opened_on",
    "Data zamknięcia": "closed_on",
}
