"""Registry snapshot JSON (Polish API fields) → English bronze columns, one row per sensor."""

import json

from smogcast.bronze.station_snapshots import snapshot_rows


def test_snapshot_rows_map_api_fields(tmp_path):
    doc = {"snapshot_ts": "2026-10-01T08:00:00+00:00", "source": "gios_api", "stations": [
        {"Identyfikator stacji": 400, "Kod stacji": "MpKrakAlKras", "Nazwa stacji": "Kraków, Aleja Krasińskiego",
         "Nazwa miasta": "Kraków", "Województwo": "MAŁOPOLSKIE", "WGS84 φ N": "50.057678", "WGS84 λ E": "19.926189",
         "sensors": [{"Identyfikator stanowiska": 2750, "Wskaźnik - kod": "PM10"},
                     {"Identyfikator stanowiska": 2752, "Wskaźnik - kod": "PM2.5"}]},
        {"Identyfikator stacji": 401, "Kod stacji": "MpKrakBujaka", "Nazwa miasta": "Kraków", "sensors": []},
    ]}
    path = tmp_path / "snapshot_x.json"
    path.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    rows = snapshot_rows(path)
    assert len(rows) == 3  # 2 sensors + 1 station without sensors
    first = rows[0]
    assert first[2:4] == (400, "MpKrakAlKras") and first[7] == 50.057678 and first[9:11] == (2750, "PM10")
    assert rows[2][9] is None  # station without sensors kept, sensor columns empty
