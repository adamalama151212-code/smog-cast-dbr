"""Configuration: environments differ only in their env file, domain settings are shared."""

import pytest

from smogcast.core.config import active_cities, deep_merge, load_config


def test_environments_share_domain_but_differ_in_storage():
    local, dev, prod = (load_config(e) for e in ("local", "dev", "prod"))
    for key in ("cities", "pollutants", "thresholds", "pm_dq", "model"):
        assert local[key] == dev[key] == prod[key]
    assert local["storage"]["mode"] == "path"
    assert dev["storage"]["catalog"] == "smogcast_dev"
    assert prod["storage"]["catalog"] == "smogcast_prod"


def test_deep_merge_overrides_single_key_only():
    merged = deep_merge({"run": {"a": 1, "b": 2}}, {"run": {"b": 3}})
    assert merged == {"run": {"a": 1, "b": 3}}


def test_unknown_city_is_rejected():
    with pytest.raises(ValueError, match="unknown cities"):
        load_config("local", {"run": {"cities": ["atlantis"]}})


def test_split_must_be_chronological():
    with pytest.raises(ValueError, match="chronological"):
        load_config("local", {"model": {"split": {"validation_year": 2020}}})


def test_ten_cities_each_in_its_own_voivodeship():
    cities = active_cities(load_config("local"))
    assert len(cities) == 10
    assert len({c["jurisdiction_code"] for c in cities.values()}) == 10
    assert all(c["city_id"] == cid for cid, c in cities.items())


def test_thresholds_are_numbers():
    limits = load_config("local")["thresholds"]["daily_limit_ug_m3"]
    assert limits == {"PM10": 50, "PM2.5": 25}
