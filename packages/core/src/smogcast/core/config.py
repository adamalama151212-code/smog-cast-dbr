"""Configuration loading: shared domain files + one environment file (local/dev/prod).

Why this layout:
- Domain files (cities, thresholds, DQ rules, model settings) are SHARED by all
  environments — the same definitions locally and in PROD.
- The environment file overrides only what really differs: where data lives and
  how much of it we process ("DEV/PROD differ only in variables").
- YAML ships inside the smogcast-core wheel (``smogcast/core/conf``), so a wheel
  deployed to Databricks carries exactly the configuration tested locally.
"""

from __future__ import annotations

import copy
import os
from importlib import resources
from typing import Any

import yaml

# Order matters: later files override earlier ones.
DOMAIN_FILES = ("cities.yaml", "thresholds.yaml", "pm_dq.yaml", "model.yaml", "gios.yaml", "weather.yaml",
                "fault_injection.yaml", "synthetic.yaml", "gold_dictionary.yaml")
ENVIRONMENTS = ("local", "dev", "prod")
ENV_VAR = "SMOGCAST_ENV"

# Offline mode writes to a SEPARATE data root: landing files act as a cache, so
# synthetic data written to `data/` would later be silently reused instead of real data.
OFFLINE_OVERRIDES = {
    "sources": {"pm": "synthetic", "weather": "synthetic"},
    "storage": {"root": "data-offline"},
    # Small scale: the whole pipeline in minutes (two cities: the fault-demo city and a clean one).
    "run": {"cities": ["krakow", "gdansk"]},
    "model": {"grids": {"logistic": {"reg_param": [0.01]}, "gbt": {"max_depth": [3], "max_iter": [10]}}},
}


def deep_merge(base: dict, override: dict) -> dict:
    """Merge dictionaries recursively; values from ``override`` win.

    A plain ``dict.update`` would replace a whole section (e.g. all of ``run``),
    while we want to override a single key (e.g. only ``run.years``).
    """
    result = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def _read_packaged_yaml(name: str) -> dict:
    # importlib.resources instead of a relative file path: works the same for an
    # editable install, an installed wheel and a Databricks cluster.
    text = (resources.files("smogcast.core") / "conf" / name).read_text(encoding="utf-8")
    return yaml.safe_load(text) or {}


def load_config(env: str | None = None, overrides: dict[str, Any] | None = None) -> dict:
    """Return the full configuration as a plain dict.

    Environment resolution: ``env`` argument → ``SMOGCAST_ENV`` variable → ``local``.
    ``overrides`` is for tests and CLI flags (e.g. ``--offline``) — no file edits needed.
    """
    env = env or os.environ.get(ENV_VAR, "local")
    if env not in ENVIRONMENTS:
        raise ValueError(f"Unknown environment {env!r}; allowed: {ENVIRONMENTS}")

    cfg: dict = {}
    for name in DOMAIN_FILES:
        cfg = deep_merge(cfg, _read_packaged_yaml(name))
    cfg = deep_merge(cfg, _read_packaged_yaml(f"{env}.yaml"))
    if overrides:
        cfg = deep_merge(cfg, overrides)
    cfg["env"] = env

    _validate(cfg)
    return cfg


def _validate(cfg: dict) -> None:
    """Fail fast on inconsistent configuration — better now than after an hour of compute."""
    run = cfg["run"]
    unknown = set(run["cities"]) - set(cfg["cities"])
    if unknown:
        raise ValueError(f"run.cities contains unknown cities: {sorted(unknown)}")
    for pollutant in cfg["thresholds"]["daily_limit_ug_m3"]:
        if pollutant not in cfg["pollutants"]:
            raise ValueError(f"Threshold defined for unknown pollutant {pollutant!r}")
    split = cfg["model"]["split"]
    if not (split["train_years"][1] < split["validation_year"] < split["test_year"]):
        # Chronological split only: neighbouring days are similar, so any overlap leaks.
        raise ValueError("model.split must be chronological: train < validation < test")


def active_cities(cfg: dict) -> dict[str, dict]:
    """Cities processed in the current environment, with ``city_id`` added."""
    return {cid: {**cfg["cities"][cid], "city_id": cid} for cid in cfg["run"]["cities"]}
