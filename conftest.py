"""Shared pytest fixtures for all packages (tests live in packages/<package>/tests).

One Spark session for the whole run (``scope="session"``): starting the JVM takes a few
seconds, creating it per test would multiply CI time. Every test that writes tables gets
its own data directory (``tmp_path``), so tests do not affect each other.
"""

from __future__ import annotations

import pytest

from smogcast.core.config import load_config

# Small test scale: seconds instead of minutes, but the same logic as in PROD.
TEST_OVERRIDES = {
    "spark": {"master": "local[2]", "shuffle_partitions": 2, "driver_memory": "2g"},
    "sources": {"pm": "synthetic", "weather": "synthetic"},
    "run": {"cities": ["krakow", "gdansk"]},
}


@pytest.fixture(scope="session")
def spark():
    from smogcast.core.session import get_spark

    s = get_spark(load_config("local", TEST_OVERRIDES))
    yield s
    s.stop()


@pytest.fixture
def cfg():
    return load_config("local", TEST_OVERRIDES)


@pytest.fixture
def ctx(spark, cfg, tmp_path, monkeypatch):
    """Pipeline context with an isolated data directory."""
    from smogcast.core.context import Context
    from smogcast.core.storage import Storage

    monkeypatch.setenv("SMOGCAST_DATA_DIR", str(tmp_path / "data"))
    return Context(cfg, spark, Storage(spark, cfg))
