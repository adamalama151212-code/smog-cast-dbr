# =============================================================================
# Local image for smogcast (Spark 4 + Delta Lake + Java 17), all step wheels
# installed in editable mode.
#
# Why Docker: Spark on Windows needs winutils.exe/hadoop.dll and manual HADOOP_HOME —
# a source of hours of frustration. In a Linux container everything behaves the same
# on Windows, macOS and Linux, and the same as in CI.
# =============================================================================
FROM python:3.11-slim-bookworm

# Java 17: minimum for Spark 4 (Databricks Runtime 17 uses 17 as well).
# procps: Spark uses `ps` when shutting processes down.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

# TZ=UTC: the same hours as on the cluster (see smogcast/core/session.py).
ENV PYTHONUNBUFFERED=1 \
    TZ=UTC \
    SMOGCAST_ENV=local

WORKDIR /app

# Only what installation needs first — Docker caches this layer, so editing code
# does not reinstall dependencies.
COPY VERSION pyproject.toml requirements-dev.txt ./
COPY packages ./packages
# One pip call for all wheels: the step wheels pin smogcast-core==<version>, which pip
# resolves from the editable core in the same call.
RUN pip install --no-cache-dir -r requirements-dev.txt \
    && pip install --no-cache-dir \
       -e "packages/core[local]" -e packages/ingest -e packages/bronze -e packages/silver_clean \
       -e packages/silver_transform -e packages/model -e packages/gold -e packages/cli

# Bake the Delta Lake jars into the image (Ivy cache). Otherwise every new container
# would download them from Maven Central on first start.
RUN python -c "from smogcast.core.config import load_config; from smogcast.core.session import get_spark; get_spark(load_config('local')).stop()"

COPY conftest.py ./

CMD ["smogcast", "--help"]
