"""Shared infrastructure for all smogcast steps: configuration, Spark session, storage
(local paths / Unity Catalog), data-quality metrics and the step runner.

This is the only package that knows whether we run locally or on Databricks.
"""

__version__ = "0.1.0"
