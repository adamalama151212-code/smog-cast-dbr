"""Storage layer: one class, two modes — local paths or Unity Catalog.

Why a separate layer (no literal paths in transformation code):
- Locally there is no Unity Catalog, so Delta tables are directories on disk
  (``data/delta/<layer>/<table>``).
- On Databricks the same tables are ``<catalog>.<layer>.<table>`` in UC and raw
  files live in a volume ``/Volumes/...``.
- Transformation code calls ``storage.read("silver", "pm_hourly")`` and does not
  know which mode is active. Moving to the cloud = changing ``storage.mode`` in
  YAML, no change in logic.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)

LAYERS = ("bronze", "silver", "gold", "ops")
DATA_DIR_VAR = "SMOGCAST_DATA_DIR"


class Storage:
    def __init__(self, spark: SparkSession, cfg: dict):
        self.spark = spark
        st = cfg["storage"]
        self.mode = st["mode"]
        if self.mode == "path":
            # The environment variable wins over YAML — tests and Docker can redirect
            # data elsewhere without editing configuration.
            root = os.environ.get(DATA_DIR_VAR, st.get("root", "data"))
            self.root = Path(root).resolve()
            self._landing_root = self.root / "landing"
            self._checkpoint_root = self.root / "checkpoints"
            self._models_root = self.root / "models"
        elif self.mode == "catalog":
            self.catalog = st["catalog"]
            # A shared catalog (the course's PROD) holds every project's schemas side by side; the prefix keeps ours
            # apart: <catalog>.<prefix><layer>.<table>. Empty where the catalog is ours alone (DEV).
            self.schema_prefix = st.get("schema_prefix", "")
            self._landing_root = Path(st["landing_root"])
            self._checkpoint_root = Path(st["checkpoint_root"])
            self._models_root = Path(st["models_root"])
        else:
            raise ValueError(f"Unknown storage.mode: {self.mode!r}")
        # Files polled from the GIOŚ API (registry snapshots, live batches). On Databricks an Azure Function writes
        # them to its own blob container, exposed as an external volume; locally they share the landing zone.
        live = st.get("live_landing_root")
        self._live_landing_root = Path(live) if live else self._landing_root

    # ------------------------------------------------------------------ files
    def landing(self, *parts: str) -> str:
        """Path in the landing zone (raw files: archive xlsx, weather JSON, fault replay, documents)."""
        return str(self._landing_root.joinpath(*parts))

    def live_landing(self, *parts: str) -> str:
        """Path in the LIVE landing zone (GIOŚ API: gios_registry/, gios_live/) — see ``live_landing_root``."""
        return str(self._live_landing_root.joinpath(*parts))

    def checkpoint(self, name: str) -> str:
        """Structured Streaming checkpoint directory — after a restart the stream knows
        which files it has already processed (exactly-once)."""
        return str(self._checkpoint_root / name)

    def models(self, *parts: str) -> str:
        """Directory for saved Spark ML models (PipelineModel.save / load)."""
        return str(self._models_root.joinpath(*parts))

    # ----------------------------------------------------------------- tables
    def _check_layer(self, layer: str) -> None:
        if layer not in LAYERS:
            raise ValueError(f"Unknown layer {layer!r}; allowed: {LAYERS}")

    def table_path(self, layer: str, name: str) -> str:
        return str(self.root / "delta" / layer / name)

    def schema_name(self, schema: str) -> str:
        """``<catalog>.<schema>`` in Unity Catalog, with the environment's schema prefix."""
        return f"{self.catalog}.{self.schema_prefix}{schema}"

    def table_name(self, layer: str, name: str) -> str:
        return f"{self.schema_name(layer)}.{name}"

    def sql_ref(self, layer: str, name: str) -> str:
        """Table reference for SQL text.

        Locally ``delta.`/path``` (path-based table), in UC the three-part name.
        The AI application builds SQL through this method, so queries work in both modes.
        """
        self._check_layer(layer)
        if self.mode == "path":
            return f"delta.`{self.table_path(layer, name)}`"
        return self.table_name(layer, name)

    def exists(self, layer: str, name: str) -> bool:
        self._check_layer(layer)
        if self.mode == "path":
            return DeltaTable.isDeltaTable(self.spark, self.table_path(layer, name))
        return self.spark.catalog.tableExists(self.table_name(layer, name))

    def read(self, layer: str, name: str) -> DataFrame:
        self._check_layer(layer)
        if self.mode == "path":
            return self.spark.read.format("delta").load(self.table_path(layer, name))
        return self.spark.read.table(self.table_name(layer, name))

    def read_stream(self, layer: str, name: str) -> DataFrame:
        """Delta table as a streaming source (bronze → silver on the live path)."""
        self._check_layer(layer)
        if self.mode == "path":
            return self.spark.readStream.format("delta").load(self.table_path(layer, name))
        return self.spark.readStream.table(self.table_name(layer, name))

    def delta_table(self, layer: str, name: str) -> DeltaTable:
        if self.mode == "path":
            return DeltaTable.forPath(self.spark, self.table_path(layer, name))
        return DeltaTable.forName(self.spark, self.table_name(layer, name))

    def _writer(self, df: DataFrame, mode: str, partition_by: list[str] | None):
        w = df.write.format("delta").mode(mode)
        if partition_by:
            w = w.partitionBy(*partition_by)
        return w

    def save(self, writer, layer: str, name: str) -> None:
        """Run a prepared DataFrameWriter against the table (path or UC name)."""
        self._check_layer(layer)
        if self.mode == "path":
            writer.save(self.table_path(layer, name))
        else:
            self._ensure_schema(layer)
            writer.saveAsTable(self.table_name(layer, name))

    def _ensure_schema(self, layer: str) -> None:
        # In PROD schemas are created by bootstrap/governance; this is only a safety
        # net for DEV, where a developer may start from an empty catalog.
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name(layer)}")

    def overwrite(
        self,
        df: DataFrame,
        layer: str,
        name: str,
        partition_by: list[str] | None = None,
        replace_where: str | None = None,
    ) -> None:
        """Overwrite a table or a slice of it.

        ``replace_where`` (e.g. ``"year = 2024"``) atomically replaces only that slice.
        Used when a WHOLE slice is recomputed: as idempotent as a MERGE on the key, but
        much cheaper for millions of rows (no join with the previous version).
        """
        self._check_layer(layer)
        writer = self._writer(df, "overwrite", partition_by)
        if replace_where and self.exists(layer, name):
            new_cols = set(df.columns) - set(self.read(layer, name).columns)
            if new_cols:
                # Schema migration (e.g. data from a previous version without a column):
                # the replaceWhere predicate would evaluate to NULL on old rows and leave
                # them in the table. Recomputing the whole table is safer.
                log.warning("%s.%s: new columns %s — overwriting the whole table (schema migration)",
                            layer, name, sorted(new_cols))
                replace_where = None
        if replace_where and self.exists(layer, name):
            # mergeSchema: a new column in the data is added to the table instead of
            # failing the write.
            writer = writer.option("replaceWhere", replace_where).option("mergeSchema", "true")
        else:
            # Allows a schema change on a full overwrite (e.g. a new gold column).
            writer = writer.option("overwriteSchema", "true")
        self.save(writer, layer, name)
        log.info("Wrote %s.%s (overwrite%s)", layer, name, f", {replace_where}" if replace_where else "")

    def merge(self, df: DataFrame, layer: str, name: str, keys: list[str]) -> None:
        """Upsert on the natural key — the basic idempotency mechanism.

        Re-running with the same data does not duplicate rows: existing keys are
        updated, new ones inserted. New columns of the data are added to the table.
        """
        self._check_layer(layer)
        # A MERGE source must not contain duplicate keys (Delta fails with "multiple
        # source rows matched"). We deduplicate deliberately here.
        df = df.dropDuplicates(keys)
        if not self.exists(layer, name):
            self.save(self._writer(df, "overwrite", None), layer, name)
            log.info("Created %s.%s", layer, name)
            return
        cond = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
        (
            self.delta_table(layer, name)
            .alias("t")
            .merge(df.alias("s"), cond)
            # Schema evolution: a column new in the data (e.g. run_kind in gold.forecast_tomorrow) is added to the
            # table instead of failing the MERGE; older rows get NULL in it.
            .withSchemaEvolution()
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        log.info("MERGE into %s.%s on %s", layer, name, keys)

    def append_stream(self, df: DataFrame, layer: str, name: str, checkpoint: str) -> None:
        """Append a streaming DataFrame to a table: process every waiting input (trigger availableNow), then stop.

        The Delta sink with its checkpoint is exactly-once on its own — a re-run appends only new input and a
        replayed micro-batch is not written twice — so no transaction ids are needed.
        """
        self._check_layer(layer)
        writer = (
            df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
        )
        if self.mode == "path":
            query = writer.start(self.table_path(layer, name))
        else:
            self._ensure_schema(layer)
            query = writer.toTable(self.table_name(layer, name))
        query.awaitTermination()
        log.info("Stream appended to %s.%s", layer, name)
