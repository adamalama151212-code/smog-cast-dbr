"""Storage.merge: upsert on the key, and a column new in the data is added to the table (schema evolution)."""


def test_merge_upserts_and_adds_a_new_column(ctx):
    st, spark = ctx.storage, ctx.spark
    st.merge(spark.createDataFrame([("a", 1), ("b", 2)], "k STRING, v INT"), "gold", "t", ["k"])
    # a later run brings a new column (as run_kind in gold.forecast_tomorrow) and updates one key
    st.merge(spark.createDataFrame([("b", 20, "official"), ("c", 3, "update")], "k STRING, v INT, kind STRING"),
             "gold", "t", ["k"])
    rows = {r["k"]: (r["v"], r["kind"]) for r in st.read("gold", "t").collect()}
    assert rows == {"a": (1, None), "b": (20, "official"), "c": (3, "update")}


def test_a_shared_catalog_gets_prefixed_schemas(spark):
    # The course's PROD has one catalog for every project; DEV has a catalog of its own and no prefix.
    from smogcast.core.config import load_config
    from smogcast.core.storage import Storage

    prod, dev = Storage(spark, load_config("prod")), Storage(spark, load_config("dev"))
    assert prod.table_name("gold", "forecast_tomorrow") == "dbr_dev.smogcast_gold.forecast_tomorrow"
    assert prod.schema_name("gold_secure") == "dbr_dev.smogcast_gold_secure"
    assert dev.table_name("gold", "forecast_tomorrow") == "smogcast_dev.gold.forecast_tomorrow"
