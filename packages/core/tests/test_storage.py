"""Storage.merge: upsert on the key, and a column new in the data is added to the table (schema evolution)."""


def test_merge_upserts_and_adds_a_new_column(ctx):
    st, spark = ctx.storage, ctx.spark
    st.merge(spark.createDataFrame([("a", 1), ("b", 2)], "k STRING, v INT"), "gold", "t", ["k"])
    # a later run brings a new column (as run_kind in gold.forecast_tomorrow) and updates one key
    st.merge(spark.createDataFrame([("b", 20, "official"), ("c", 3, "update")], "k STRING, v INT, kind STRING"),
             "gold", "t", ["k"])
    rows = {r["k"]: (r["v"], r["kind"]) for r in st.read("gold", "t").collect()}
    assert rows == {"a": (1, None), "b": (20, "official"), "c": (3, "update")}
