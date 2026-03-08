# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Transform (CDC merge)
# MAGIC
# MAGIC Reads bronze delta tables and merges latest CDC state into external silver delta tables.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "walmart_lakehouse")
dbutils.widgets.text("domain", "customers")  # customers | products | orders
dbutils.widgets.text("bucket", "dms-snowpipe-dev-05d6e64a")
dbutils.widgets.text("silver_base_path", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
domain = dbutils.widgets.get("domain").strip().lower()
bucket = dbutils.widgets.get("bucket")
silver_base_path = dbutils.widgets.get("silver_base_path").strip()

if not silver_base_path:
    silver_base_path = f"s3://{bucket}/databricks/silver"

if domain not in {"customers", "products", "orders"}:
    raise ValueError(f"Unsupported domain: {domain}")

# Try to use the specified catalog, fall back to hive_metastore if it doesn't exist
try:
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
except Exception as e:
    if "not found" in str(e).lower() or "does not exist" in str(e).lower():
        print(f"Catalog '{catalog}' not found, falling back to hive_metastore")
        catalog = "hive_metastore"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
        spark.sql(f"USE {schema}")
    else:
        raise


def table_exists(full_name: str) -> bool:
    try:
        spark.table(full_name)
        return True
    except Exception:
        return False


def latest_cdc(df, key_cols):
    for col_name in ["dms_commit_ts", "updated_at", "created_at", "ingested_at"]:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast("timestamp"))

    w = Window.partitionBy(*key_cols).orderBy(
        F.col("dms_commit_ts").desc_nulls_last(),
        F.col("updated_at").desc_nulls_last(),
        F.col("created_at").desc_nulls_last(),
        F.col("ingested_at").desc_nulls_last(),
    )
    return df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


def upsert_with_deletes(source_df, target_table, target_path, key_cols):
    if not table_exists(target_table):
        source_df.filter(F.col("dms_op") != "D").limit(0).write.format("delta").mode("overwrite").save(target_path)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {target_table} USING DELTA LOCATION '{target_path}'")

    delta_t = DeltaTable.forName(spark, target_table)
    join_expr = " AND ".join([f"t.{c} = s.{c}" for c in key_cols])

    (
        delta_t.alias("t")
        .merge(source_df.alias("s"), join_expr)
        .whenMatchedDelete("s.dms_op = 'D'")
        .whenMatchedUpdateAll("s.dms_op <> 'D'")
        .whenNotMatchedInsertAll("s.dms_op <> 'D'")
        .execute()
    )


def process_customers():
    bronze = spark.table(f"{catalog}.{schema}.bronze_customers")
    latest = latest_cdc(bronze, ["customer_id"])
    upsert_with_deletes(
        latest,
        f"{catalog}.{schema}.silver_customers",
        f"{silver_base_path}/customers",
        ["customer_id"],
    )


def process_products():
    bronze = spark.table(f"{catalog}.{schema}.bronze_products")
    latest = latest_cdc(bronze, ["product_id"])
    upsert_with_deletes(
        latest,
        f"{catalog}.{schema}.silver_products",
        f"{silver_base_path}/products",
        ["product_id"],
    )


def process_orders():
    orders = spark.table(f"{catalog}.{schema}.bronze_orders")
    latest_orders = latest_cdc(orders, ["order_id"])
    upsert_with_deletes(
        latest_orders,
        f"{catalog}.{schema}.silver_orders",
        f"{silver_base_path}/orders",
        ["order_id"],
    )

    items = spark.table(f"{catalog}.{schema}.bronze_order_items").withColumn(
        "line_total", F.col("quantity") * F.col("unit_price")
    )
    latest_items = latest_cdc(items, ["order_item_id"])
    upsert_with_deletes(
        latest_items,
        f"{catalog}.{schema}.silver_order_items",
        f"{silver_base_path}/order_items",
        ["order_item_id"],
    )


if domain == "customers":
    process_customers()
elif domain == "products":
    process_products()
else:
    process_orders()

dbutils.notebook.exit(f"Silver transform completed for domain={domain}")
