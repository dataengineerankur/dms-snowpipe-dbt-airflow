# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Transform
# MAGIC
# MAGIC Reads silver delta tables and creates/merges gold serving tables.
# MAGIC
# MAGIC - Gold tables are managed Delta tables in metastore.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "hive_metastore")
dbutils.widgets.text("schema", "walmart_lakehouse")
dbutils.widgets.text("domain", "customers")  # customers | products | orders

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
domain = dbutils.widgets.get("domain").strip().lower()

if domain not in {"customers", "products", "orders"}:
    raise ValueError(f"Unsupported domain: {domain}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def table_exists(full_name: str) -> bool:
    try:
        spark.table(full_name)
        return True
    except Exception:
        return False


def merge_table(source_df, target_table, key_cols):
    if not table_exists(target_table):
        source_df.limit(0).write.format("delta").saveAsTable(target_table)

    dt = DeltaTable.forName(spark, target_table)
    cond = " AND ".join([f"t.{c} = s.{c}" for c in key_cols])
    dt.alias("t").merge(source_df.alias("s"), cond).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def gold_customers():
    src = spark.table(f"{catalog}.{schema}.silver_customers").select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "created_at",
        "updated_at",
        "dms_commit_ts",
    )
    merge_table(src, f"{catalog}.{schema}.gold_dim_customers", ["customer_id"])


def gold_products():
    src = spark.table(f"{catalog}.{schema}.silver_products").select(
        "product_id",
        "sku",
        "product_name",
        "category",
        "price",
        "created_at",
        "updated_at",
        "dms_commit_ts",
    )
    merge_table(src, f"{catalog}.{schema}.gold_dim_products", ["product_id"])


def gold_orders():
    orders = spark.table(f"{catalog}.{schema}.silver_orders").select(
        "order_id",
        "customer_id",
        "order_status",
        "order_date",
        F.col("updated_at").alias("order_updated_at"),
    )
    customers = spark.table(f"{catalog}.{schema}.silver_customers").select(
        "customer_id", "first_name", "last_name", "email"
    )
    items = spark.table(f"{catalog}.{schema}.silver_order_items").groupBy("order_id").agg(
        F.sum("quantity").alias("total_items"),
        F.sum("line_total").alias("gross_revenue"),
        F.max("updated_at").alias("items_updated_at"),
    )

    fct_orders = (
        orders.join(customers, "customer_id", "left")
        .join(items, "order_id", "left")
        .select(
            "order_id",
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "order_status",
            "order_date",
            "order_updated_at",
            F.coalesce(F.col("total_items"), F.lit(0)).alias("total_items"),
            F.coalesce(F.col("gross_revenue"), F.lit(0.0)).alias("gross_revenue"),
            F.greatest(
                F.col("order_updated_at"), F.coalesce(F.col("items_updated_at"), F.col("order_updated_at"))
            ).alias("record_updated_at"),
        )
    )
    merge_table(fct_orders, f"{catalog}.{schema}.gold_fct_orders", ["order_id"])

    order_items = spark.table(f"{catalog}.{schema}.silver_order_items").select(
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "line_total",
        F.col("updated_at").alias("record_updated_at"),
    )
    merge_table(order_items, f"{catalog}.{schema}.gold_fct_order_items", ["order_item_id"])


if domain == "customers":
    gold_customers()
elif domain == "products":
    gold_products()
else:
    gold_orders()

dbutils.notebook.exit(f"Gold transform completed for domain={domain}")
