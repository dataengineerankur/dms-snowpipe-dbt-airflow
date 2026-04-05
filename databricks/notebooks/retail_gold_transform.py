# Databricks notebook source
# MAGIC %md
# MAGIC # Retail Gold Transform
# MAGIC
# MAGIC Builds gold Delta tables for transaction reporting from curated retail silver tables.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("catalog", "patchit_demo2")
dbutils.widgets.text("schema", "walmart_lakehouse")
dbutils.widgets.text("aws_secret_scope", "")
dbutils.widgets.text("aws_access_key_name", "AWS_ACCESS_KEY_ID")
dbutils.widgets.text("aws_secret_key_name", "AWS_SECRET_ACCESS_KEY")
dbutils.widgets.text("aws_session_token_name", "AWS_SESSION_TOKEN")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()


def configure_s3_access():
    scope = dbutils.widgets.get("aws_secret_scope").strip()
    if not scope:
        return

    access_key = dbutils.secrets.get(scope, dbutils.widgets.get("aws_access_key_name"))
    secret_key = dbutils.secrets.get(scope, dbutils.widgets.get("aws_secret_key_name"))
    try:
        token = dbutils.secrets.get(scope, dbutils.widgets.get("aws_session_token_name"))
    except Exception:
        token = ""

    def _h(key: str, value: str) -> None:
        spark.conf.set(f"spark.hadoop.{key}", value)

    _h("fs.s3.awsAccessKeyId", access_key)
    _h("fs.s3.awsSecretAccessKey", secret_key)
    _h("fs.s3n.awsAccessKeyId", access_key)
    _h("fs.s3n.awsSecretAccessKey", secret_key)
    _h("fs.s3a.access.key", access_key)
    _h("fs.s3a.secret.key", secret_key)
    _h("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
    _h("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    if token:
        _h("fs.s3.awsSessionToken", token)
        _h("fs.s3n.awsSessionToken", token)
        _h("fs.s3a.session.token", token)
        _h("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")


configure_s3_access()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

silver_transactions_table = f"{catalog}.{schema}.silver_transactions"
silver_transaction_items_table = f"{catalog}.{schema}.silver_transaction_items"
dim_customers_table = f"{catalog}.{schema}.dim_customers"
fact_transactions_table = f"{catalog}.{schema}.fact_transactions"


def table_exists(full_name: str) -> bool:
    try:
        spark.table(full_name)
        return True
    except Exception:
        return False


def merge_table(source_df, target_table: str, key_cols):
    if not table_exists(target_table):
        source_df.limit(0).write.format("delta").saveAsTable(target_table)

    delta_t = DeltaTable.forName(spark, target_table)
    join_expr = " AND ".join([f"t.{col} = s.{col}" for col in key_cols])
    (
        delta_t.alias("t")
        .merge(source_df.alias("s"), join_expr)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


transactions = spark.table(silver_transactions_table)
items = spark.table(silver_transaction_items_table)

latest_customer_window = Window.partitionBy("customer_email").orderBy(F.col("transaction_ts").desc_nulls_last())
dim_customers = (
    transactions.filter(F.col("customer_email").isNotNull())
    .withColumn("row_num", F.row_number().over(latest_customer_window))
    .filter(F.col("row_num") == 1)
    .select(
        F.col("customer_id"),
        F.col("customer_email"),
        F.col("customer_name"),
        F.col("store_id").alias("last_store_id"),
        F.col("payment_method").alias("last_payment_method"),
        F.col("currency").alias("preferred_currency"),
        F.col("transaction_ts").alias("last_transaction_ts"),
        F.col("silver_updated_at").alias("record_updated_at"),
    )
)

fact_items = items.groupBy("order_id").agg(
    F.sum("quantity").alias("total_quantity"),
    F.countDistinct("sku").alias("distinct_skus"),
    F.sum("line_total").alias("merchandise_amount"),
    F.max("silver_updated_at").alias("items_updated_at"),
)

fact_transactions = (
    transactions.alias("t")
    .join(dim_customers.select("customer_id", "customer_email", "customer_name").alias("c"), on="customer_id", how="left")
    .join(fact_items.alias("i"), on="order_id", how="left")
    .select(
        F.col("t.order_id").alias("transaction_id"),
        F.col("t.order_id"),
        F.col("t.customer_id"),
        F.coalesce(F.col("c.customer_email"), F.col("t.customer_email")).alias("customer_email"),
        F.coalesce(F.col("c.customer_name"), F.col("t.customer_name")).alias("customer_name"),
        F.col("t.event_type"),
        F.col("t.transaction_ts"),
        F.col("t.store_id"),
        F.col("t.payment_method"),
        F.col("t.currency"),
        F.col("t.item_count"),
        F.coalesce(F.col("i.total_quantity"), F.col("t.item_count")).alias("total_quantity"),
        F.coalesce(F.col("i.distinct_skus"), F.lit(0)).alias("distinct_skus"),
        F.col("t.subtotal"),
        F.col("t.tax"),
        F.col("t.total").alias("transaction_total"),
        F.coalesce(F.col("i.merchandise_amount"), F.col("t.subtotal")).alias("merchandise_amount"),
        F.col("t.raw_s3_key"),
        F.greatest(F.col("t.silver_updated_at"), F.coalesce(F.col("i.items_updated_at"), F.col("t.silver_updated_at"))).alias("record_updated_at"),
    )
)

merge_table(dim_customers, dim_customers_table, ["customer_email"])
merge_table(fact_transactions, fact_transactions_table, ["order_id"])

dbutils.notebook.exit(
    f"Retail gold transform completed: dim={dim_customers_table}, fact={fact_transactions_table}"
)
