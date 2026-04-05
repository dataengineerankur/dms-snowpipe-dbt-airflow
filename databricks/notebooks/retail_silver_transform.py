# Databricks notebook source
# MAGIC %md
# MAGIC # Retail Silver Transform
# MAGIC
# MAGIC Builds curated silver Delta tables from the retail bronze parquet zone in S3.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("catalog", "patchit_demo2")
dbutils.widgets.text("schema", "walmart_lakehouse")
dbutils.widgets.text("bucket", "dms-snowpipe-dev-05d6e64a")
dbutils.widgets.text("bronze_prefix", "retail-live/bronze/transactions")
dbutils.widgets.text("aws_secret_scope", "")
dbutils.widgets.text("aws_access_key_name", "AWS_ACCESS_KEY_ID")
dbutils.widgets.text("aws_secret_key_name", "AWS_SECRET_ACCESS_KEY")
dbutils.widgets.text("aws_session_token_name", "AWS_SESSION_TOKEN")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
bucket = dbutils.widgets.get("bucket").strip()
bronze_prefix = dbutils.widgets.get("bronze_prefix").strip().strip("/")


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

bronze_path = f"s3://{bucket}/{bronze_prefix}"
silver_transactions_table = f"{catalog}.{schema}.silver_transactions"
silver_transaction_items_table = f"{catalog}.{schema}.silver_transaction_items"


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


bronze_df = spark.read.parquet(bronze_path)
latest_window = Window.partitionBy("order_id").orderBy(
    F.col("bronze_loaded_at").desc_nulls_last(),
    F.col("spark_ingested_at").desc_nulls_last(),
    F.col("kafka_timestamp").desc_nulls_last(),
    F.col("submitted_at").desc_nulls_last(),
)
item_window = Window.partitionBy("transaction_item_id").orderBy(
    F.col("bronze_loaded_at").desc_nulls_last(),
    F.col("spark_ingested_at").desc_nulls_last(),
    F.col("kafka_timestamp").desc_nulls_last(),
    F.col("transaction_ts").desc_nulls_last(),
)

transactions_df = (
    bronze_df.select(
        F.col("order_id"),
        F.to_timestamp("submitted_at").alias("transaction_ts"),
        F.col("event_type"),
        F.col("schema_version"),
        F.col("customer_name"),
        F.lower(F.col("customer_email")).alias("customer_email"),
        F.col("store_id"),
        F.col("payment_method"),
        F.col("currency"),
        F.col("item_count").cast("int").alias("item_count"),
        F.col("subtotal").cast("double").alias("subtotal"),
        F.col("tax").cast("double").alias("tax"),
        F.col("total").cast("double").alias("total"),
        F.to_timestamp("kafka_timestamp").alias("kafka_timestamp"),
        F.to_timestamp("spark_ingested_at").alias("spark_ingested_at"),
        F.to_timestamp("bronze_loaded_at").alias("bronze_loaded_at"),
        F.col("raw_s3_key"),
        F.sha2(F.lower(F.col("customer_email")), 256).alias("customer_id"),
        F.current_timestamp().alias("silver_updated_at"),
    )
    .filter(F.col("order_id").isNotNull())
    .withColumn("row_num", F.row_number().over(latest_window))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

items_df = (
    bronze_df.select(
        "order_id",
        F.to_timestamp("submitted_at").alias("transaction_ts"),
        F.to_timestamp("kafka_timestamp").alias("kafka_timestamp"),
        F.to_timestamp("spark_ingested_at").alias("spark_ingested_at"),
        F.to_timestamp("bronze_loaded_at").alias("bronze_loaded_at"),
        F.col("currency"),
        F.col("store_id"),
        F.lower(F.col("customer_email")).alias("customer_email"),
        F.posexplode_outer("items").alias("item_position", "item"),
    )
    .select(
        "order_id",
        F.col("item_position").cast("int").alias("item_position"),
        F.col("item.sku").alias("sku"),
        F.col("item.name").alias("product_name"),
        F.col("item.category").alias("category"),
        F.col("item.quantity").cast("int").alias("quantity"),
        F.col("item.unit_price").cast("double").alias("unit_price"),
        F.col("item.line_total").cast("double").alias("line_total"),
        "transaction_ts",
        "kafka_timestamp",
        "spark_ingested_at",
        "bronze_loaded_at",
        "currency",
        "store_id",
        "customer_email",
    )
    .filter(F.col("order_id").isNotNull() & F.col("sku").isNotNull())
    .withColumn("transaction_item_id", F.sha2(F.concat_ws("||", F.col("order_id"), F.col("sku"), F.col("item_position")), 256))
    .withColumn("row_num", F.row_number().over(item_window))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
    .withColumn("silver_updated_at", F.current_timestamp())
)

merge_table(transactions_df, silver_transactions_table, ["order_id"])
merge_table(items_df, silver_transaction_items_table, ["transaction_item_id"])

dbutils.notebook.exit(
    f"Retail silver transform completed: transactions={silver_transactions_table}, items={silver_transaction_items_table}"
)
