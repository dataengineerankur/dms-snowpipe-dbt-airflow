# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion with Auto Loader
# MAGIC
# MAGIC Ingests DMS parquet files from `s3://<bucket>/dms/sales/<table>/` into external Delta bronze tables.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "patchit")
dbutils.widgets.text("schema", "walmart_lakehouse")
dbutils.widgets.text("domain", "customers")  # customers | products | orders
dbutils.widgets.text("bucket", "dms-snowpipe-dev-05d6e64a")
dbutils.widgets.text("bronze_base_path", "")
dbutils.widgets.text("checkpoint_base_path", "")
dbutils.widgets.dropdown("include_existing_files", "true", ["true", "false"])
dbutils.widgets.text("aws_secret_scope", "")
dbutils.widgets.text("aws_access_key_name", "AWS_ACCESS_KEY_ID")
dbutils.widgets.text("aws_secret_key_name", "AWS_SECRET_ACCESS_KEY")
dbutils.widgets.text("aws_session_token_name", "AWS_SESSION_TOKEN")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
domain = dbutils.widgets.get("domain").strip().lower()
bucket = dbutils.widgets.get("bucket")
include_existing_files = dbutils.widgets.get("include_existing_files") == "true"

bronze_base_path = dbutils.widgets.get("bronze_base_path").strip()
checkpoint_base_path = dbutils.widgets.get("checkpoint_base_path").strip()


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

    # USER_ISOLATION clusters block sc._jsc.hadoopConfiguration(); spark.hadoop.* is applied to Hadoop FS config.
    def _h(k: str, v: str) -> None:
        spark.conf.set(f"spark.hadoop.{k}", v)

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

if not bronze_base_path:
    bronze_base_path = "dbfs:/tmp/dms-snowpipe/bronze"
if not checkpoint_base_path:
    # Public DBFS is often disabled; file:/local_disk0 is blocked. S3 needs secrets when scope is empty.
    # UC managed volume uses workspace Default Storage (no AWS keys required on Azure).
    checkpoint_base_path = f"/Volumes/{catalog}/{schema}/pipeline_checkpoints/bronze"

domain_to_tables = {
    "customers": ["customers"],
    "products": ["products"],
    "orders": ["orders", "order_items"],
}

if domain not in domain_to_tables:
    raise ValueError(f"Unsupported domain: {domain}")

tables = domain_to_tables[domain]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

def start_autoloader(table_name: str):
    source_path = f"s3://{bucket}/dms/sales/{table_name}/"
    target_table = f"{catalog}.{schema}.bronze_{table_name}"
    checkpoint_path = f"{checkpoint_base_path}/{table_name}"
    schema_path = f"{checkpoint_base_path}/_schemas/{table_name}"

    stream_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.includeExistingFiles", str(include_existing_files).lower())
        .option("pathGlobFilter", "*.parquet")
        .load(source_path)
    )

    # DMS can emit either Op or dms_op; normalize without referencing missing columns.
    if "dms_op" in stream_df.columns and "Op" in stream_df.columns:
        stream_df = stream_df.withColumn("dms_op", F.coalesce(F.col("dms_op"), F.col("Op"))).drop("Op")
    elif "Op" in stream_df.columns:
        stream_df = stream_df.withColumnRenamed("Op", "dms_op")
    elif "dms_op" not in stream_df.columns:
        stream_df = stream_df.withColumn("dms_op", F.lit(None).cast("string"))

    stream_df = (
        stream_df.withColumn("dms_commit_ts", F.to_timestamp("dms_commit_ts"))
        .withColumn("source_file", F.col("_metadata.file_path").cast("string"))
        .withColumn("ingested_at", F.current_timestamp())
    )

    query = (
        stream_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    return query


queries = [start_autoloader(t) for t in tables]
for q in queries:
    q.awaitTermination()

dbutils.notebook.exit(f"Bronze ingestion completed for domain={domain}, tables={tables}")
