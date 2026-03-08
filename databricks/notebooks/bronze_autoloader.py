# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion with Auto Loader
# MAGIC
# MAGIC Ingests DMS parquet files from `s3://<bucket>/dms/sales/<table>/` into external Delta bronze tables.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Configure AWS credentials for S3 access
try:
    aws_access_key = dbutils.secrets.get(scope="aws", key="access_key_id")
    aws_secret_key = dbutils.secrets.get(scope="aws", key="secret_access_key")
    
    spark.conf.set("fs.s3a.access.key", aws_access_key)
    spark.conf.set("fs.s3a.secret.key", aws_secret_key)
    spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    spark.conf.set("fs.s3.access.key", aws_access_key)
    spark.conf.set("fs.s3.secret.key", aws_secret_key)
    spark.conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.conf.set("fs.s3.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
except Exception as e:
    print(f"Warning: Could not set AWS credentials from secrets: {e}")
    print("Attempting to use instance profile or environment credentials")

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "walmart_lakehouse")
dbutils.widgets.text("domain", "customers")  # customers | products | orders
dbutils.widgets.text("bucket", "dms-snowpipe-dev-05d6e64a")
dbutils.widgets.text("bronze_base_path", "")
dbutils.widgets.text("checkpoint_base_path", "")
dbutils.widgets.dropdown("include_existing_files", "true", ["true", "false"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
domain = dbutils.widgets.get("domain").strip().lower()
bucket = dbutils.widgets.get("bucket")
include_existing_files = dbutils.widgets.get("include_existing_files") == "true"

bronze_base_path = dbutils.widgets.get("bronze_base_path").strip()
checkpoint_base_path = dbutils.widgets.get("checkpoint_base_path").strip()

if not bronze_base_path:
    bronze_base_path = f"s3://{bucket}/databricks/bronze"
if not checkpoint_base_path:
    checkpoint_base_path = f"s3://{bucket}/databricks/checkpoints/bronze"

domain_to_tables = {
    "customers": ["customers"],
    "products": ["products"],
    "orders": ["orders", "order_items"],
}

if domain not in domain_to_tables:
    raise ValueError(f"Unsupported domain: {domain}")

tables = domain_to_tables[domain]

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

def start_autoloader(table_name: str):
    source_path = f"s3://{bucket}/dms/sales/{table_name}/"
    target_table = f"{catalog}.{schema}.bronze_{table_name}"
    target_path = f"{bronze_base_path}/{table_name}"
    checkpoint_path = f"{checkpoint_base_path}/{table_name}"
    schema_path = f"{checkpoint_base_path}/_schemas/{table_name}"

    stream_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.includeExistingFiles", str(include_existing_files).lower())
        .option("pathGlobFilter", "*.parquet")
        .load(source_path)
        .withColumn("dms_op", F.coalesce(F.col("dms_op"), F.col("Op")))
        .drop("Op")
        .withColumn("dms_commit_ts", F.to_timestamp("dms_commit_ts"))
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingested_at", F.current_timestamp())
    )

    query = (
        stream_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .option("path", target_path)
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    return query


queries = [start_autoloader(t) for t in tables]
for q in queries:
    q.awaitTermination()

dbutils.notebook.exit(f"Bronze ingestion completed for domain={domain}, tables={tables}")
