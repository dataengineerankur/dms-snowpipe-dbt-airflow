# Databricks notebook source
# MAGIC %md
# MAGIC # Sessions Events Transform
# MAGIC
# MAGIC Transforms raw sessions events data into analytics-ready format.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "walmart_lakehouse")
dbutils.widgets.text("bucket", "dms-snowpipe-dev-05d6e64a")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
bucket = dbutils.widgets.get("bucket")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

def table_exists(full_name: str) -> bool:
    try:
        spark.table(full_name)
        return True
    except Exception:
        return False

# COMMAND ----------

# Check if raw.sessions_events_delta table exists
# If not, create an empty table with the expected schema
raw_table = f"{catalog}.raw.sessions_events_delta"

if not table_exists(raw_table):
    # Create the raw schema if it doesn't exist
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.raw")
    
    # Create an empty table with expected schema
    empty_df = spark.createDataFrame([], schema="""
        session_id STRING,
        event_type STRING,
        event_timestamp TIMESTAMP,
        user_id STRING,
        page_url STRING,
        referrer STRING,
        user_agent STRING,
        ip_address STRING,
        created_at TIMESTAMP
    """)
    
    empty_df.write.format("delta").mode("overwrite").saveAsTable(raw_table)
    print(f"Created empty table: {raw_table}")
else:
    print(f"Table exists: {raw_table}")

# COMMAND ----------

# Read from raw table and transform
raw_df = spark.table(raw_table)

# Basic transformations
transformed_df = raw_df.select(
    "session_id",
    "event_type",
    "event_timestamp",
    "user_id",
    "page_url",
    "referrer",
    "user_agent",
    "ip_address",
    F.current_timestamp().alias("processed_at")
)

# Create target table if it doesn't exist
target_table = f"{catalog}.{schema}.sessions_events"

if not table_exists(target_table):
    transformed_df.limit(0).write.format("delta").mode("overwrite").saveAsTable(target_table)
    print(f"Created target table: {target_table}")

# Write transformed data
if transformed_df.count() > 0:
    transformed_df.write.format("delta").mode("append").saveAsTable(target_table)
    print(f"Loaded {transformed_df.count()} records into {target_table}")
else:
    print(f"No data to process. Table {target_table} is ready but empty.")

dbutils.notebook.exit(f"Sessions events transform completed")
