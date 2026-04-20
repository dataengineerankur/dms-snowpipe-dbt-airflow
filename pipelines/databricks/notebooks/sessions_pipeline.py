# Databricks notebook source
# MAGIC %md
# MAGIC # Sessions Pipeline Transform
# MAGIC
# MAGIC Transforms sessions events from raw.sessions_events_delta to gold layer.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "walmart_lakehouse")
dbutils.widgets.text("raw_schema", "raw")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
raw_schema = dbutils.widgets.get("raw_schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{raw_schema}")

# COMMAND ----------

def table_exists(catalog_name: str, schema_name: str, table_name: str) -> bool:
    try:
        spark.table(f"{catalog_name}.{schema_name}.{table_name}")
        return True
    except Exception:
        return False


def create_upstream_table_if_not_exists():
    """Create the upstream raw.sessions_events_delta table if it doesn't exist."""
    table_name = f"{catalog}.{raw_schema}.sessions_events_delta"
    
    if not table_exists(catalog, raw_schema, "sessions_events_delta"):
        schema_def = """
            session_id STRING,
            user_id STRING,
            event_type STRING,
            event_timestamp TIMESTAMP,
            page_url STRING,
            referrer STRING,
            user_agent STRING,
            ip_address STRING,
            created_at TIMESTAMP,
            ingested_at TIMESTAMP
        """
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {schema_def}
            )
            USING DELTA
        """)
        print(f"Created upstream table: {table_name}")
    else:
        print(f"Upstream table exists: {table_name}")


def transform_sessions():
    """Transform sessions data from raw to gold layer."""
    source_table = f"{catalog}.{raw_schema}.sessions_events_delta"
    target_table = f"{catalog}.{schema}.gold_sessions_summary"
    
    if not table_exists(catalog, raw_schema, "sessions_events_delta"):
        create_upstream_table_if_not_exists()
    
    sessions_df = spark.table(source_table)
    
    if sessions_df.isEmpty():
        print("No sessions data to process")
        if not table_exists(catalog, schema, "gold_sessions_summary"):
            sessions_df.limit(0).write.format("delta").mode("overwrite").saveAsTable(target_table)
            print(f"Created empty target table: {target_table}")
        return
    
    sessions_summary = (
        sessions_df
        .groupBy("session_id", "user_id")
        .agg(
            F.min("event_timestamp").alias("session_start"),
            F.max("event_timestamp").alias("session_end"),
            F.count("*").alias("event_count"),
            F.countDistinct("page_url").alias("unique_pages"),
            F.max("created_at").alias("last_updated")
        )
    )
    
    sessions_summary.write.format("delta").mode("overwrite").saveAsTable(target_table)
    print(f"Transformed sessions data to: {target_table}")


create_upstream_table_if_not_exists()
transform_sessions()

dbutils.notebook.exit("Sessions pipeline completed successfully")
