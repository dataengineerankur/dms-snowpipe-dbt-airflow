# Databricks notebook source
# MAGIC %md
# MAGIC # User Profile Standardization Pipeline
# MAGIC
# MAGIC Normalizes JSON user profile data from bronze layer with error handling for malformed records.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "user_profiles")
dbutils.widgets.text("source_path", "")
dbutils.widgets.text("target_path", "")
dbutils.widgets.text("bronze_table", "bronze_user_profiles")
dbutils.widgets.text("target_table", "normalized_user_profiles")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_path = dbutils.widgets.get("source_path").strip()
target_path = dbutils.widgets.get("target_path").strip()
bronze_table = dbutils.widgets.get("bronze_table")
target_table = dbutils.widgets.get("target_table")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

def normalize_json():
    """
    Read and normalize JSON user profile data with proper error handling for malformed records.
    """
    
    # Read JSON data with DROPMALFORMED mode to skip malformed records
    # This prevents SparkException: "Malformed records are detected in JSON parser"
    if source_path:
        df = (
            spark.read
            .format("json")
            .option("mode", "DROPMALFORMED")  # Skip malformed records instead of failing
            .option("multiLine", "false")
            .load(source_path)
        )
    else:
        # Read from bronze table if no source path specified
        bronze_df = spark.table(f"{catalog}.{schema}.{bronze_table}")
        
        # If bronze contains JSON strings in a column, parse them
        if "json_payload" in bronze_df.columns:
            df = (
                bronze_df
                .select(
                    F.from_json(
                        F.col("json_payload"),
                        "user_id STRING, username STRING, email STRING, profile_data STRING, created_at TIMESTAMP"
                    ).alias("data"),
                    "*"
                )
                .select("data.*", F.col("ingested_at").alias("source_ingested_at"))
            )
        else:
            df = bronze_df
    
    # Normalize and standardize the data
    normalized_df = (
        df
        .withColumn("normalized_at", F.current_timestamp())
        .withColumn("email_domain", F.split(F.col("email"), "@").getItem(1))
        .withColumn("username_lower", F.lower(F.col("username")))
    )
    
    # Write to target location
    if target_path:
        normalized_df.write.format("delta").mode("overwrite").save(target_path)
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{target_table} "
            f"USING DELTA LOCATION '{target_path}'"
        )
    else:
        normalized_df.write.format("delta").mode("overwrite").saveAsTable(
            f"{catalog}.{schema}.{target_table}"
        )
    
    record_count = normalized_df.count()
    return record_count

# COMMAND ----------

record_count = normalize_json()
dbutils.notebook.exit(f"User profile normalization completed. Records processed: {record_count}")
