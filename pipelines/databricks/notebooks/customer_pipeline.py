# Databricks notebook source
# MAGIC %md
# MAGIC # User Profile Standardization - Normalize JSON
# MAGIC
# MAGIC Ingests and normalizes JSON user profile data from bronze layer with error handling for malformed records.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "walmart_lakehouse")
dbutils.widgets.text("bucket", "dms-snowpipe-dev-05d6e64a")
dbutils.widgets.text("source_path", "")
dbutils.widgets.text("target_table", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
bucket = dbutils.widgets.get("bucket")
source_path = dbutils.widgets.get("source_path").strip()
target_table = dbutils.widgets.get("target_table").strip()

if not source_path:
    source_path = f"s3://{bucket}/bronze/user_profiles/"
if not target_table:
    target_table = f"{catalog}.{schema}.normalized_user_profiles"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# Define schema for expected user profile structure
expected_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("profile_data", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# COMMAND ----------

def normalize_json():
    """
    Read and normalize JSON user profile data with error handling for malformed records.
    Uses PERMISSIVE mode to capture malformed records in a separate column.
    """
    
    # Read JSON with PERMISSIVE mode to handle malformed records gracefully
    df = (
        spark.read
        .format("json")
        .option("mode", "PERMISSIVE")  # Handle malformed records
        .option("columnNameOfCorruptRecord", "_corrupt_record")  # Store malformed records
        .schema(expected_schema.add(StructField("_corrupt_record", StringType(), True)))
        .load(source_path)
    )
    
    # Split valid and invalid records
    valid_df = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
    invalid_df = df.filter(F.col("_corrupt_record").isNotNull())
    
    # Log invalid records count if any
    invalid_count = invalid_df.count()
    if invalid_count > 0:
        print(f"WARNING: Found {invalid_count} malformed records. Writing to error table.")
        
        # Write malformed records to error table for investigation
        error_table = f"{catalog}.{schema}.normalized_user_profiles_errors"
        (
            invalid_df
            .select(
                F.col("_corrupt_record").alias("corrupt_record"),
                F.current_timestamp().alias("error_detected_at")
            )
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(error_table)
        )
    
    # Add processing metadata to valid records
    normalized_df = (
        valid_df
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
    )
    
    # Write normalized records to target table
    (
        normalized_df
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )
    
    valid_count = normalized_df.count()
    print(f"Successfully normalized {valid_count} user profile records.")
    print(f"Malformed records: {invalid_count}")
    
    return valid_count, invalid_count


# COMMAND ----------

valid_count, invalid_count = normalize_json()
dbutils.notebook.exit(f"Normalization completed: {valid_count} valid records, {invalid_count} malformed records")
