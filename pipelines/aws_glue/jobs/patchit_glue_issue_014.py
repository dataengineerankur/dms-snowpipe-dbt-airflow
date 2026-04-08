"""GL014 - Schema drift handling for missing total_amount_usd column
Glue job that handles missing column by computing it from available data."""
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_BUCKET",
        "SOURCE_PREFIX",
        "REDSHIFT_SCHEMA",
        "REDSHIFT_CONNECTION_NAME",
        "TempDir",
    ],
)

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

bucket = args["S3_BUCKET"]
source_prefix = args["SOURCE_PREFIX"].rstrip("/")
schema = args["REDSHIFT_SCHEMA"]
conn_name = args["REDSHIFT_CONNECTION_NAME"]
tmp_dir = args["TempDir"]

s3_path = f"s3://{bucket}/{source_prefix}/orders/"
df = spark.read.parquet(s3_path)

if "total_amount_usd" not in df.columns:
    if "quantity" in df.columns and "unit_price" in df.columns:
        df = df.withColumn("total_amount_usd", F.col("quantity") * F.col("unit_price"))
    elif "amount" in df.columns:
        df = df.withColumn("total_amount_usd", F.col("amount"))
    else:
        df = df.withColumn("total_amount_usd", F.lit(0.0))

dyf = glue_context.create_dynamic_frame.from_df(df, glue_context, "orders")

create_table_ddl = f"""
    CREATE TABLE IF NOT EXISTS {schema}.orders (
      order_id BIGINT,
      customer_id BIGINT,
      order_status VARCHAR(32),
      order_date TIMESTAMP,
      quantity INTEGER,
      unit_price NUMERIC(12,2),
      total_amount_usd NUMERIC(12,2),
      updated_at TIMESTAMP
    )
"""

glue_context.write_dynamic_frame.from_jdbc_conf(
    frame=dyf,
    catalog_connection=conn_name,
    connection_options={
        "dbtable": f"{schema}.orders",
        "preactions": f"CREATE SCHEMA IF NOT EXISTS {schema}; {create_table_ddl}",
    },
    redshift_tmp_dir=tmp_dir,
)

job.commit()
