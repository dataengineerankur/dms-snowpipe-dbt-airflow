"""GL005 - Glue finance daily rollup with schema fix
Handles schema changes in upstream source by renaming gross_revenue to total_amount_usd."""
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
    ],
)

optional_args = {}
for arg in ["S3_BUCKET", "GOLD_PREFIX", "REDSHIFT_SCHEMA", "REDSHIFT_CONNECTION_NAME", "TempDir"]:
    for i, argv in enumerate(sys.argv):
        if argv == f"--{arg}" and i + 1 < len(sys.argv):
            optional_args[arg] = sys.argv[i + 1]
            break

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

bucket = optional_args.get("S3_BUCKET", "test-bucket")
gold_prefix = optional_args.get("GOLD_PREFIX", "gold").rstrip("/")
schema = optional_args.get("REDSHIFT_SCHEMA", "finance")
conn_name = optional_args.get("REDSHIFT_CONNECTION_NAME", "redshift-connection")
tmp_dir = optional_args.get("TempDir", f"s3://{bucket}/temp/")

fct_orders = spark.read.parquet(f"s3://{bucket}/{gold_prefix}/fct_orders/")

finance_rollup = fct_orders.select(
    F.col("order_id"),
    F.col("customer_id"),
    F.col("order_status"),
    F.col("order_date"),
    F.col("total_items"),
    F.col("gross_revenue").alias("total_amount_usd"),
    F.current_timestamp().alias("processed_at")
)

dyf = glue_context.create_dynamic_frame.from_df(
    finance_rollup, glue_context, "finance_daily_rollup"
)

ddl = """
    CREATE TABLE IF NOT EXISTS {schema}.finance_daily_rollup (
      order_id BIGINT,
      customer_id BIGINT,
      order_status VARCHAR(32),
      order_date TIMESTAMP,
      total_items INTEGER,
      total_amount_usd NUMERIC(12,2),
      processed_at TIMESTAMP
    )
"""

glue_context.write_dynamic_frame.from_jdbc_conf(
    frame=dyf,
    catalog_connection=conn_name,
    connection_options={
        "dbtable": f"{schema}.finance_daily_rollup",
        "preactions": f"CREATE SCHEMA IF NOT EXISTS {schema}; {ddl.format(schema=schema)}",
    },
    redshift_tmp_dir=tmp_dir,
)

job.commit()
