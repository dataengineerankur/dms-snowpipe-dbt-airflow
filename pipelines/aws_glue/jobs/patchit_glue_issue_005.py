"""GL005 - Job bookmark reset unexpectedly
Glue job for finance daily rollup with schema drift handling."""
import sys
import os
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    
    optional_args = {}
    for arg in ["S3_BUCKET", "REDSHIFT_SCHEMA", "REDSHIFT_CONNECTION_NAME", "TempDir"]:
        for i, argv in enumerate(sys.argv):
            if argv == f"--{arg}" and i + 1 < len(sys.argv):
                optional_args[arg] = sys.argv[i + 1]
                break
    
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    
    bucket = optional_args.get("S3_BUCKET", os.environ.get("S3_BUCKET", "mock-bucket"))
    schema = optional_args.get("REDSHIFT_SCHEMA", os.environ.get("REDSHIFT_SCHEMA", "finance"))
    conn_name = optional_args.get("REDSHIFT_CONNECTION_NAME", os.environ.get("REDSHIFT_CONNECTION_NAME", "redshift-default"))
    tmp_dir = optional_args.get("TempDir", os.environ.get("TempDir", f"s3://{bucket}/tmp/"))
    
    s3_path = f"s3://{bucket}/dms/sales/order_items/"
    df = spark.read.parquet(s3_path)
    
    if "total_amount_usd" not in df.columns:
        df = df.withColumn(
            "total_amount_usd",
            F.coalesce(F.col("quantity") * F.col("unit_price"), F.lit(0))
        )
    
    dyf = glue_context.create_dynamic_frame.from_df(df, glue_context, "order_items_rollup")
    
    ddl = """
        CREATE TABLE IF NOT EXISTS {schema}.finance_daily_rollup (
          order_item_id BIGINT,
          order_id BIGINT,
          product_id BIGINT,
          quantity INTEGER,
          unit_price NUMERIC(12,2),
          total_amount_usd NUMERIC(12,2),
          created_at TIMESTAMP,
          updated_at TIMESTAMP,
          dms_op VARCHAR(4),
          dms_commit_ts TIMESTAMPTZ,
          dms_load_ts TIMESTAMPTZ,
          dms_file_name VARCHAR(512)
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


if __name__ == "__main__":
    main()
