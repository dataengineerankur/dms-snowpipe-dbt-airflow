"""GL005 - Glue finance daily rollup job
Reads gold orders data and writes aggregated finance metrics to Redshift."""
import sys
import os
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


def main():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME"],
    )

    optional_args = {}
    for arg in ["S3_BUCKET", "GOLD_PREFIX", "REDSHIFT_CONNECTION_NAME", "REDSHIFT_SCHEMA", "TempDir"]:
        for i, argv in enumerate(sys.argv):
            if argv == f"--{arg}" and i + 1 < len(sys.argv):
                optional_args[arg] = sys.argv[i + 1]
                break

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    bucket = optional_args.get("S3_BUCKET", os.environ.get("S3_BUCKET", "test-bucket"))
    gold_prefix = optional_args.get("GOLD_PREFIX", os.environ.get("GOLD_PREFIX", "gold")).rstrip("/")
    conn_name = optional_args.get("REDSHIFT_CONNECTION_NAME", os.environ.get("REDSHIFT_CONNECTION_NAME", "redshift_default"))
    schema = optional_args.get("REDSHIFT_SCHEMA", os.environ.get("REDSHIFT_SCHEMA", "finance"))
    tmp_dir = optional_args.get("TempDir", os.environ.get("TempDir", f"s3://{bucket}/tmp/"))

    fct_orders = spark.read.parquet(f"s3://{bucket}/{gold_prefix}/fct_orders/")

    daily_rollup = (
        fct_orders
        .withColumn("order_date_only", F.to_date("order_date"))
        .groupBy("order_date_only")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("gross_revenue").alias("total_revenue"),
            F.avg("gross_revenue").alias("avg_order_value"),
        )
        .select(
            F.col("order_date_only").alias("date"),
            "total_orders",
            "unique_customers",
            F.col("total_revenue").alias("gross_revenue"),
            "avg_order_value",
        )
    )

    dyf = glue_context.create_dynamic_frame.from_df(daily_rollup, glue_context, "daily_rollup")

    table_name = "finance_daily_rollup"
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            date DATE,
            total_orders BIGINT,
            unique_customers BIGINT,
            gross_revenue NUMERIC(18,2),
            avg_order_value NUMERIC(12,2)
        )
    """

    glue_context.write_dynamic_frame.from_jdbc_conf(
        frame=dyf,
        catalog_connection=conn_name,
        connection_options={
            "dbtable": f"{schema}.{table_name}",
            "preactions": f"CREATE SCHEMA IF NOT EXISTS {schema}; {ddl}",
        },
        redshift_tmp_dir=tmp_dir,
    )

    job.commit()


if __name__ == "__main__":
    main()
