import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_BUCKET",
        "S3_SILVER_PREFIX",
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
s3_prefix = args["S3_SILVER_PREFIX"]
schema = args["REDSHIFT_SCHEMA"]
conn_name = args["REDSHIFT_CONNECTION_NAME"]
tmp_dir = args["TempDir"]

TABLES = {
    "customers": """
        CREATE TABLE IF NOT EXISTS {schema}.customers (
          customer_id BIGINT,
          first_name VARCHAR(255),
          last_name VARCHAR(255),
          email VARCHAR(255),
          created_at TIMESTAMP,
          updated_at TIMESTAMP,
          dms_op VARCHAR(4),
          dms_commit_ts TIMESTAMPTZ,
          dms_load_ts TIMESTAMPTZ,
          dms_file_name VARCHAR(512)
        )
    """,
    "products": """
        CREATE TABLE IF NOT EXISTS {schema}.products (
          product_id BIGINT,
          sku VARCHAR(64),
          product_name VARCHAR(255),
          category VARCHAR(64),
          price NUMERIC(12,2),
          created_at TIMESTAMP,
          updated_at TIMESTAMP,
          dms_op VARCHAR(4),
          dms_commit_ts TIMESTAMPTZ,
          dms_load_ts TIMESTAMPTZ,
          dms_file_name VARCHAR(512)
        )
    """,
    "orders": """
        CREATE TABLE IF NOT EXISTS {schema}.orders (
          order_id BIGINT,
          customer_id BIGINT,
          order_status VARCHAR(32),
          order_date TIMESTAMP,
          order_amount NUMERIC(12,2),
          updated_at TIMESTAMP,
          dms_op VARCHAR(4),
          dms_commit_ts TIMESTAMPTZ,
          dms_load_ts TIMESTAMPTZ,
          dms_file_name VARCHAR(512)
        )
    """,
    "order_items": """
        CREATE TABLE IF NOT EXISTS {schema}.order_items (
          order_item_id BIGINT,
          order_id BIGINT,
          product_id BIGINT,
          quantity INTEGER,
          unit_price NUMERIC(12,2),
          created_at TIMESTAMP,
          updated_at TIMESTAMP,
          dms_op VARCHAR(4),
          dms_commit_ts TIMESTAMPTZ,
          dms_load_ts TIMESTAMPTZ,
          dms_file_name VARCHAR(512)
        )
    """,
}

for table, ddl in TABLES.items():
    s3_path = f"s3://{bucket}/{s3_prefix}/sales/{table}/"
    df = spark.read.parquet(s3_path)
    dyf = glue_context.create_dynamic_frame.from_df(df, glue_context, table)

    glue_context.write_dynamic_frame.from_jdbc_conf(
        frame=dyf,
        catalog_connection=conn_name,
        connection_options={
            "dbtable": f"{schema}.{table}",
            "preactions": f"CREATE SCHEMA IF NOT EXISTS {schema}; {ddl.format(schema=schema)}",
        },
        redshift_tmp_dir=tmp_dir,
    )

job.commit()
