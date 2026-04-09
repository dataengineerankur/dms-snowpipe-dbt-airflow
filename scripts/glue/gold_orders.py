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
        "SILVER_PREFIX",
        "GOLD_PREFIX",
    ],
)

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

bucket = args["S3_BUCKET"]
silver_prefix = args["SILVER_PREFIX"].rstrip("/")
gold_prefix = args["GOLD_PREFIX"].rstrip("/")

orders = spark.read.parquet(f"s3://{bucket}/{silver_prefix}/orders/")
order_items = spark.read.parquet(f"s3://{bucket}/{silver_prefix}/order_items/")

orders_summary = orders.select("order_id", "customer_id", "order_amount")
orders_summary.write.mode("overwrite").parquet(
    f"s3://{bucket}/{gold_prefix}/orders_summary/"
)

items_agg = (
    order_items.groupBy("order_id")
    .agg(
        F.sum("quantity").alias("total_items"),
        F.sum("line_total").alias("gross_revenue"),
        F.max("updated_at").alias("items_updated_at"),
    )
)

fct_orders = (
    orders.join(items_agg, "order_id", "left")
    .select(
        orders.order_id,
        orders.customer_id,
        orders.order_status,
        orders.order_date,
        orders.updated_at.alias("order_updated_at"),
        F.coalesce(items_agg.total_items, F.lit(0)).alias("total_items"),
        F.coalesce(items_agg.gross_revenue, F.lit(0)).alias("gross_revenue"),
    )
)

fct_orders.write.mode("overwrite").parquet(
    f"s3://{bucket}/{gold_prefix}/fct_orders/"
)

job.commit()
