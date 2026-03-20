import sys
import os
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
for arg in ["S3_BUCKET", "SILVER_PREFIX", "GOLD_PREFIX"]:
    for i, argv in enumerate(sys.argv):
        if argv == f"--{arg}" and i + 1 < len(sys.argv):
            optional_args[arg] = sys.argv[i + 1]
            break

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

bucket = optional_args.get("S3_BUCKET", os.environ.get("S3_BUCKET", ""))
silver_prefix = optional_args.get("SILVER_PREFIX", os.environ.get("SILVER_PREFIX", "silver")).rstrip("/")
gold_prefix = optional_args.get("GOLD_PREFIX", os.environ.get("GOLD_PREFIX", "gold")).rstrip("/")

orders = spark.read.parquet(f"s3://{bucket}/{silver_prefix}/orders/")
order_items = spark.read.parquet(f"s3://{bucket}/{silver_prefix}/order_items/")

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
        orders.order_amount,
        orders.updated_at.alias("order_updated_at"),
        F.coalesce(items_agg.total_items, F.lit(0)).alias("total_items"),
        F.coalesce(items_agg.gross_revenue, F.lit(0)).alias("gross_revenue"),
    )
)

fct_orders.write.mode("overwrite").parquet(
    f"s3://{bucket}/{gold_prefix}/fct_orders/"
)

job.commit()
