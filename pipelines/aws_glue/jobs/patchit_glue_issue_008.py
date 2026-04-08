"""GL008 - Customer dimension join with skew handling
Glue job that performs customer joins with proper skew optimization."""
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

spark_conf = sc.getConf()
spark_conf.set("spark.sql.adaptive.enabled", "true")
spark_conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark_conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark_conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark_conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "10MB")
spark_conf.set("spark.sql.shuffle.partitions", "200")

glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

bucket = optional_args.get("S3_BUCKET", os.environ.get("S3_BUCKET", ""))
silver_prefix = optional_args.get("SILVER_PREFIX", os.environ.get("SILVER_PREFIX", "silver")).rstrip("/")
gold_prefix = optional_args.get("GOLD_PREFIX", os.environ.get("GOLD_PREFIX", "gold")).rstrip("/")

customers = spark.read.parquet(f"s3://{bucket}/{silver_prefix}/customers/")
orders = spark.read.parquet(f"s3://{bucket}/{silver_prefix}/orders/")

customer_orders = (
    orders.join(customers, "customer_id", "left")
    .select(
        orders.order_id,
        orders.customer_id,
        orders.order_status,
        orders.order_date,
        customers.first_name,
        customers.last_name,
        customers.email,
    )
)

customer_orders.write.mode("overwrite").parquet(
    f"s3://{bucket}/{gold_prefix}/customer_orders/"
)

job.commit()
