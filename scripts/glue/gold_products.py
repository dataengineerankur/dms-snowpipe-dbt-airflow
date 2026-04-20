import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SILVER_PREFIX",
        "GOLD_PREFIX",
    ],
)

optional_args = getResolvedOptions(sys.argv, [], ["S3_BUCKET"])

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

bucket = optional_args.get("S3_BUCKET", "")
silver_prefix = args["SILVER_PREFIX"].rstrip("/")
gold_prefix = args["GOLD_PREFIX"].rstrip("/")

products = spark.read.parquet(f"s3://{bucket}/{silver_prefix}/products/")

dim_products = products.select(
    "product_id", "sku", "product_name", "category", "price", "created_at", "updated_at"
)

dim_products.write.mode("overwrite").parquet(
    f"s3://{bucket}/{gold_prefix}/dim_products/"
)

job.commit()
