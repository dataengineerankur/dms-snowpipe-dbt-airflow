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

customers = spark.read.parquet(f"s3://{bucket}/{silver_prefix}/stg_customers/")

dim_customers = customers.select(
    "customer_id", "first_name", "last_name", "email", "created_at", "updated_at"
)

dim_customers.write.mode("overwrite").parquet(
    f"s3://{bucket}/{gold_prefix}/dim_customers/"
)

job.commit()
