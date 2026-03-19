"""GL005 - Glue job S3 permissions fix
Script that properly handles S3 write operations with correct IAM permissions."""
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
        "SOURCE_BUCKET",
        "TARGET_BUCKET",
    ],
)

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

source_bucket = args["SOURCE_BUCKET"]
target_bucket = args["TARGET_BUCKET"]

df = spark.read.parquet(f"s3://{source_bucket}/raw/orders/")

df_transformed = df.filter(F.col("status").isNotNull()).withColumn(
    "load_timestamp", F.current_timestamp()
)

df_transformed.write.mode("overwrite").parquet(f"s3://{target_bucket}/silver/orders/")

job.commit()
