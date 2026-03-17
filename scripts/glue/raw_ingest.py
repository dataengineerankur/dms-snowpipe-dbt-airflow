import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "TARGET_BUCKET",
        "RAW_PREFIX",
    ],
)

try:
    optional_args = getResolvedOptions(sys.argv, ["SOURCE_BUCKET", "SOURCE_PREFIX"])
    args.update(optional_args)
except Exception:
    pass

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

source_bucket = args.get("SOURCE_BUCKET", args["TARGET_BUCKET"])
source_prefix = args.get("SOURCE_PREFIX", "dms-source").rstrip("/")
target_bucket = args["TARGET_BUCKET"]
raw_prefix = args["RAW_PREFIX"].rstrip("/")

s3 = boto3.client("s3")

source_prefix = source_prefix + "/" if source_prefix else ""
raw_prefix = raw_prefix + "/" if raw_prefix else ""

paginator = s3.get_paginator("list_objects_v2")
copied = 0

for page in paginator.paginate(Bucket=source_bucket, Prefix=source_prefix):
    for obj in page.get("Contents", []):
        key = obj["Key"]
        if key.endswith("/"):
            continue
        relative_key = key[len(source_prefix) :] if source_prefix else key
        target_key = f"{raw_prefix}{relative_key}"
        s3.copy_object(
            Bucket=target_bucket,
            Key=target_key,
            CopySource={"Bucket": source_bucket, "Key": key},
        )
        copied += 1

if copied == 0:
    raise RuntimeError(f"No objects found under s3://{source_bucket}/{source_prefix}")

job.commit()
