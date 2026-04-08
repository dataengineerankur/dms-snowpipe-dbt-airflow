import sys
import os
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RAW_PREFIX",
        "SILVER_PREFIX",
    ],
)

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

bucket = args.get("S3_BUCKET") or os.environ.get("S3_BUCKET", "default-data-bucket")
raw_prefix = args["RAW_PREFIX"].rstrip("/")
silver_prefix = args["SILVER_PREFIX"].rstrip("/")

today = F.date_format(F.current_date(), "yyyyMMdd")


def read_raw(table):
    return (
        spark.read.option("recursiveFileLookup", "true")
        .parquet(f"s3://{bucket}/{raw_prefix}/{table}/")
    )




def run_pydeequ_checks(df, dataset_name, required_cols, key_col=None, non_negative_cols=None):
    non_negative_cols = non_negative_cols or []

    check = Check(spark, CheckLevel.Error, dataset_name)
    for c in required_cols:
        if c in df.columns:
            check = check.isComplete(c)

    if key_col and key_col in df.columns:
        check = check.isUnique(key_col)

    for c in non_negative_cols:
        if c in df.columns:
            check = check.satisfies(f"{c} >= 0", f"{c}_non_negative")

    result = VerificationSuite(spark).onData(df).addCheck(check).run()
    if result.status != "Success":
        raise ValueError(f"PyDeequ DQ failed for {dataset_name}: {result.status}")

def dedupe_latest(df, key_col, ts_cols):
    order_cols = [F.col(c).desc_nulls_last() for c in ts_cols if c in df.columns]
    if not order_cols:
        order_cols = [F.lit(0)]
    w = Window.partitionBy(key_col).orderBy(*order_cols)
    return df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

products = read_raw("products")
op_col = F.col("dms_op") if "dms_op" in products.columns else F.col("Op")
products = products.filter((op_col.isNull()) | (op_col != "D"))
products = dedupe_latest(
    products, "product_id", ["dms_commit_ts", "DMS_COMMIT_TS", "updated_at", "created_at"]
)
products = products.withColumn("load_dt", today)
run_pydeequ_checks(products, "silver_products", ["product_id", "product_name"], key_col="product_id", non_negative_cols=["price"])

products.write.mode("overwrite").parquet(
    f"s3://{bucket}/{silver_prefix}/products/"
)

job.commit()
