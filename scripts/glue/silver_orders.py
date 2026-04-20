import sys
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
        "S3_BUCKET",
        "RAW_PREFIX",
        "SILVER_PREFIX",
    ],
)

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

bucket = args["S3_BUCKET"]
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

orders = read_raw("orders")
order_items = read_raw("order_items")
op_col_orders = F.col("dms_op") if "dms_op" in orders.columns else F.col("Op")
orders = orders.filter((op_col_orders.isNull()) | (op_col_orders != "D"))
orders = dedupe_latest(orders, "order_id", ["dms_commit_ts", "DMS_COMMIT_TS", "updated_at", "order_date"])
orders = orders.withColumn("load_dt", today)
run_pydeequ_checks(orders, "silver_orders", ["order_id", "customer_id"], key_col="order_id", non_negative_cols=["total_amount"])

op_col_items = F.col("dms_op") if "dms_op" in order_items.columns else F.col("Op")
order_items = order_items.filter((op_col_items.isNull()) | (op_col_items != "D"))
order_items = dedupe_latest(
    order_items, "order_item_id", ["dms_commit_ts", "DMS_COMMIT_TS", "updated_at", "created_at"]
)
order_items = order_items.withColumn("line_total", F.col("quantity") * F.col("unit_price")).withColumn(
    "load_dt", today
)
run_pydeequ_checks(order_items, "silver_order_items", ["order_item_id", "order_id"], key_col="order_item_id", non_negative_cols=["quantity", "unit_price", "line_total"])

orders.write.mode("overwrite").parquet(
    f"s3://{bucket}/{silver_prefix}/orders/"
)
order_items.write.mode("overwrite").parquet(
    f"s3://{bucket}/{silver_prefix}/order_items/"
)

job.commit()