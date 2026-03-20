import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
spark.conf.set("spark.sql.adaptive.enabled", "true")
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

bucket = args["S3_BUCKET"]
raw_prefix = args["RAW_PREFIX"].rstrip("/")
silver_prefix = args["SILVER_PREFIX"].rstrip("/")

today = F.date_format(F.current_date(), "yyyyMMdd")


def read_raw(table):
    return spark.read.parquet(f"s3://{bucket}/{raw_prefix}/sales/{table}/")


def dedupe_latest(df, key_col, ts_cols):
    order_cols = [F.col(c).desc_nulls_last() for c in ts_cols if c in df.columns]
    if not order_cols:
        order_cols = [F.lit(0)]
    w = Window.partitionBy(key_col).orderBy(*order_cols)
    return df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")


op_col_orders = F.col("dms_op") if "dms_op" in orders.columns else F.col("Op")
orders = orders.filter((op_col_orders.isNull()) | (op_col_orders != "D"))
orders = dedupe_latest(orders, "order_id", ["dms_commit_ts", "DMS_COMMIT_TS", "updated_at", "order_date"])
orders = orders.withColumn("load_dt", today)

op_col_items = F.col("dms_op") if "dms_op" in order_items.columns else F.col("Op")
order_items = order_items.filter((op_col_items.isNull()) | (op_col_items != "D"))
order_items = dedupe_latest(
    order_items, "order_item_id", ["dms_commit_ts", "DMS_COMMIT_TS", "updated_at", "created_at"]
)
order_items = order_items.withColumn("line_total", F.col("quantity") * F.col("unit_price")).withColumn(
    "load_dt", today
)

orders.write.mode("overwrite").parquet(
    f"s3://{bucket}/{silver_prefix}/stg_orders/"
)
order_items.write.mode("overwrite").parquet(
    f"s3://{bucket}/{silver_prefix}/stg_order_items/"
)

job.commit()
