import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType, StructField, StructType

RUNTIME_DIR = Path("/opt/retail/runtime")
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
STATUS_PATH = RUNTIME_DIR / "streaming_status.json"
CHECKPOINT_DIR = "/opt/retail/runtime/checkpoints/retail_raw_to_s3"
TOPIC = os.getenv("RETAIL_KAFKA_TOPIC", "retail.transactions.raw")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
BUCKET = os.getenv("RETAIL_S3_BUCKET", "dms-snowpipe-dev-05d6e64a")
RAW_PREFIX = os.getenv("RETAIL_RAW_PREFIX", "retail-live/raw/transactions")
RAW_TARGET = f"s3a://{BUCKET}/{RAW_PREFIX}"
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", os.getenv("AWS_REGION", "us-east-1"))
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN", "")

schema = StructType(
    [
        StructField("event_type", StringType()),
        StructField("schema_version", StringType()),
        StructField("order_id", StringType()),
        StructField("submitted_at", StringType()),
        StructField("customer_name", StringType()),
        StructField("customer_email", StringType()),
        StructField("store_id", StringType()),
        StructField("payment_method", StringType()),
        StructField("currency", StringType()),
        StructField("item_count", IntegerType()),
        StructField("subtotal", DoubleType()),
        StructField("tax", DoubleType()),
        StructField("total", DoubleType()),
        StructField(
            "items",
            ArrayType(
                StructType(
                    [
                        StructField("sku", StringType()),
                        StructField("name", StringType()),
                        StructField("category", StringType()),
                        StructField("unit_price", DoubleType()),
                        StructField("quantity", IntegerType()),
                        StructField("line_total", DoubleType()),
                    ]
                )
            ),
        ),
    ]
)


def write_status(state: str, **details):
    payload = {"state": state, "updated_at": datetime.now(timezone.utc).isoformat(), **details}
    STATUS_PATH.write_text(json.dumps(payload, indent=2))


def wait_for_kafka(max_attempts: int = 30, delay_seconds: int = 5):
    import socket

    host, port = BOOTSTRAP.split(":", 1)
    last_error = None
    for _ in range(max_attempts):
        try:
            with socket.create_connection((host, int(port)), timeout=5):
                return
        except Exception as exc:
            last_error = exc
            write_status("waiting_for_kafka", error=str(exc), target=RAW_TARGET)
            time.sleep(delay_seconds)
    raise RuntimeError(f"Kafka bootstrap failed: {last_error}")


spark = (
    SparkSession.builder.appName("retail-live-transactions")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
    .config("spark.hadoop.fs.s3a.path.style.access", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

if AWS_SESSION_TOKEN:
    spark.conf.set("spark.hadoop.fs.s3a.session.token", AWS_SESSION_TOKEN)
    spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")

write_status("starting", topic=TOPIC, target=RAW_TARGET)
wait_for_kafka()

source_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

parsed_df = (
    source_df.selectExpr("CAST(value AS STRING) AS raw_json", "topic", "partition", "offset", "timestamp AS kafka_timestamp")
    .withColumn("payload", from_json(col("raw_json"), schema))
    .select("raw_json", "topic", "partition", "offset", "kafka_timestamp", "payload.*")
    .withColumn("spark_ingested_at", current_timestamp())
)


def write_batch(batch_df, batch_id: int):
    count = batch_df.count()
    write_status("running", batch_id=batch_id, batch_count=count, target=RAW_TARGET)
    if count == 0:
        return
    (
        batch_df.write.mode("append").json(RAW_TARGET)
    )
    write_status("running", batch_id=batch_id, batch_count=count, last_successful_batch=batch_id, target=RAW_TARGET)


query = (
    parsed_df.writeStream.foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime="10 seconds")
    .start()
)

write_status("running", query_id=query.id, topic=TOPIC, target=RAW_TARGET)
query.awaitTermination()
