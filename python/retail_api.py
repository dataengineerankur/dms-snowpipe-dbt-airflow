import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
from fastapi import FastAPI, HTTPException
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import NewTopic
from pydantic import BaseModel, Field

BASE_DIR = Path(__file__).resolve().parents[1]
CATALOG_PATH = BASE_DIR / "shared" / "retail_catalog.json"
RUNTIME_DIR = BASE_DIR / "runtime"
RUNTIME_DIR.mkdir(exist_ok=True)
STATUS_PATH = RUNTIME_DIR / "streaming_status.json"
SUBMITTED_PATH = RUNTIME_DIR / "submitted_transactions.jsonl"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("RETAIL_KAFKA_TOPIC", "retail.transactions.raw")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", os.getenv("AWS_REGION", "us-east-1"))
RAW_BUCKET = os.getenv("RETAIL_S3_BUCKET", "dms-snowpipe-dev-05d6e64a")
RAW_PREFIX = os.getenv("RETAIL_RAW_PREFIX", "retail-live/raw/transactions")
BRONZE_PREFIX = os.getenv("RETAIL_BRONZE_PREFIX", "retail-live/bronze/transactions")
STREAMLIT_URL = os.getenv("RETAIL_UI_URL", "http://localhost:8502")
CONSOLE_URL = os.getenv("REDPANDA_CONSOLE_URL", "http://localhost:8081")

app = FastAPI(title="Retail Live Transactions API")


class CartItem(BaseModel):
    sku: str
    quantity: int = Field(ge=1, le=20)


class TransactionRequest(BaseModel):
    customer_name: str = Field(min_length=2, max_length=100)
    customer_email: Optional[str] = None
    store_id: str = "walmart-supercenter-4521"
    payment_method: str = "card"
    items: List[CartItem]


class RetailProduct(BaseModel):
    sku: str
    name: str
    category: str
    price: float
    inventory: int
    image: str


_products_cache: List[Dict[str, Any]] = []
_producer: Optional[KafkaProducer] = None


def load_catalog() -> List[Dict[str, Any]]:
    global _products_cache
    if not _products_cache:
        _products_cache = json.loads(CATALOG_PATH.read_text())
    return _products_cache


def get_product_map() -> Dict[str, Dict[str, Any]]:
    return {p["sku"]: p for p in load_catalog()}


def get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda value: value.encode("utf-8"),
            retries=5,
            linger_ms=50,
        )
    return _producer


def ensure_topic(retries: int = 15, delay_seconds: int = 4) -> None:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="retail-api-admin")
            try:
                existing = set(admin.list_topics())
                if KAFKA_TOPIC not in existing:
                    admin.create_topics([NewTopic(name=KAFKA_TOPIC, num_partitions=3, replication_factor=1)])
                return
            finally:
                admin.close()
        except Exception as exc:
            last_error = exc
            time.sleep(delay_seconds)
    raise RuntimeError(f"Kafka topic bootstrap failed after {retries} attempts: {last_error}")


def append_local_transaction(record: Dict[str, Any]) -> None:
    with SUBMITTED_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")


def s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def count_s3_objects(prefix: str) -> Dict[str, Any]:
    client = s3_client()
    paginator = client.get_paginator("list_objects_v2")
    total = 0
    latest_key = None
    latest_modified = None
    for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            total += 1
            modified = obj["LastModified"].isoformat()
            if latest_modified is None or modified > latest_modified:
                latest_modified = modified
                latest_key = obj["Key"]
    return {"bucket": RAW_BUCKET, "prefix": prefix, "object_count": total, "latest_key": latest_key}


def kafka_topic_snapshot() -> Dict[str, Any]:
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP, enable_auto_commit=False)
    try:
        partitions = consumer.partitions_for_topic(KAFKA_TOPIC) or set()
        topic_partitions = [TopicPartition(KAFKA_TOPIC, p) for p in partitions]
        end_offsets = consumer.end_offsets(topic_partitions) if topic_partitions else {}
        total_messages = sum(end_offsets.values()) if end_offsets else 0
        return {
            "topic": KAFKA_TOPIC,
            "partitions": len(partitions),
            "estimated_messages": total_messages,
        }
    finally:
        consumer.close()


def read_stream_status() -> Dict[str, Any]:
    if not STATUS_PATH.exists():
        return {"state": "starting", "details": "Spark status file not written yet."}
    try:
        return json.loads(STATUS_PATH.read_text())
    except json.JSONDecodeError:
        return {"state": "unknown", "details": "Spark status file is unreadable."}


def recent_transactions(limit: int = 10) -> List[Dict[str, Any]]:
    if not SUBMITTED_PATH.exists():
        return []
    lines = SUBMITTED_PATH.read_text().splitlines()[-limit:]
    records = []
    for line in reversed(lines):
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


def build_transaction(payload: TransactionRequest) -> Dict[str, Any]:
    catalog = get_product_map()
    enriched_items = []
    subtotal = 0.0
    for item in payload.items:
        product = catalog.get(item.sku)
        if not product:
            raise HTTPException(status_code=404, detail=f"Unknown SKU: {item.sku}")
        line_total = round(product["price"] * item.quantity, 2)
        subtotal += line_total
        enriched_items.append(
            {
                "sku": item.sku,
                "name": product["name"],
                "category": product["category"],
                "unit_price": product["price"],
                "quantity": item.quantity,
                "line_total": line_total,
            }
        )
    tax = round(subtotal * 0.0825, 2)
    total = round(subtotal + tax, 2)
    return {
        "event_type": "checkout_completed",
        "schema_version": "1.0.0",
        "order_id": f"WM-{uuid.uuid4().hex[:12].upper()}",
        "submitted_at": datetime.now(timezone.utc).isoformat(),
        "customer_name": payload.customer_name,
        "customer_email": payload.customer_email,
        "store_id": payload.store_id,
        "payment_method": payload.payment_method,
        "currency": "USD",
        "item_count": sum(item.quantity for item in payload.items),
        "subtotal": round(subtotal, 2),
        "tax": tax,
        "total": total,
        "items": enriched_items,
    }


@app.on_event("startup")
def startup() -> None:
    ensure_topic()
    RUNTIME_DIR.mkdir(exist_ok=True)
    if not STATUS_PATH.exists():
        STATUS_PATH.write_text(json.dumps({"state": "starting", "updated_at": datetime.now(timezone.utc).isoformat()}))


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"status": "ok", "topic": KAFKA_TOPIC, "bucket": RAW_BUCKET}


@app.get("/retail/products")
def products() -> List[RetailProduct]:
    return load_catalog()


@app.post("/retail/transactions")
def create_transaction(payload: TransactionRequest) -> Dict[str, Any]:
    transaction = build_transaction(payload)
    future = get_producer().send(KAFKA_TOPIC, key=transaction["order_id"], value=transaction)
    future.get(timeout=15)
    append_local_transaction(transaction)
    return {
        "status": "accepted",
        "order_id": transaction["order_id"],
        "topic": KAFKA_TOPIC,
        "bucket": RAW_BUCKET,
        "raw_prefix": RAW_PREFIX,
        "transaction": transaction,
    }


@app.get("/retail/monitor/overview")
def overview() -> Dict[str, Any]:
    return {
        "ui_url": STREAMLIT_URL,
        "console_url": CONSOLE_URL,
        "bucket": RAW_BUCKET,
        "raw_prefix": RAW_PREFIX,
        "bronze_prefix": BRONZE_PREFIX,
        "recent_transactions": recent_transactions(),
    }


@app.get("/retail/monitor/kafka")
def kafka_monitor() -> Dict[str, Any]:
    snapshot = kafka_topic_snapshot()
    snapshot["updated_at"] = datetime.now(timezone.utc).isoformat()
    return snapshot


@app.get("/retail/monitor/storage")
def storage_monitor() -> Dict[str, Any]:
    return {
        "raw": count_s3_objects(RAW_PREFIX),
        "bronze": count_s3_objects(BRONZE_PREFIX),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/retail/monitor/streaming")
def streaming_monitor() -> Dict[str, Any]:
    status = read_stream_status()
    status["updated_at"] = datetime.now(timezone.utc).isoformat()
    return status
