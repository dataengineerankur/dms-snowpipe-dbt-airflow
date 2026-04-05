from __future__ import annotations

import io
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", os.getenv("AWS_REGION", "us-east-1"))
BUCKET = os.getenv("RETAIL_S3_BUCKET", "dms-snowpipe-dev-05d6e64a")
RAW_PREFIX = os.getenv("RETAIL_RAW_PREFIX", "retail-live/raw/transactions")
BRONZE_PREFIX = os.getenv("RETAIL_BRONZE_PREFIX", "retail-live/bronze/transactions")
MANIFEST_KEY = f"{BRONZE_PREFIX}/_manifests/processed_keys.json"


def _list_keys(client, prefix: str):
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys


def _load_processed_keys(client):
    try:
        body = client.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)["Body"].read().decode("utf-8")
        return set(json.loads(body).get("processed_keys", []))
    except Exception:
        return set()


def _save_processed_keys(client, processed_keys):
    payload = json.dumps({"processed_keys": sorted(processed_keys), "updated_at": datetime.now(timezone.utc).isoformat()})
    client.put_object(Bucket=BUCKET, Key=MANIFEST_KEY, Body=payload.encode("utf-8"), ContentType="application/json")


def raw_to_bronze():
    s3 = boto3.client("s3", region_name=AWS_REGION)
    processed_keys = _load_processed_keys(s3)
    keys = [key for key in _list_keys(s3, RAW_PREFIX) if key.endswith(".json") and key not in processed_keys]
    if not keys:
        print(f"No new raw transaction files found under s3://{BUCKET}/{RAW_PREFIX}")
        return

    records = []
    for key in keys:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8")
        for line in body.splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            payload["raw_s3_key"] = key
            payload["bronze_loaded_at"] = datetime.now(timezone.utc).isoformat()
            records.append(payload)

    if not records:
        print("Raw files existed but contained no JSON records")
        return

    df = pd.json_normalize(records)
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    load_dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    target_key = f"{BRONZE_PREFIX}/load_dt={load_dt}/run_ts={run_ts}/transactions.parquet"

    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        df.to_parquet(tmp.name, index=False)
        s3.upload_file(tmp.name, BUCKET, target_key)

    processed_keys.update(keys)
    _save_processed_keys(s3, processed_keys)
    print(f"Wrote {len(df)} bronze rows to s3://{BUCKET}/{target_key}")


default_args = {
    "owner": "patchit",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="retail_raw_to_bronze",
    start_date=datetime(2024, 1, 1),
    schedule="*/2 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["retail", "streaming", "bronze"],
) as dag:
    PythonOperator(task_id="promote_raw_events_to_bronze", python_callable=raw_to_bronze)
