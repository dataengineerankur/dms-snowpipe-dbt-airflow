import os
from urllib.parse import unquote_plus

import boto3

s3 = boto3.client("s3")

SOURCE_BUCKET = os.environ.get("SOURCE_BUCKET", "")
SOURCE_PREFIX = os.environ.get("SOURCE_PREFIX", "")
TARGET_BUCKET = os.environ.get("TARGET_BUCKET", "")
TARGET_PREFIX = os.environ.get("TARGET_PREFIX", "glue/raw")


def _normalize_prefix(prefix: str) -> str:
    if not prefix:
        return ""
    return prefix if prefix.endswith("/") else f"{prefix}/"


def _build_target_key(source_key: str) -> str:
    source_prefix = _normalize_prefix(SOURCE_PREFIX)
    target_prefix = _normalize_prefix(TARGET_PREFIX)
    if source_prefix and source_key.startswith(source_prefix):
        suffix = source_key[len(source_prefix) :]
    else:
        suffix = source_key
    return f"{target_prefix}{suffix}"


def lambda_handler(event, context):
    if not SOURCE_BUCKET or not TARGET_BUCKET:
        raise ValueError("SOURCE_BUCKET and TARGET_BUCKET must be set.")

    for record in event.get("Records", []):
        if record.get("eventSource") != "aws:s3":
            continue
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        if bucket != SOURCE_BUCKET:
            continue
        if SOURCE_PREFIX and not key.startswith(_normalize_prefix(SOURCE_PREFIX)):
            continue

        target_key = _build_target_key(key)
        s3.copy_object(
            Bucket=TARGET_BUCKET,
            Key=target_key,
            CopySource={"Bucket": bucket, "Key": key},
        )

    return {"status": "ok"}
