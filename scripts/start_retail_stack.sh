#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-$(aws configure get aws_access_key_id)}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-$(aws configure get aws_secret_access_key)}"
export AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-$(aws configure get aws_session_token || true)}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-${AWS_REGION:-$(aws configure get region || echo us-east-1)}}"
export RETAIL_S3_BUCKET="${RETAIL_S3_BUCKET:-dms-snowpipe-dev-05d6e64a}"
export RETAIL_RAW_PREFIX="${RETAIL_RAW_PREFIX:-retail-live/raw/transactions}"
export RETAIL_BRONZE_PREFIX="${RETAIL_BRONZE_PREFIX:-retail-live/bronze/transactions}"

if [[ -z "$AWS_ACCESS_KEY_ID" || -z "$AWS_SECRET_ACCESS_KEY" ]]; then
  echo "Missing AWS CLI credentials. Configure aws cli first."
  exit 1
fi

aws s3api head-bucket --bucket "$RETAIL_S3_BUCKET" >/dev/null
mkdir -p runtime
printf '{"state":"bootstrapping"}\n' > runtime/streaming_status.json

if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon is not running. Start Docker Desktop (or your Docker engine) and rerun this script."
  exit 1
fi

docker compose -f docker-compose.retail.yml up -d --remove-orphans

echo "UI: http://localhost:8502"
echo "API: http://localhost:8001/docs"
echo "Airflow: http://localhost:8080"
echo "Kafka console: http://localhost:8081"
echo "Raw data: s3://$RETAIL_S3_BUCKET/$RETAIL_RAW_PREFIX"
echo "Bronze data: s3://$RETAIL_S3_BUCKET/$RETAIL_BRONZE_PREFIX"
