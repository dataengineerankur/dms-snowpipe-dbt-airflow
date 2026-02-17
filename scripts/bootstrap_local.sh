#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Starting local Postgres..."
docker compose -f "${ROOT_DIR}/docker-compose.postgres.yml" up -d

echo "Building dbt runner image..."
docker build -f "${ROOT_DIR}/airflow/dbt/Dockerfile" -t dms-dbt-runner:latest "${ROOT_DIR}"

echo "Done."
