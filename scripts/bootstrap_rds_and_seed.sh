#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "${AWS_PROFILE:-}" ]]; then
  echo "Set AWS_PROFILE before running."
  exit 1
fi

if [[ -z "${POSTGRES_PASSWORD:-}" ]]; then
  echo "Set POSTGRES_PASSWORD (RDS master password) before running."
  exit 1
fi

echo "Applying AWS Terraform (includes RDS + DMS + S3)..."
terraform -chdir="${ROOT_DIR}/infra/aws" init
terraform -chdir="${ROOT_DIR}/infra/aws" apply -auto-approve

RDS_ENDPOINT="$(terraform -chdir="${ROOT_DIR}/infra/aws" output -raw rds_endpoint)"
RDS_PORT="$(terraform -chdir="${ROOT_DIR}/infra/aws" output -raw rds_port)"

if [[ -z "${RDS_ENDPOINT}" ]]; then
  echo "RDS endpoint not found. Check terraform outputs."
  exit 1
fi

echo "Seeding RDS schema and data..."
export PGPASSWORD="${POSTGRES_PASSWORD}"
psql -h "${RDS_ENDPOINT}" -p "${RDS_PORT}" -U "${POSTGRES_USER:-app_user}" -d "${POSTGRES_DB:-source_db}" -f "${ROOT_DIR}/scripts/postgres/01_schema.sql"
psql -h "${RDS_ENDPOINT}" -p "${RDS_PORT}" -U "${POSTGRES_USER:-app_user}" -d "${POSTGRES_DB:-source_db}" -f "${ROOT_DIR}/scripts/postgres/02_seed.sql"

echo "Done. RDS endpoint: ${RDS_ENDPOINT}"
