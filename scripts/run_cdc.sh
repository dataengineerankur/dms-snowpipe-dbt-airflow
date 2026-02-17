#!/usr/bin/env bash
set -euo pipefail

docker exec -i dms_postgres psql -U "${POSTGRES_USER:-app_user}" -d "${POSTGRES_DB:-source_db}" < "$(dirname "$0")/postgres/03_cdc_simulation.sql"
