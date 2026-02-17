# Postgres → S3 (DMS) → Snowflake (Snowpipe) → dbt → Airflow

End-to-end data platform reference project that:

- Replicates Postgres to S3 (full load + CDC) using AWS DMS.
- Auto-ingests S3 parquet files into Snowflake RAW using Snowpipe.
- Transforms RAW → STG → INT → GOLD using dbt.
- Orchestrates dbt with Airflow (local Docker, optional ECS/Fargate mode).

## Architecture

```
             ┌───────────────────────────┐
             │     Postgres (source)     │
             │  sales.customers/products │
             │  sales.orders/order_items │
             └────────────┬──────────────┘
                          │ Full load + CDC
                          ▼
                ┌──────────────────┐
                │ AWS DMS Task     │
                └────────┬─────────┘
                         │ Parquet to S3
                         ▼
              ┌───────────────────────┐
              │ S3 bucket: /dms/...   │
              └─────────┬─────────────┘
                        │ Auto-ingest
                        ▼
             ┌──────────────────────────┐
             │ Snowflake RAW + Snowpipe │
             └─────────┬────────────────┘
                       │ dbt
                       ▼
              ┌──────────────────────┐
              │ STG → INT → GOLD     │
              └─────────┬────────────┘
                        │
                        ▼
                 ┌────────────┐
                 │ Airflow    │
                 │ dbt run/test│
                 └────────────┘
```

## Repository Structure

```
.
├── airflow/
│   ├── dags/
│   │   └── dbt_pipeline.py
│   ├── dbt/
│   │   └── Dockerfile
│   └── docker-compose.yml
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── sources.yml
│       ├── schema.yml
│       ├── stg/
│       ├── int/
│       └── gold/
├── infra/
│   ├── aws/
│   └── snowflake/
├── scripts/
│   ├── bootstrap_local.sh
│   ├── verify_dms.sh
│   ├── verify_snowpipe.sql
│   ├── backfill_snowpipe.sql
│   └── run_cdc.sh
├── docker-compose.postgres.yml
├── .env.example
├── LICENSE
└── README.md
```

## Defaults & Design Choices

- **S3 folder layout:** `s3://<bucket>/dms/<schema>/<table>/load_dt=YYYYMMDD/...`
  - DMS supports `YYYYMMDD` partitions. You can format to `YYYY-MM-DD` in Snowflake if needed.
- **File format:** Parquet (DMS target), compressed with GZIP.
- **RAW tables:** Typed columns + DMS metadata columns (`DMS_OP`, `DMS_COMMIT_TS`, `DMS_LOAD_TS`).
  - Chosen for performance and predictable dbt typing; schema evolution is handled with `ALTER TABLE` and dbt `on_schema_change`.

## Prerequisites

- Docker + Docker Compose
- Terraform >= 1.5
- AWS CLI configured (`AWS_PROFILE` or env vars)
- Snowflake credentials in env vars
- `snowsql` (optional, for running SQL scripts)

## Automated RDS + Seed (Steps 1-4)

This script provisions AWS resources (including RDS Postgres), then seeds data.

```
export AWS_PROFILE=default
export POSTGRES_PASSWORD=your_password
export POSTGRES_USER=app_user
export POSTGRES_DB=source_db
./scripts/bootstrap_rds_and_seed.sh
```

Make sure `infra/aws/terraform.tfvars` is filled in before running.

## Local Postgres (optional)

Start Postgres with seed data:

```
docker compose -f docker-compose.postgres.yml up -d
```

Simulate CDC:

```
./scripts/run_cdc.sh
```

## Terraform: AWS (DMS + S3 + IAM + CloudWatch)

```
cd infra/aws
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

Outputs include:

- DMS replication instance + task
- S3 bucket
- IAM role for DMS
- IAM role for Snowflake storage integration

## Terraform: Snowflake (DB + Schemas + Pipes)

```
cd infra/snowflake
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

Capture the outputs:

- `storage_aws_external_id`
- `storage_aws_iam_user_arn`
- `pipe_notification_channels`

Update AWS with Snowflake trust + Snowpipe SNS topics:

```
cd infra/aws
# update terraform.tfvars with:
# snowflake_aws_iam_user_arn, snowflake_external_id
# snowpipe_sns_topic_arns (from pipe_notification_channels)
terraform apply
```

## DMS → S3 Verification

```
export DMS_TASK_ID=<replication-task-id>
export DMS_BUCKET=<bucket-name>
./scripts/verify_dms.sh
```

You should see S3 objects under `dms/sales/<table>/load_dt=YYYYMMDD/`.

## Snowpipe Verification

Run the SQL in `scripts/verify_snowpipe.sql`:

```
snowsql -f scripts/verify_snowpipe.sql
```

Look for recent pipe usage and copy history.

## dbt: Local Run

```
export DBT_PROFILES_DIR=$(pwd)/dbt
export SNOWFLAKE_ACCOUNT=...
export SNOWFLAKE_USER=...
export SNOWFLAKE_PASSWORD=...
export SNOWFLAKE_ROLE=TRANSFORM_ROLE
export SNOWFLAKE_WAREHOUSE=TRANSFORM_WH
export SNOWFLAKE_DATABASE=ANALYTICS

cd dbt
dbt deps
dbt run --select gold
dbt test --select gold
```

## Airflow: Local Run (Docker)

Build the dbt runner image:

```
docker build -f airflow/dbt/Dockerfile -t dms-dbt-runner:latest .
```

Start Airflow:

```
cd airflow
docker compose up -d
```

Trigger the `dbt_pipeline` DAG in the UI (http://localhost:8080).

## Optional: ECS/Fargate Execution Mode

Set env vars before starting Airflow:

```
export DBT_EXECUTION_MODE=ecs
export ECS_CLUSTER_ARN=...
export ECS_TASK_DEFINITION_ARN=...
export ECS_SUBNETS=subnet-1,subnet-2
export ECS_SECURITY_GROUPS=sg-123
```

The DAG will use `EcsRunTaskOperator` with container name `dbt`.

## Operational Considerations

- **Schema evolution:** DMS captures DDL. Use `ALTER TABLE` in Snowflake when new columns appear; dbt models use `on_schema_change=append_new_columns` for incremental loads.
- **Deletes:** DMS emits `DMS_OP='D'`. STG models filter deletes to keep current-state dims/facts.
- **Late arriving CDC:** `fct_orders` uses a lookback window (`fct_orders_lookback_days`) in incremental merge.
- **Idempotency/backfills:** Use `ALTER PIPE <pipe> REFRESH` for Snowpipe, and rerun dbt. `fct_orders` is merge-based.
- **Cost controls:** Warehouses use autosuspend (60/120s). Adjust sizes in Snowflake Terraform vars.
- **Observability:** `gold.audit_ingestion` aggregates raw load stats; also use `INFORMATION_SCHEMA.COPY_HISTORY` and DMS CloudWatch logs.

## Common Failure Modes

- **No files in S3:** Check DMS task status and network access from DMS to Postgres.
- **Snowpipe not ingesting:** Ensure S3 bucket notifications point to each pipe SNS topic.
- **dbt failures:** Verify roles/warehouse grants for `TRANSFORM_ROLE`.
- **Airflow DockerOperator failing:** Ensure Docker socket is reachable (`docker-proxy` service).

## Backfill Procedure

1. Run `scripts/backfill_snowpipe.sql` in Snowflake.
2. Rerun `dbt run --select gold`.

## Notes on Security

- No secrets are stored in the repo.
- Use env vars or AWS Secrets Manager for Postgres credentials if needed.

## PATCHIT Test: AWS Step Functions + Glue Failure

Use this to verify PATCHIT can catch AWS failures and propose a fix against this repo.

1) Ensure PATCHIT has this repo registered with `repo_key=aws_dms` and mounted at:
   - `/opt/patchit/repo/dms-snowpipe-dbt-airflow`
2) Trigger a Step Function run (or run a Glue job directly) and wait for failure.
3) Run the relay script to ingest failure details and CloudWatch logs into PATCHIT.

Example:

```bash
cd /Users/ankurchopra/repo_projects/dms-snowpipe-dbt-airflow

# Optional: explicit profile/region
export AWS_PROFILE=default
export AWS_REGION=us-east-1

# Pull state machine ARN from terraform output
SM_ARN=$(cd infra/aws && terraform output -raw step_function_arn)

# Start a new execution (optional if you already started one)
aws stepfunctions start-execution --state-machine-arn "$SM_ARN" --input '{}'

# Ingest latest failed execution into PATCHIT
python scripts/patchit/ingest_aws_failures_to_patchit.py \
  --state-machine-arn "$SM_ARN" \
  --repo-key aws_dms \
  --patchit-ingest-url http://127.0.0.1:18088/events/ingest
```

If you want to ingest a specific failed execution:

```bash
python scripts/patchit/ingest_aws_failures_to_patchit.py \
  --execution-arn "arn:aws:states:...:execution:dms-datalake-orchestrator:..." \
  --repo-key aws_dms
```

If you want to ingest a direct Glue failure:

```bash
python scripts/patchit/ingest_aws_failures_to_patchit.py \
  --glue-job-name dms-glue-silver-orders \
  --repo-key aws_dms
```

## PATCHIT Test: Snowflake Failure (after AWS)

For Snowflake, ingest a failed task/query event to PATCHIT with `platform=snowflake` and `repo_key=snow`.
If you already run the PATCHIT Snowflake lab, continue using that ingest flow, then compare PR quality across AWS and Snowflake incidents.
