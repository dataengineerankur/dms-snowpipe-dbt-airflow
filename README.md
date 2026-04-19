# SQL Server → Snowflake Migration Lab

End-to-end migration from SQL Server to Snowflake — four source databases (e-commerce, ERP, CRM, inventory), two ingestion paths (AWS DMS and Airbyte), a four-stage Snowflake medallion architecture, Airflow orchestration, dbt transformations, and a live migration dashboard.

This is a training lab. The goal is to understand every layer of a real migration, not just copy configs.

---

## What you're building

```
SQL Server (Docker local or RDS)
        │
        ├── via AWS DMS ────► S3 (Parquet) ──► Snowpipe ──► RAW_MSSQL.RAW_DMS_VARIANT
        │                                                              │
        └── via Airbyte ────────────────────────────────────► MSSQL_MIGRATION_LAB.AIRBYTE_RAW
                                                                       │
                                                        BRONZE (typed, MERGE dedup)
                                                                       │ Airflow DAG 2
                                                        SILVER (SCD Type-2 via dbt snapshot)
                                                                       │ Airflow DAG 3
                                                        GOLD (facts + dims via dbt run)
                                                                       │ Airflow DAG 4
                                                        dbt test (bronze + silver + gold)
```

DMS writes Parquet to S3 and Snowpipe auto-ingests. Airbyte connects directly to SQL Server and writes to Snowflake without touching S3. Both feed the same Bronze layer — you can run them side by side and compare counts.

---

## Repository layout

```
dms-snowpipe-dbt-airflow/
│
├── airflow/
│   ├── dags/
│   │   ├── mssql_01_ingest_bronze.py    DAG 1: RAW → Bronze MERGE for all 15 tables
│   │   ├── mssql_02_silver_snapshots.py DAG 2: dbt snapshot (SCD Type-2) for all tables
│   │   ├── mssql_03_gold_transforms.py  DAG 3: dbt run (stg → int → gold)
│   │   └── mssql_04_data_quality.py     DAG 4: dbt test (bronze + silver + gold)
│   ├── dashboard/
│   │   ├── app.py                       Live migration dashboard (Flask, port 8050)
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── dbt/
│   │   └── Dockerfile                   dbt runner image for Airflow
│   └── docker-compose.yml
│
├── airbyte/
│   ├── docker-compose.yml               Airbyte OSS 0.50.55 stack
│   ├── setup_connections.py             Bootstraps MSSQL → Snowflake via Airbyte API
│   ├── setup_s3_connection.py           Bootstraps MSSQL → S3 via Airbyte (no Snowflake needed)
│   └── compare_dms_vs_airbyte.py        Row count comparison: DMS Bronze vs Airbyte RAW
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── sources.yml
│       ├── customers/    stg → int → gold
│       ├── orders/
│       └── products/
│
├── infra/
│   ├── aws/              Terraform: DMS replication instance, S3, IAM, Step Functions
│   └── snowflake/        Terraform: warehouse, DB/schemas, Snowpipes, stages
│
├── scripts/              Helper scripts: bootstrap, CDC simulation, Snowpipe verify
├── pipelines/            Glue PySpark jobs, Snowflake SQL pipelines
├── .env.example          All required credentials (copy → .env, never commit .env)
└── README.md
```

---

## Prerequisites

- Docker Desktop
- AWS account with S3, DMS, IAM permissions (only needed for the DMS path)
- Snowflake account (trial works)
- Python 3.10+
- Terraform 1.5+
- dbt Core 1.7+: `pip install dbt-snowflake`

Copy `.env.example` to `.env` and fill in your values. Every script reads from there.

```bash
cp .env.example .env
```

---

## Step 1 — Start SQL Server locally

The lab uses four SQL Server databases to cover a realistic mixed workload: an e-commerce/stress DB (orders, customers, products), ERP (employees, payroll, departments), CRM (accounts, contacts, opportunities), and inventory (warehouses, SKUs, stock movements).

```bash
# Start SQL Server in Docker
docker run -e "ACCEPT_EULA=Y" \
           -e "SA_PASSWORD=${MSSQL_PASSWORD}" \
           -p 1433:1433 \
           --name sqlserver \
           -d mcr.microsoft.com/mssql/server:2019-latest
```

Wait ~30 seconds, then run the seed scripts from the `scripts/` directory or connect with any SQL client (Azure Data Studio, DBeaver, sqlcmd) and run the DDL + seed SQL files in order.

The source schema covers:
- `dbo.Customers`, `dbo.Products`, `dbo.Categories`, `dbo.Orders`, `dbo.OrderItems`
- `dbo.ERP_Departments`, `dbo.ERP_Employees`, `dbo.ERP_PayrollRuns`, `dbo.ERP_PayrollLines`
- `dbo.CRM_Accounts`, `dbo.CRM_Contacts`, `dbo.CRM_Opportunities`
- `dbo.INV_Warehouses`, `dbo.INV_SKU`, `dbo.INV_StockMovements`

---

## Step 2 — Choose your ingestion path

### Option A: AWS DMS → S3 → Snowpipe

DMS needs a resolvable endpoint — it runs inside AWS VPC and cannot reach `localhost`. Use RDS SQL Server or an EC2 instance.

```bash
# Deploy AWS infrastructure
cd infra/aws
cp terraform.tfvars.example terraform.tfvars   # fill in your values
terraform init && terraform apply
```

This creates a DMS replication instance, source endpoint (RDS SQL Server), target endpoint (S3), and a full-load task for all 15 tables. Start the task:

```bash
aws dms start-replication-task \
    --replication-task-arn $(terraform output -raw dms_task_arn) \
    --start-replication-task-type start-replication
```

Parquet files land in S3. Snowpipe (deployed via Snowflake Terraform) picks them up automatically and loads into `RAW_MSSQL.RAW_DMS_VARIANT`.

Verify:

```bash
./scripts/verify_dms.sh
# then in Snowflake:
# SELECT COUNT(*) FROM RAW_MSSQL.RAW_DMS_VARIANT;
```

### Option B: Airbyte → Snowflake or S3 (local SQL Server works)

Airbyte connects directly from Docker to your local SQL Server — no AWS needed. Two sub-options:

**Write to Snowflake directly:**

```bash
cd airbyte
docker compose up -d          # starts Airbyte OSS (UI at http://localhost:8000)
# wait ~2 minutes for Temporal to initialize
pip install requests python-dotenv
python setup_connections.py   # creates MSSQL source + Snowflake dest + triggers sync
```

Data lands in `MSSQL_MIGRATION_LAB.AIRBYTE_RAW.*`.

**Write to S3 (matches the DMS path, Snowpipe picks it up):**

```bash
python setup_s3_connection.py
# requires AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY + DMS_BUCKET in .env
```

Parquet files land at `s3://<bucket>/airbyte-raw/<table>/`. The Snowpipe stage can read them directly.

After either Airbyte run, compare counts with DMS:

```bash
python compare_dms_vs_airbyte.py
```

Airbyte counts should be >= Bronze counts — it keeps raw history while Bronze MERGE deduplicates by primary key.

---

## Step 3 — Set up Snowflake

```bash
cd infra/snowflake
cp terraform.tfvars.example terraform.tfvars
terraform init && terraform apply
```

Set these before running (Terraform reads them automatically):

```bash
export SNOWFLAKE_ORGANIZATION_NAME=your_org
export SNOWFLAKE_ACCOUNT_NAME=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
```

Terraform creates the `MSSQL_MIGRATION_LAB` database with schemas `RAW_MSSQL`, `BRONZE`, `SILVER`, `GOLD`, `AIRBYTE_RAW`, the warehouse, external stage, Snowpipe, and all DDL tables.

---

## Step 4 — Run the Airflow pipeline

```bash
cd airflow
docker compose up -d
# UI at http://localhost:8080  (admin / admin)
```

Four DAGs run in sequence:

### DAG 1: mssql_01_ingest_bronze

Reads `RAW_DMS_VARIANT` and MERGEs into all 15 Bronze typed tables. Key detail: uses `QUALIFY ROW_NUMBER() OVER (PARTITION BY <pk>) = 1` in every MERGE source subquery — `SELECT DISTINCT` alone can't deduplicate rows where computed columns like `CURRENT_TIMESTAMP()` differ between identical source rows.

Also resumes Snowflake Streams + Tasks (CDC equivalents for triggers) before MERGE and suspends them after, so they don't fire on partial data.

Tasks: `check_raw_counts` → `resume_sf_tasks` → parallel MERGE for all 4 source DBs → `suspend_sf_tasks` → `report_bronze_counts`

### DAG 2: mssql_02_silver_snapshots

Waits for DAG 1, then runs `dbt snapshot` for all 15 tables. Builds SCD Type-2 history in Silver — adds `dbt_valid_from`, `dbt_valid_to`, `dbt_updated_at`. Current record = `dbt_valid_to IS NULL`.

### DAG 3: mssql_03_gold_transforms

Waits for DAG 2, then runs `dbt run` for staging → intermediate → gold models. Produces `CUSTOMERS_DIM`, `ORDERS_FACT`, `PRODUCTS_DIM`.

### DAG 4: mssql_04_data_quality

Waits for DAG 3, then runs `dbt test` across all three layers. Expected: 27 PASS, 3 WARN.

### Running the pipeline manually

Trigger DAG 1 first. For DAGs 2-4, use the same `--exec-date` as DAG 1 — the `ExternalTaskSensor` matches by exact `execution_date`, not "most recent run":

```bash
# Get DAG 1's execution_date from Airflow UI, then:
airflow dags trigger mssql_02_silver_snapshots --exec-date "2026-04-19T15:27:15+00:00"
airflow dags trigger mssql_03_gold_transforms  --exec-date "2026-04-19T15:27:15+00:00"
airflow dags trigger mssql_04_data_quality     --exec-date "2026-04-19T15:27:15+00:00"
```

---

## Step 5 — Live migration dashboard

```bash
cd airflow/dashboard
docker compose up -d
# open http://localhost:8050
```

Shows DAG status cards, row counts per layer (RAW / Bronze / Silver / Gold), coverage progress bars, and a CDC events table with the last 50 rows landed in RAW. Refreshes every 30 seconds. POST to `/api/refresh` to force an immediate update.

---

## SQL Server object migration map

### Stored procedures

SQL Server stored procs became Snowflake Scripting SPs. The main translation challenges:

| Pattern | SQL Server | Snowflake |
|---|---|---|
| Dynamic SQL | `sp_executesql` | `EXECUTE IMMEDIATE` |
| JSON parsing | `OPENJSON` | `FLATTEN` on VARIANT |
| JSON output | `FOR JSON PATH` | `ARRAY_AGG + OBJECT_CONSTRUCT` |
| XML | `FOR XML PATH` / `XQuery` | Snowpark Python (no native XML type) |
| Table-valued params | `@tvp TABLE TYPE` | VARIANT array + FLATTEN |
| Cursors | `DECLARE CURSOR ... FETCH` | Removed; replaced with set-based UPDATE |
| Try/catch | `BEGIN TRY / BEGIN CATCH` | `EXCEPTION WHEN OTHER THEN` |
| Save points | `SAVE TRANSACTION` | Not supported — not ported |
| Temp tables across procs | Scoped temp tables | Not supported in Snowflake |
| Wait | `WAITFOR DELAY` | `SYSTEM$WAIT` |

Notable ports:

| SQL Server | Snowflake SP | Notes |
|---|---|---|
| `usp_RefreshOrderTotals` | `SP_REFRESH_ORDER_TOTALS` | Direct port |
| `usp_ListOpenOrders` | `SP_LIST_OPEN_ORDERS` | Returns RESULTSET |
| `usp_Stress_DynamicSearchOrders` | `SP_DYNAMIC_SEARCH_ORDERS` | `sp_executesql` → `EXECUTE IMMEDIATE` |
| `usp_Stress_MergeUpsertCustomers` | `SP_MERGE_UPSERT_CUSTOMERS` | `OPENJSON` → `FLATTEN` |
| `usp_Stress_XmlOrderDocument` | `SP_XML_ORDER_DOCUMENT` | Snowpark Python |
| `usp_Stress_RecursiveCategoryClosure` | `SP_BUILD_CATEGORY_CLOSURE` | `WITH RECURSIVE` — natively supported |
| `usp_Stress_SavePointPartialRollback` | Not ported | No `SAVE TRANSACTION` in Snowflake |
| `usp_Erp_ClosePayrollRun` | `SP_ERP_CLOSE_PAYROLL_RUN` | Direct port |
| `usp_Crm_MergeAccountsFromJson` | `SP_CRM_MERGE_ACCOUNTS_FROM_JSON` | `OPENJSON` → `FLATTEN` |

### Triggers → Streams + Tasks

Snowflake has no triggers. Audit/side-effect triggers became Streams + Tasks (async, fires within ~1 minute). Tasks are suspended by default; Airflow DAG 1 resumes them before MERGE and suspends after.

| SQL Server trigger | Snowflake stream | Task |
|---|---|---|
| `tr_Orders_Audit_IU` | `STREAM_ORDERS_CHANGES` | `TASK_ORDERS_AUDIT` |
| `tr_OrderItems_RecalcAndQueue` | `STREAM_ORDER_ITEMS_CHANGES` | `TASK_ORDER_ITEMS_RECALC` |
| `tr_Products_ListPriceAudit` | `STREAM_PRODUCTS_CHANGES` | `TASK_PRODUCTS_PRICE_AUDIT` |
| `tr_Employees_Audit` | `STREAM_ERP_EMPLOYEES_CHANGES` | `TASK_ERP_EMPLOYEES_AUDIT` |
| `tr_PayrollLines_Recalc` | `STREAM_ERP_PAYROLL_LINES_CHANGES` | `TASK_ERP_PAYROLL_RECALC` |
| `tr_Accounts_Activity` | `STREAM_CRM_ACCOUNTS_CHANGES` | `TASK_CRM_ACCOUNTS_ACTIVITY` |

INSTEAD OF triggers became stored procedures — callers must use the SP instead of writing directly:

| SQL Server | Snowflake SP |
|---|---|
| `tr_vw_Orders_Dml_IOD` (INSTEAD OF DELETE) | `SP_SOFT_DELETE_ORDER(order_id)` |
| `tr_vw_Orders_Dml_IOU` (INSTEAD OF UPDATE) | `SP_UPDATE_ORDER(order_id, ...)` |
| `tr_Opportunities_StageGuard` (AFTER UPDATE) | `SP_CRM_UPDATE_OPPORTUNITY_STAGE` |

### Constraints

Snowflake supports declaring constraints but does not enforce them at write time. Enforcement moves into stored procedures or MERGE logic.

| SQL Server | Snowflake | Enforcement |
|---|---|---|
| `CHECK (Quantity > 0)` | `NOT ENFORCED` | Inside `SP_TVP_APPEND_ORDER_LINES` |
| Computed column `LINE_TOTAL` | Stored `LINE_TOTAL NUMBER(18,4)` | Computed during Bronze MERGE |
| `UNIQUE` | `UNIQUE NOT ENFORCED` | MERGE dedup logic |
| Foreign keys | `FOREIGN KEY NOT ENFORCED` | Declared, not enforced |

---

## DMS vs Airbyte

| | AWS DMS | Airbyte |
|---|---|---|
| Works with local SQL Server | No — needs resolvable endpoint | Yes — connects to `host.docker.internal` |
| Intermediate storage | Parquet in S3 (durable archive) | None by default |
| Setup complexity | High (IAM, VPC, DMS instance, Snowpipe) | Low (`docker compose up`) |
| Cost | DMS instance runs 24/7 | Free OSS |
| CDC support | Yes | Yes (needs SQL Server Agent) |
| Connector coverage | AWS-native sources mainly | 300+ including SaaS APIs |

For local dev and testing, Airbyte is much faster to stand up. For production on AWS where you need a Parquet archive and tight AWS integration, DMS is the better fit.

---

## Credential security

`.env` is in `.gitignore` — never commit it. Verify:

```bash
git check-ignore -v .env
```

Rotate any secrets that were ever committed. For production, use AWS Secrets Manager or Vault instead of `.env` files.

---

## Smoke tests

After setup, run these in Snowflake to verify the migration:

```sql
SELECT COUNT(*) FROM BRONZE.CUSTOMERS;
SELECT COUNT(*) FROM BRONZE.ORDERS;

CALL BRONZE.SP_LIST_OPEN_ORDERS();
CALL BRONZE.SP_DYNAMIC_SEARCH_ORDERS('Open', NULL, NULL, 'ORDER_DATE', 'DESC');
SELECT BRONZE.FN_FORMAT_MONEY(12345.678);

-- Chained proc (appends [C][B][A] to notes)
CALL BRONZE.SP_CHAINED_A_OUTER(1);
SELECT NOTES FROM BRONZE.ORDERS WHERE ORDER_ID = 1;

-- Stream check
SELECT SYSTEM$STREAM_HAS_DATA('BRONZE.STREAM_ORDERS_CHANGES');

-- Task history
SELECT NAME, STATE, COMPLETED_TIME, ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
    TASK_NAME => 'TASK_ORDERS_AUDIT'
));
```

---

## Troubleshooting

**Bronze MERGE fails with `Duplicate row detected during DML action`**
Source subquery returns two rows with the same PK. The fix is `QUALIFY ROW_NUMBER() OVER (PARTITION BY <pk>) = 1` in the MERGE source — `SELECT DISTINCT` doesn't help when computed columns like `CURRENT_TIMESTAMP()` differ between rows.

**ExternalTaskSensor polls forever**
It matches by exact `execution_date`. Trigger downstream DAGs with `--exec-date` equal to the upstream DAG's execution_date shown in the Airflow UI.

**dbt fails with `--profiles-dir is not a valid option`**
Not a valid global flag in dbt 1.x. Set `ENV DBT_PROFILES_DIR=/opt/dbt` in the Dockerfile and use `ENTRYPOINT ["dbt"]` with no arguments.

**Airbyte can't reach `host.docker.internal`**
On Linux, add `extra_hosts: ["host.docker.internal:host-gateway"]` to the Airbyte worker service in `docker-compose.yml`.

**RAW_DMS_VARIANT empty after DMS full-load**
Check Snowpipe copy history: `SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME=>'RAW_DMS_VARIANT', START_TIME=>DATEADD('hour',-1,CURRENT_TIMESTAMP())))`. Usually the IAM trust between Snowflake and S3 is misconfigured — run `DESC STORAGE INTEGRATION` in Snowflake to get the IAM user ARN and external ID, then update the IAM role trust policy.

---

## PATCHIT integration

PATCHIT monitors this repo for failures across Airflow, Glue, and Snowflake. To ingest a failure:

```bash
# AWS Step Functions failure
python scripts/patchit/ingest_aws_failures_to_patchit.py \
  --state-machine-arn $(cd infra/aws && terraform output -raw step_function_arn) \
  --repo-key aws_dms

# Direct Glue failure
python scripts/patchit/ingest_aws_failures_to_patchit.py \
  --glue-job-name dms-glue-silver-orders \
  --repo-key aws_dms
```
