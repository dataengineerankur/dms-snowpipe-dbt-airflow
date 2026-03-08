# Databricks Lakehouse Pipeline

This module creates a medallion-style pipeline in Databricks using Auto Loader:

- Bronze: ingest DMS parquet from `s3://<bucket>/dms/sales/...` to Delta.
- Silver: CDC merge/upsert (and delete handling) to curated Delta tables.
- Gold: BI-ready serving tables.

## Notebooks

- `notebooks/bronze_autoloader.py`
- `notebooks/silver_transform.py`
- `notebooks/gold_transform.py`

## Workflows

`workflows/workflows.yml` contains 3 workflows (same pattern as Airflow):

- `customers_pipeline`
- `products_pipeline`
- `orders_pipeline`

Each workflow has 3 tasks:

1. Bronze
2. Silver
3. Gold

## Prerequisites

1. Cluster runtime:
   - Databricks Runtime 13.3+ (includes Delta + Auto Loader).
2. IAM access from Databricks to S3 bucket `dms-snowpipe-dev-05d6e64a`.
   - Configure AWS credentials in Databricks secrets:
     ```bash
     databricks secrets create-scope aws
     databricks secrets put-secret aws access_key_id
     databricks secrets put-secret aws secret_access_key
     ```
   - Alternatively, configure the cluster with an instance profile that has S3 access.
3. Repo imported into Databricks Repos.
4. Provide `cluster_id` when deploying bundle.

## Deploy with Databricks Bundle (`databricks.yml`)

From `databricks/`:

```bash
databricks bundle validate
databricks bundle deploy -t dev --var "cluster_id=<your-cluster-id>"
```

Run workflows:

```bash
databricks bundle run customers_pipeline -t dev --var "cluster_id=<your-cluster-id>"
databricks bundle run products_pipeline -t dev --var "cluster_id=<your-cluster-id>"
databricks bundle run orders_pipeline -t dev --var "cluster_id=<your-cluster-id>"
```

`DATABRICKS_HOST` and auth (`DATABRICKS_TOKEN` or CLI profile) are read from your environment.

## Deploy with Terraform (`infra/databricks`)

```bash
cd /Users/ankurchopra/repo_projects/dms-snowpipe-dbt-airflow/infra/databricks
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars with your workspace/token/cluster/repo details
terraform init
terraform apply
```

## Run One by One (first run)

### 1) Customers pipeline

Run notebook `bronze_autoloader.py` with:

- `domain=customers`
- `catalog=hive_metastore`
- `schema=walmart_lakehouse`
- `bucket=dms-snowpipe-dev-05d6e64a`

Then run `silver_transform.py` with same params, then `gold_transform.py` with:

- `domain=customers`
- `catalog=hive_metastore`
- `schema=walmart_lakehouse`

### 2) Products pipeline

Repeat with `domain=products`.

### 3) Orders pipeline

Repeat with `domain=orders` (handles both `orders` and `order_items`).

## Validate Delta Tables

Use Databricks SQL / notebook SQL:

```sql
-- Bronze
SELECT count(*) FROM hive_metastore.walmart_lakehouse.bronze_customers;
SELECT count(*) FROM hive_metastore.walmart_lakehouse.bronze_orders;
SELECT count(*) FROM hive_metastore.walmart_lakehouse.bronze_order_items;
SELECT count(*) FROM hive_metastore.walmart_lakehouse.bronze_products;

-- Silver
SELECT * FROM hive_metastore.walmart_lakehouse.silver_customers WHERE customer_id = 2;
SELECT * FROM hive_metastore.walmart_lakehouse.silver_orders ORDER BY dms_commit_ts DESC LIMIT 20;

-- Gold
SELECT * FROM hive_metastore.walmart_lakehouse.gold_dim_customers WHERE customer_id = 2;
SELECT * FROM hive_metastore.walmart_lakehouse.gold_fct_orders ORDER BY record_updated_at DESC LIMIT 20;
```

## Recommended Storage Pattern

Yes, your proposed approach is good:

- Bronze + Silver as Delta on S3 (external) for openness and interoperability.
- Gold as managed Delta tables for governance/performance.

This is exactly how these notebooks are implemented:

- Bronze/Silver use S3 paths under:
  - `s3://<bucket>/databricks/bronze/...`
  - `s3://<bucket>/databricks/silver/...`
- Gold uses managed metastore tables (`saveAsTable` / merge by table name).
