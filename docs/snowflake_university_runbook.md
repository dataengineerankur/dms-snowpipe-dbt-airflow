# Snowflake University Runbook

## Visual guide
- For senior-level explanations (why each feature exists, cost optimization, backend behavior, and extra real-world patterns), read `docs/snowflake_university_deep_dive.md`.
- Open `docs/snowflake_university_visual_guide.html` in a browser for the animated side-by-side learning view.
- Use `docs/snowflake_iceberg_lab.md` for the Iceberg extension and `scripts/11_iceberg_s3_lab.sql` as the starter SQL.

## Current scale behavior
- `scripts/00_create_foundation_dataset.sql` is currently hard-coded to full scale.
- Current defaults inside the script are `ORDERS_TARGET_ROWS=50000000`, `EVENTS_TARGET_ROWS=100000000`, `PRODUCTS_TARGET_ROWS=200000`, `CUSTOMERS_TARGET_ROWS=500000`, `BATCH_SIZE=1000000`.
- If you want a smaller dev run, edit those `SET` statements in `scripts/00_create_foundation_dataset.sql` before executing it.

## Manual concept walkthrough
1. Run `scripts/00_create_foundation_dataset.sql`.
2. Run `scripts/01_transient_tables.sql`.
3. Run `scripts/02_streams.sql` once to create the stream objects and inspect baseline behavior.
4. Run `scripts/02_streams_trigger_changes.sql` to generate fresh CDC events after the stream exists.
5. Run `scripts/02_streams_consume.sql` to load CDC rows into audit and silver tables and advance the stream offset.
6. Run `scripts/03_tasks.sql` to deploy and resume the task graph.
7. Optionally run `EXECUTE TASK AUDIT_DB.PUBLIC.ROOT_TASK_INGEST;` to force an immediate task-graph run.
8. Run `scripts/04_dynamic_tables.sql`.
9. Run `scripts/05_materialized_views.sql`.
10. Run `PYTHONPATH=$(pwd) .venv-snowflake/bin/python snowpark/snowpark_pipelines.py`.
11. Run `cd dbt && DBT_PROFILES_DIR=. ../.venv-snowflake/bin/dbt snapshot --select snp_products snp_customers snp_orders_status`.
12. Run `scripts/08_trigger_snapshot_changes.sql`.
13. Run `cd dbt && DBT_PROFILES_DIR=. ../.venv-snowflake/bin/dbt snapshot --select snp_products snp_customers snp_orders_status` again.
14. Run `cd dbt && DBT_PROFILES_DIR=. ../.venv-snowflake/bin/dbt run --select university+ orders_with_status_history`.
15. Run `scripts/09_clustering_benchmark.sql`.
16. Run `scripts/10_internals_deep_dive.sql`.
17. Run `scripts/12_external_tables_and_lake_patterns.sql` (requires `S3_DMS_INT` and DMS files under `dms/sales/orders/`; see script header).
18. Run `scripts/11_iceberg_s3_lab.sql` (see `docs/snowflake_iceberg_lab.md`).

## Airflow end-to-end path
1. Ensure `scripts/02_streams.sql` has already been run at least once so the stream exists.
2. Run `scripts/02_streams_trigger_changes.sql` immediately before triggering the DAG.
3. Trigger `airflow dags trigger snowflake_master_pipeline`.
4. Confirm the DAG takes the `consume_stream_delta` branch instead of `skip_pipeline`.
5. Use `task_dag_pipeline` if you want Airflow to deploy the task graph and explicitly execute the root task.

## What to observe
- Streams: `SYSTEM$STREAM_HAS_DATA`, `SHOW STREAMS`, and rows landing in `AUDIT_DB.PUBLIC.ORDERS_AUDIT`.
- Tasks: `INFORMATION_SCHEMA.TASK_HISTORY` and `AUDIT_DB.PUBLIC.PIPELINE_RUNS`.
- Dynamic tables: target lag, refresh mode, and dependency graph.
- Materialized views: query profile, bytes scanned, and refresh history.
- Snapshots: history rows and SCD2 validity windows.
- External tables: `SHOW EXTERNAL TABLES IN SCHEMA EXT_DEMO.PUBLIC`, `LIST @EXT_DEMO.PUBLIC.DMS_ORDERS_EXT`, compare Query Profile to native `ANALYTICS.RAW.ORDERS` if present.

## Local validation notes
- The repo Python files compile successfully.
- Local `dbt` now works through the repo venv at `.venv-snowflake`.
- Local Airflow now works through `airflow/docker-compose.yml`, and the repaired Snowflake University DAGs can be triggered from the Dockerized Airflow UI.
