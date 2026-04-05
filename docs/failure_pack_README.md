# PATCHIT 150-Issue Failure Pack

This pack contains intentionally failing pipelines to stress-test PATCHIT remediation behavior.

## Contents
- `docs/patchit_issue_catalog_150.md` / `.json`: 150 real-world issue scenarios
- `pipelines/airflow/dags/patchit_failure_pack_100.py`: 100 failing Airflow DAGs
- `pipelines/aws_glue/jobs/*.py`: 25 failing Glue job scripts
- `pipelines/snowflake/sql/*.sql`: 25 failing Snowflake SQL scripts
- `scripts/patchit/deploy_failure_pack.py`: deployment helper (dry-run/live)

## Quick Start
```bash
python3 scripts/patchit/deploy_failure_pack.py --dry-run
```

## Live deploy examples
```bash
# Glue scripts upload
python3 scripts/patchit/deploy_failure_pack.py --live --glue-bucket <your-bucket>

# Snowflake script execution via snowsql connection
python3 scripts/patchit/deploy_failure_pack.py --live --snowflake-connection <snowsql-conn-name>
```

## Notes
- Airflow DAG file should be copied/synced into your Airflow `dags/` folder.
- These jobs are intentionally broken to validate RCA/fix behavior.
- Databricks scenarios are excluded per request.
