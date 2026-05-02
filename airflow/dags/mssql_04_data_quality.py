"""
DAG 4 of 4 — MSSQL Migration: Data Quality Tests
==================================================
SOURCE  : MSSQL_MIGRATION_LAB.GOLD.*  (Gold layer tables)
TARGET  : Airflow task logs + dbt test results

What it does
------------
Runs dbt tests on the Gold layer to catch data quality issues:
  - not_null tests on all primary keys
  - unique tests on dimension keys
  - relationship tests (FK integrity between facts and dims)
  - accepted_values tests on status/stage columns

If any test fails, the DAG task turns RED in Airflow and
you see exactly which table+column+row count failed.

Schedule: @daily, runs AFTER mssql_03_gold_transforms
"""
from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from dbt_utils import build_dbt_task, DEFAULT_ENV

DBT_ENV = {**DEFAULT_ENV, "SNOWFLAKE_DATABASE": "MSSQL_MIGRATION_LAB"}
ARGS = {"owner": "data-platform", "retries": 0, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="mssql_04_data_quality",
    default_args=ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["mssql-migration", "dbt", "data-quality", "tests"],
    doc_md=__doc__,
) as dag:

    wait_gold = ExternalTaskSensor(
        task_id="wait_for_gold_transforms",
        external_dag_id="mssql_03_gold_transforms",
        external_task_id="dbt_run_gold",
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    test_bronze = build_dbt_task("dbt_test_bronze", "dbt test --select source:bronze")
    test_silver = build_dbt_task("dbt_test_silver", "dbt test --select snp_customers snp_products snp_orders")
    test_gold   = build_dbt_task("dbt_test_gold",   "dbt test --select gold")

    for t in [test_bronze, test_silver, test_gold]:
        t.environment = DBT_ENV

    wait_gold >> test_bronze >> test_silver >> test_gold
