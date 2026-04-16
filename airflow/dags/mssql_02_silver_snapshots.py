"""
DAG 2 of 4 — MSSQL Migration: Silver Snapshots (SCD Type-2)
=============================================================
SOURCE  : MSSQL_MIGRATION_LAB.BRONZE.*  (typed Bronze tables)
TARGET  : MSSQL_MIGRATION_LAB.SILVER.*  (SCD2 history tables)

What it does
------------
Runs dbt snapshots to create and maintain SCD Type-2 history in SILVER.
Each snapshot adds dbt_valid_from / dbt_valid_to / dbt_updated_at columns
so you can query "what did a record look like on date X".

Snapshots run:
  snp_customers         BRONZE.CUSTOMERS     → SILVER.SNP_CUSTOMERS
  snp_products          BRONZE.PRODUCTS      → SILVER.SNP_PRODUCTS
  snp_orders            BRONZE.ORDERS        → SILVER.SNP_ORDERS
  snp_erp_employees     BRONZE.ERP_EMPLOYEES → SILVER.SNP_ERP_EMPLOYEES
  snp_crm_accounts      BRONZE.CRM_ACCOUNTS  → SILVER.SNP_CRM_ACCOUNTS
  snp_crm_opportunities BRONZE.CRM_OPPORTUNITIES → SILVER.SNP_CRM_OPPORTUNITIES
  snp_inv_sku           BRONZE.INV_SKU       → SILVER.SNP_INV_SKU

Schedule: @daily, runs AFTER mssql_01_ingest_bronze via ExternalTaskSensor
          (or trigger manually after DAG 1 completes)
"""
from __future__ import annotations
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from dbt_utils import build_dbt_task, DEFAULT_ENV

DBT_ENV = {**DEFAULT_ENV, "SNOWFLAKE_DATABASE": "MSSQL_MIGRATION_LAB"}
ARGS = {"owner": "data-platform", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="mssql_02_silver_snapshots",
    default_args=ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["mssql-migration", "silver", "scd2", "dbt"],
    doc_md=__doc__,
) as dag:

    # Wait for Bronze ingest to finish on the same day
    wait_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze_ingest",
        external_dag_id="mssql_01_ingest_bronze",
        external_task_id="report_bronze_counts",
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    # Run all 7 snapshots in one dbt snapshot call
    run_snapshots = build_dbt_task("dbt_snapshot_all", "dbt snapshot")
    run_snapshots.environment = DBT_ENV

    wait_bronze >> run_snapshots
