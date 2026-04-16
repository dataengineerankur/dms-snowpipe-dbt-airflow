"""
DAG 3 of 4 — MSSQL Migration: Gold Transforms (dbt models)
============================================================
SOURCE  : MSSQL_MIGRATION_LAB.SILVER.*  (SCD2 snapshot tables)
          MSSQL_MIGRATION_LAB.BRONZE.*  (reference / append-only tables)
TARGET  : MSSQL_MIGRATION_LAB.GOLD.*   (BI-ready tables & views)

What it does  (3 sequential dbt layers)
----------------------------------------
Layer 1  dbt_run_stg   Staging views — thin rename + type cast over BRONZE
  stg_customers        BRONZE.CUSTOMERS      → STG.STG_CUSTOMERS (view)
  stg_products         BRONZE.PRODUCTS       → STG.STG_PRODUCTS (view)
  stg_orders           BRONZE.ORDERS         → STG.STG_ORDERS (view)
  stg_order_items      BRONZE.ORDER_ITEMS    → STG.STG_ORDER_ITEMS (view)
  stg_erp_*            BRONZE.ERP_*          → STG.STG_ERP_* (views)
  stg_crm_*            BRONZE.CRM_*          → STG.STG_CRM_* (views)
  stg_inv_*            BRONZE.INV_*          → STG.STG_INV_* (views)

Layer 2  dbt_run_int   Intermediate views — current records from SILVER + enrichment
  int_customers        SILVER.SNP_CUSTOMERS  → INT.INT_CUSTOMERS (view, current only)
  int_products         SILVER.SNP_PRODUCTS   → INT.INT_PRODUCTS (view)
  int_orders           SILVER + STG          → INT.INT_ORDERS (view, enriched)
  int_order_items      STG + INT             → INT.INT_ORDER_ITEMS (incremental table)
  int_orders_enriched  INT.INT_ORDERS        → INT.INT_ORDERS_ENRICHED (view + derived cols)
  int_erp_employees    SILVER.SNP_ERP_*      → INT.INT_ERP_EMPLOYEES (view)
  int_erp_payroll_lines STG + INT            → INT.INT_ERP_PAYROLL_LINES (incremental)
  int_crm_*            SILVER.SNP_CRM_*      → INT.INT_CRM_* (views)
  int_inv_*            STG + SNP             → INT.INT_INV_* (incremental)

Layer 3  dbt_run_gold  Gold tables & views — BI ready
  dim_customers        INT.INT_CUSTOMERS     → GOLD.DIM_CUSTOMERS (table)
  dim_products         INT.INT_PRODUCTS      → GOLD.DIM_PRODUCTS (table)
  dim_date             Snowflake GENERATOR   → GOLD.DIM_DATE (table, 2020-2034)
  fct_orders           INT.INT_ORDERS_ENRICHED → GOLD.FCT_ORDERS (incremental)
  fct_order_items      INT.INT_ORDER_ITEMS   → GOLD.FCT_ORDER_ITEMS (incremental)
  fct_payroll_summary  INT.INT_ERP_PAYROLL   → GOLD.FCT_PAYROLL_SUMMARY (incremental)
  fct_crm_pipeline     INT.INT_CRM_*         → GOLD.FCT_CRM_PIPELINE (incremental)
  fct_inventory_position INT.INT_INV_*       → GOLD.FCT_INVENTORY_POSITION (incremental)
  rpt_order_line_detail    INT views         → GOLD.RPT_ORDER_LINE_DETAIL (view)
  rpt_customer_order_totals INT views        → GOLD.RPT_CUSTOMER_ORDER_TOTALS (table)
  rpt_open_orders          INT views         → GOLD.RPT_OPEN_ORDERS (view)
  rpt_category_closure     BRONZE.CATEGORIES → GOLD.RPT_CATEGORY_CLOSURE (table)
  rpt_stock_orders         INT + SNP         → GOLD.RPT_STOCK_ORDERS (view)
  audit_ingestion          BRONZE.*          → GOLD.AUDIT_INGESTION (table)

Schedule: @daily, runs AFTER mssql_02_silver_snapshots
"""
from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from dbt_utils import build_dbt_task, DEFAULT_ENV

DBT_ENV = {**DEFAULT_ENV, "SNOWFLAKE_DATABASE": "MSSQL_MIGRATION_LAB"}
ARGS = {"owner": "data-platform", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="mssql_03_gold_transforms",
    default_args=ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["mssql-migration", "gold", "dbt", "transforms"],
    doc_md=__doc__,
) as dag:

    wait_silver = ExternalTaskSensor(
        task_id="wait_for_silver_snapshots",
        external_dag_id="mssql_02_silver_snapshots",
        external_task_id="dbt_snapshot_all",
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    dbt_stg  = build_dbt_task("dbt_run_stg",  "dbt run --select stg")
    dbt_int  = build_dbt_task("dbt_run_int",  "dbt run --select int")
    dbt_gold = build_dbt_task("dbt_run_gold", "dbt run --select gold")

    for t in [dbt_stg, dbt_int, dbt_gold]:
        t.environment = DBT_ENV

    wait_silver >> dbt_stg >> dbt_int >> dbt_gold
