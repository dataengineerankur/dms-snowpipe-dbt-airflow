"""
Airflow DAG: MSSQL → S3 (DMS) → Snowflake Bronze → Silver (dbt snapshots) → Gold (dbt models)

Pipeline stages
───────────────
  1. check_bronze_tables    — SELECT COUNT(*) on every Bronze table to verify data landed
  2. flatten_bronze         — MERGE from RAW_DMS_VARIANT → typed BRONZE tables (idempotent)
  3. dbt_snapshot           — dbt snapshot  (writes SCD2 history to SILVER)
  4. dbt_run_stg            — dbt run --select stg  (staging views over BRONZE)
  5. dbt_run_int            — dbt run --select int  (intermediate views over SILVER)
  6. dbt_run_gold           — dbt run --select gold (Gold tables + reports)
  7. dbt_test               — dbt test --select gold (data quality gate)
  8. report_row_counts      — row count summary across all layers

Connections / variables
───────────────────────
  Airflow connection:  snowflake_default
    Account  : SNOWFLAKE_ACCOUNT env var
    User     : SNOWFLAKE_USER env var
    Password : SNOWFLAKE_PASSWORD env var
    Role     : SNOWFLAKE_ROLE env var (default ACCOUNTADMIN)
    Warehouse: SNOWFLAKE_WAREHOUSE env var (default COMPUTE_WH)
    Database : MSSQL_MIGRATION_LAB (hardcoded — migration project DB)

  dbt runs via DockerOperator using dms-dbt-runner:latest image.
  Build command:
    cd sqlserver-to-snowflake-migration
    docker build -t dms-dbt-runner:latest -f dbt/Dockerfile dbt/
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from dbt_utils import build_dbt_task, DEFAULT_ENV

# ── Constants ─────────────────────────────────────────────────────────────────
SNOWFLAKE_CONN_ID = "snowflake_default"
DB  = "MSSQL_MIGRATION_LAB"
RAW = "RAW_MSSQL"
BRZ = "BRONZE"
WH  = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
ROLE = "ACCOUNTADMIN"

# dbt Docker env override — use migration DB, not the default ANALYTICS
DBT_ENV = {**DEFAULT_ENV, "SNOWFLAKE_DATABASE": DB}

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


# ─────────────────────────────────────────────────────────────────────────────
# SQL: Bronze health check (all source tables)
# ─────────────────────────────────────────────────────────────────────────────
CHECK_BRONZE_SQL = f"""
SELECT
    'CUSTOMERS'          AS table_name, COUNT(*) AS row_count FROM {DB}.{BRZ}.CUSTOMERS UNION ALL
SELECT 'PRODUCTS',        COUNT(*) FROM {DB}.{BRZ}.PRODUCTS UNION ALL
SELECT 'CATEGORIES',      COUNT(*) FROM {DB}.{BRZ}.CATEGORIES UNION ALL
SELECT 'ORDERS',          COUNT(*) FROM {DB}.{BRZ}.ORDERS UNION ALL
SELECT 'ORDER_ITEMS',     COUNT(*) FROM {DB}.{BRZ}.ORDER_ITEMS UNION ALL
SELECT 'ERP_DEPARTMENTS', COUNT(*) FROM {DB}.{BRZ}.ERP_DEPARTMENTS UNION ALL
SELECT 'ERP_EMPLOYEES',   COUNT(*) FROM {DB}.{BRZ}.ERP_EMPLOYEES UNION ALL
SELECT 'ERP_PAYROLL_RUNS',COUNT(*) FROM {DB}.{BRZ}.ERP_PAYROLL_RUNS UNION ALL
SELECT 'ERP_PAYROLL_LINES',COUNT(*) FROM {DB}.{BRZ}.ERP_PAYROLL_LINES UNION ALL
SELECT 'CRM_ACCOUNTS',    COUNT(*) FROM {DB}.{BRZ}.CRM_ACCOUNTS UNION ALL
SELECT 'CRM_CONTACTS',    COUNT(*) FROM {DB}.{BRZ}.CRM_CONTACTS UNION ALL
SELECT 'CRM_OPPORTUNITIES',COUNT(*) FROM {DB}.{BRZ}.CRM_OPPORTUNITIES UNION ALL
SELECT 'INV_WAREHOUSES',  COUNT(*) FROM {DB}.{BRZ}.INV_WAREHOUSES UNION ALL
SELECT 'INV_SKU',         COUNT(*) FROM {DB}.{BRZ}.INV_SKU UNION ALL
SELECT 'INV_STOCK_MOVEMENTS', COUNT(*) FROM {DB}.{BRZ}.INV_STOCK_MOVEMENTS
ORDER BY table_name;
"""

# ─────────────────────────────────────────────────────────────────────────────
# SQL: Bronze flatten (idempotent MERGE from RAW VARIANT)
# ─────────────────────────────────────────────────────────────────────────────
# DMS Full Load Parquet uses original SQL Server column names (PascalCase).
# For full loads: _DMS_OPERATION = 'I', _DMS_COMMIT_TS = NULL (no CDC ts).
# Uses MERGE so re-runs are safe (no data duplication).
# ─────────────────────────────────────────────────────────────────────────────
FLATTEN_BRONZE_SQL = f"""
USE DATABASE {DB};
USE WAREHOUSE {WH};

-- ── CUSTOMERS ──────────────────────────────────────────────
MERGE INTO {BRZ}.CUSTOMERS tgt
USING (
    SELECT DISTINCT
        v:CustomerId::NUMBER         AS CUSTOMER_ID,
        v:CustomerCode::VARCHAR(20)  AS CUSTOMER_CODE,
        v:FullName::VARCHAR(200)     AS FULL_NAME,
        v:Email::VARCHAR(320)        AS EMAIL,
        v:Country::VARCHAR(100)      AS COUNTRY,
        v:CreatedAt::TIMESTAMP_NTZ   AS CREATED_AT,
        'I'                          AS _DMS_OPERATION,
        NULL::TIMESTAMP_NTZ          AS _DMS_COMMIT_TS,
        CURRENT_TIMESTAMP()          AS _LOADED_AT,
        'SnowConvertStressDB'        AS _SOURCE_DB
    FROM {RAW}.RAW_DMS_VARIANT
    WHERE v:CustomerId IS NOT NULL
) src ON tgt.CUSTOMER_ID = src.CUSTOMER_ID AND tgt._SOURCE_DB = src._SOURCE_DB
WHEN MATCHED AND src._LOADED_AT > tgt._LOADED_AT THEN UPDATE SET
    CUSTOMER_CODE = src.CUSTOMER_CODE, FULL_NAME = src.FULL_NAME,
    EMAIL = src.EMAIL, COUNTRY = src.COUNTRY,
    _DMS_COMMIT_TS = src._DMS_COMMIT_TS, _LOADED_AT = src._LOADED_AT
WHEN NOT MATCHED THEN INSERT
    (CUSTOMER_ID, CUSTOMER_CODE, FULL_NAME, EMAIL, COUNTRY, CREATED_AT,
     _DMS_OPERATION, _DMS_COMMIT_TS, _LOADED_AT, _SOURCE_DB)
VALUES (src.CUSTOMER_ID, src.CUSTOMER_CODE, src.FULL_NAME, src.EMAIL,
        src.COUNTRY, src.CREATED_AT, src._DMS_OPERATION, src._DMS_COMMIT_TS,
        src._LOADED_AT, src._SOURCE_DB);

-- ── CATEGORIES ─────────────────────────────────────────────
MERGE INTO {BRZ}.CATEGORIES tgt
USING (
    SELECT DISTINCT
        v:CategoryId::NUMBER         AS CATEGORY_ID,
        v:CategoryName::VARCHAR(100) AS CATEGORY_NAME,
        v:Description::VARCHAR(500)  AS DESCRIPTION,
        'I'                          AS _DMS_OPERATION,
        NULL::TIMESTAMP_NTZ          AS _DMS_COMMIT_TS,
        CURRENT_TIMESTAMP()          AS _LOADED_AT,
        'SnowConvertStressDB'        AS _SOURCE_DB
    FROM {RAW}.RAW_DMS_VARIANT
    WHERE v:CategoryId IS NOT NULL AND v:CategoryName IS NOT NULL
) src ON tgt.CATEGORY_ID = src.CATEGORY_ID AND tgt._SOURCE_DB = src._SOURCE_DB
WHEN NOT MATCHED THEN INSERT
    (CATEGORY_ID, CATEGORY_NAME, DESCRIPTION, _DMS_OPERATION, _DMS_COMMIT_TS, _LOADED_AT, _SOURCE_DB)
VALUES (src.CATEGORY_ID, src.CATEGORY_NAME, src.DESCRIPTION,
        src._DMS_OPERATION, src._DMS_COMMIT_TS, src._LOADED_AT, src._SOURCE_DB);

-- ── PRODUCTS ───────────────────────────────────────────────
MERGE INTO {BRZ}.PRODUCTS tgt
USING (
    SELECT DISTINCT
        v:ProductId::NUMBER          AS PRODUCT_ID,
        v:CategoryId::NUMBER         AS CATEGORY_ID,
        v:SKU::VARCHAR(50)           AS SKU,
        v:ProductName::VARCHAR(200)  AS PRODUCT_NAME,
        v:ListPrice::NUMBER(18,4)    AS LIST_PRICE,
        v:IsActive::BOOLEAN          AS IS_ACTIVE,
        'I'                          AS _DMS_OPERATION,
        NULL::TIMESTAMP_NTZ          AS _DMS_COMMIT_TS,
        CURRENT_TIMESTAMP()          AS _LOADED_AT,
        'SnowConvertStressDB'        AS _SOURCE_DB
    FROM {RAW}.RAW_DMS_VARIANT
    WHERE v:ProductId IS NOT NULL AND v:SKU IS NOT NULL
) src ON tgt.PRODUCT_ID = src.PRODUCT_ID AND tgt._SOURCE_DB = src._SOURCE_DB
WHEN MATCHED AND src._LOADED_AT > tgt._LOADED_AT THEN UPDATE SET
    LIST_PRICE = src.LIST_PRICE, IS_ACTIVE = src.IS_ACTIVE,
    PRODUCT_NAME = src.PRODUCT_NAME, _LOADED_AT = src._LOADED_AT
WHEN NOT MATCHED THEN INSERT
    (PRODUCT_ID, CATEGORY_ID, SKU, PRODUCT_NAME, LIST_PRICE, IS_ACTIVE,
     _DMS_OPERATION, _DMS_COMMIT_TS, _LOADED_AT, _SOURCE_DB)
VALUES (src.PRODUCT_ID, src.CATEGORY_ID, src.SKU, src.PRODUCT_NAME,
        src.LIST_PRICE, src.IS_ACTIVE, src._DMS_OPERATION, src._DMS_COMMIT_TS,
        src._LOADED_AT, src._SOURCE_DB);

-- ── ORDERS ─────────────────────────────────────────────────
MERGE INTO {BRZ}.ORDERS tgt
USING (
    SELECT DISTINCT
        v:OrderId::NUMBER            AS ORDER_ID,
        v:CustomerId::NUMBER         AS CUSTOMER_ID,
        v:OrderDate::DATE            AS ORDER_DATE,
        v:Status::VARCHAR(30)        AS STATUS,
        v:TotalAmount::NUMBER(18,4)  AS TOTAL_AMOUNT,
        v:Notes::VARCHAR(500)        AS NOTES,
        'I'                          AS _DMS_OPERATION,
        NULL::TIMESTAMP_NTZ          AS _DMS_COMMIT_TS,
        CURRENT_TIMESTAMP()          AS _LOADED_AT,
        'SnowConvertStressDB'        AS _SOURCE_DB
    FROM {RAW}.RAW_DMS_VARIANT
    WHERE v:OrderId IS NOT NULL AND v:TotalAmount IS NOT NULL
) src ON tgt.ORDER_ID = src.ORDER_ID AND tgt._SOURCE_DB = src._SOURCE_DB
WHEN MATCHED AND src._LOADED_AT > tgt._LOADED_AT THEN UPDATE SET
    STATUS = src.STATUS, TOTAL_AMOUNT = src.TOTAL_AMOUNT,
    NOTES = src.NOTES, _LOADED_AT = src._LOADED_AT
WHEN NOT MATCHED THEN INSERT
    (ORDER_ID, CUSTOMER_ID, ORDER_DATE, STATUS, TOTAL_AMOUNT, NOTES,
     _DMS_OPERATION, _DMS_COMMIT_TS, _LOADED_AT, _SOURCE_DB)
VALUES (src.ORDER_ID, src.CUSTOMER_ID, src.ORDER_DATE, src.STATUS,
        src.TOTAL_AMOUNT, src.NOTES, src._DMS_OPERATION, src._DMS_COMMIT_TS,
        src._LOADED_AT, src._SOURCE_DB);

-- ── ORDER_ITEMS ────────────────────────────────────────────
MERGE INTO {BRZ}.ORDER_ITEMS tgt
USING (
    SELECT DISTINCT
        v:OrderItemId::NUMBER        AS ORDER_ITEM_ID,
        v:OrderId::NUMBER            AS ORDER_ID,
        v:ProductId::NUMBER          AS PRODUCT_ID,
        v:Quantity::NUMBER           AS QUANTITY,
        v:UnitPrice::NUMBER(18,4)    AS UNIT_PRICE,
        COALESCE(v:LineTotal::NUMBER(18,4),
                 v:Quantity::NUMBER * v:UnitPrice::NUMBER(18,4)) AS LINE_TOTAL,
        'I'                          AS _DMS_OPERATION,
        NULL::TIMESTAMP_NTZ          AS _DMS_COMMIT_TS,
        CURRENT_TIMESTAMP()          AS _LOADED_AT,
        'SnowConvertStressDB'        AS _SOURCE_DB
    FROM {RAW}.RAW_DMS_VARIANT
    WHERE v:OrderItemId IS NOT NULL
) src ON tgt.ORDER_ITEM_ID = src.ORDER_ITEM_ID AND tgt._SOURCE_DB = src._SOURCE_DB
WHEN NOT MATCHED THEN INSERT
    (ORDER_ITEM_ID, ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, LINE_TOTAL,
     _DMS_OPERATION, _DMS_COMMIT_TS, _LOADED_AT, _SOURCE_DB)
VALUES (src.ORDER_ITEM_ID, src.ORDER_ID, src.PRODUCT_ID, src.QUANTITY,
        src.UNIT_PRICE, src.LINE_TOTAL, src._DMS_OPERATION, src._DMS_COMMIT_TS,
        src._LOADED_AT, src._SOURCE_DB);

-- ── ERP_DEPARTMENTS ────────────────────────────────────────
MERGE INTO {BRZ}.ERP_DEPARTMENTS tgt
USING (
    SELECT DISTINCT
        v:DeptId::NUMBER             AS DEPT_ID,
        v:DeptCode::VARCHAR(20)      AS DEPT_CODE,
        v:DeptName::VARCHAR(120)     AS DEPT_NAME,
        v:BudgetUsd::NUMBER(18,2)    AS BUDGET_USD,
        'I' AS _DMS_OPERATION, NULL::TIMESTAMP_NTZ AS _DMS_COMMIT_TS,
        CURRENT_TIMESTAMP() AS _LOADED_AT, 'LabERP_DB' AS _SOURCE_DB
    FROM {RAW}.RAW_DMS_VARIANT
    WHERE v:DeptId IS NOT NULL AND v:DeptCode IS NOT NULL
) src ON tgt.DEPT_ID = src.DEPT_ID AND tgt._SOURCE_DB = src._SOURCE_DB
WHEN NOT MATCHED THEN INSERT
    (DEPT_ID, DEPT_CODE, DEPT_NAME, BUDGET_USD, _DMS_OPERATION, _DMS_COMMIT_TS, _LOADED_AT, _SOURCE_DB)
VALUES (src.DEPT_ID, src.DEPT_CODE, src.DEPT_NAME, src.BUDGET_USD,
        src._DMS_OPERATION, src._DMS_COMMIT_TS, src._LOADED_AT, src._SOURCE_DB);

-- ── ERP_EMPLOYEES ──────────────────────────────────────────
MERGE INTO {BRZ}.ERP_EMPLOYEES tgt
USING (
    SELECT DISTINCT
        v:EmployeeId::NUMBER         AS EMP_ID,
        v:DeptId::NUMBER             AS DEPT_ID,
        v:EmpCode::VARCHAR(20)       AS EMP_CODE,
        v:FullName::VARCHAR(200)     AS FULL_NAME,
        v:HireDate::DATE             AS HIRE_DATE,
        v:Salary::NUMBER(18,4)       AS SALARY,
        'I' AS _DMS_OPERATION, NULL::TIMESTAMP_NTZ AS _DMS_COMMIT_TS,
        CURRENT_TIMESTAMP() AS _LOADED_AT, 'LabERP_DB' AS _SOURCE_DB
    FROM {RAW}.RAW_DMS_VARIANT
    WHERE v:EmployeeId IS NOT NULL AND v:EmpCode IS NOT NULL
) src ON tgt.EMP_ID = src.EMP_ID AND tgt._SOURCE_DB = src._SOURCE_DB
WHEN MATCHED AND src._LOADED_AT > tgt._LOADED_AT THEN UPDATE SET
    SALARY = src.SALARY, DEPT_ID = src.DEPT_ID, FULL_NAME = src.FULL_NAME,
    _LOADED_AT = src._LOADED_AT
WHEN NOT MATCHED THEN INSERT
    (EMP_ID, DEPT_ID, EMP_CODE, FULL_NAME, HIRE_DATE, SALARY,
     _DMS_OPERATION, _DMS_COMMIT_TS, _LOADED_AT, _SOURCE_DB)
VALUES (src.EMP_ID, src.DEPT_ID, src.EMP_CODE, src.FULL_NAME, src.HIRE_DATE,
        src.SALARY, src._DMS_OPERATION, src._DMS_COMMIT_TS, src._LOADED_AT, src._SOURCE_DB);

-- ── ERP_PAYROLL_RUNS ───────────────────────────────────────
MERGE INTO {BRZ}.ERP_PAYROLL_RUNS tgt
USING (
    SELECT DISTINCT
        v:RunId::NUMBER              AS RUN_ID,
        v:RunMonth::VARCHAR(7)       AS RUN_MONTH,
        v:Status::VARCHAR(20)        AS STATUS,
        v:CreatedAt::TIMESTAMP_NTZ   AS CREATED_AT,
        'I' AS _DMS_OPERATION, NULL::TIMESTAMP_NTZ AS _DMS_COMMIT_TS,
        CURRENT_TIMESTAMP() AS _LOADED_AT, 'LabERP_DB' AS _SOURCE_DB
    FROM {RAW}.RAW_DMS_VARIANT
    WHERE v:RunId IS NOT NULL AND v:RunMonth IS NOT NULL
) src ON tgt.RUN_ID = src.RUN_ID AND tgt._SOURCE_DB = src._SOURCE_DB
WHEN MATCHED THEN UPDATE SET STATUS = src.STATUS, _LOADED_AT = src._LOADED_AT
WHEN NOT MATCHED THEN INSERT
    (RUN_ID, RUN_MONTH, STATUS, CREATED_AT, _DMS_OPERATION, _DMS_COMMIT_TS, _LOADED_AT, _SOURCE_DB)
VALUES (src.RUN_ID, src.RUN_MONTH, src.STATUS, src.CREATED_AT,
        src._DMS_OPERATION, src._DMS_COMMIT_TS, src._LOADED_AT, src._SOURCE_DB);

-- ── ERP_PAYROLL_LINES ──────────────────────────────────────
MERGE INTO {BRZ}.ERP_PAYROLL_LINES tgt
USING (
    SELECT DISTINCT
        v:LineId::NUMBER             AS LINE_ID,
        v:RunId::NUMBER              AS RUN_ID,
        v:EmployeeId::NUMBER         AS EMP_ID,
        v:GrossPay::NUMBER(18,4)     AS GROSS_PAY,
        COALESCE(v:NetPay::NUMBER(18,4),
                 v:GrossPay::NUMBER(18,4) * 0.92)  AS NET_PAY,
        'I' AS _DMS_OPERATION, NULL::TIMESTAMP_NTZ AS _DMS_COMMIT_TS,
        CURRENT_TIMESTAMP() AS _LOADED_AT, 'LabERP_DB' AS _SOURCE_DB
    FROM {RAW}.RAW_DMS_VARIANT
    WHERE v:LineId IS NOT NULL AND v:RunId IS NOT NULL
) src ON tgt.LINE_ID = src.LINE_ID AND tgt._SOURCE_DB = src._SOURCE_DB
WHEN NOT MATCHED THEN INSERT
    (LINE_ID, RUN_ID, EMP_ID, GROSS_PAY, NET_PAY, _DMS_OPERATION, _DMS_COMMIT_TS, _LOADED_AT, _SOURCE_DB)
VALUES (src.LINE_ID, src.RUN_ID, src.EMP_ID, src.GROSS_PAY, src.NET_PAY,
        src._DMS_OPERATION, src._DMS_COMMIT_TS, src._LOADED_AT, src._SOURCE_DB);

-- ── CRM_ACCOUNTS ───────────────────────────────────────────
MERGE INTO {BRZ}.CRM_ACCOUNTS tgt
USING (
    SELECT DISTINCT
        v:AccountId::NUMBER          AS ACCOUNT_ID,
        v:AccountCode::VARCHAR(30)   AS ACCOUNT_CODE,
        v:Name::VARCHAR(200)         AS NAME,
        v:Region::VARCHAR(50)        AS REGION,
        v:CreatedAt::TIMESTAMP_NTZ   AS CREATED_AT,
        'I' AS _DMS_OPERATION, NULL::TIMESTAMP_NTZ AS _DMS_COMMIT_TS,
        CURRENT_TIMESTAMP() AS _LOADED_AT, 'LabCRM_DB' AS _SOURCE_DB
    FROM {RAW}.RAW_DMS_VARIANT
    WHERE v:AccountId IS NOT NULL AND v:AccountCode IS NOT NULL
) src ON tgt.ACCOUNT_ID = src.ACCOUNT_ID AND tgt._SOURCE_DB = src._SOURCE_DB
WHEN MATCHED AND src._LOADED_AT > tgt._LOADED_AT THEN UPDATE SET
    NAME = src.NAME, REGION = src.REGION, _LOADED_AT = src._LOADED_AT
WHEN NOT MATCHED THEN INSERT
    (ACCOUNT_ID, ACCOUNT_CODE, NAME, REGION, CREATED_AT,
     _DMS_OPERATION, _DMS_COMMIT_TS, _LOADED_AT, _SOURCE_DB)
VALUES (src.ACCOUNT_ID, src.ACCOUNT_CODE, src.NAME, src.REGION, src.CREATED_AT,
        src._DMS_OPERATION, src._DMS_COMMIT_TS, src._LOADED_AT, src._SOURCE_DB);

-- ── CRM_CONTACTS ───────────────────────────────────────────
MERGE INTO {BRZ}.CRM_CONTACTS tgt
USING (
    SELECT DISTINCT
        v:ContactId::NUMBER          AS CONTACT_ID,
        v:AccountId::NUMBER          AS ACCOUNT_ID,
        v:Email::VARCHAR(320)        AS EMAIL,
        v:FullName::VARCHAR(200)     AS FULL_NAME,
        v:IsPrimary::BOOLEAN         AS IS_PRIMARY,
        'I' AS _DMS_OPERATION, NULL::TIMESTAMP_NTZ AS _DMS_COMMIT_TS,
        CURRENT_TIMESTAMP() AS _LOADED_AT, 'LabCRM_DB' AS _SOURCE_DB
    FROM {RAW}.RAW_DMS_VARIANT
    WHERE v:ContactId IS NOT NULL AND v:Email IS NOT NULL
) src ON tgt.CONTACT_ID = src.CONTACT_ID AND tgt._SOURCE_DB = src._SOURCE_DB
WHEN NOT MATCHED THEN INSERT
    (CONTACT_ID, ACCOUNT_ID, EMAIL, FULL_NAME, IS_PRIMARY,
     _DMS_OPERATION, _DMS_COMMIT_TS, _LOADED_AT, _SOURCE_DB)
VALUES (src.CONTACT_ID, src.ACCOUNT_ID, src.EMAIL, src.FULL_NAME, src.IS_PRIMARY,
        src._DMS_OPERATION, src._DMS_COMMIT_TS, src._LOADED_AT, src._SOURCE_DB);

-- ── CRM_OPPORTUNITIES ──────────────────────────────────────
MERGE INTO {BRZ}.CRM_OPPORTUNITIES tgt
USING (
    SELECT DISTINCT
        v:OppId::NUMBER              AS OPP_ID,
        v:AccountId::NUMBER          AS ACCOUNT_ID,
        v:Title::VARCHAR(200)        AS TITLE,
        v:Stage::VARCHAR(40)         AS STAGE,
        v:AmountUsd::NUMBER(18,2)    AS AMOUNT_USD,
        v:CloseDate::DATE            AS CLOSE_DATE,
        'I' AS _DMS_OPERATION, NULL::TIMESTAMP_NTZ AS _DMS_COMMIT_TS,
        CURRENT_TIMESTAMP() AS _LOADED_AT, 'LabCRM_DB' AS _SOURCE_DB
    FROM {RAW}.RAW_DMS_VARIANT
    WHERE v:OppId IS NOT NULL AND v:AccountId IS NOT NULL
) src ON tgt.OPP_ID = src.OPP_ID AND tgt._SOURCE_DB = src._SOURCE_DB
WHEN MATCHED AND src._LOADED_AT > tgt._LOADED_AT THEN UPDATE SET
    STAGE = src.STAGE, AMOUNT_USD = src.AMOUNT_USD, CLOSE_DATE = src.CLOSE_DATE,
    _LOADED_AT = src._LOADED_AT
WHEN NOT MATCHED THEN INSERT
    (OPP_ID, ACCOUNT_ID, TITLE, STAGE, AMOUNT_USD, CLOSE_DATE,
     _DMS_OPERATION, _DMS_COMMIT_TS, _LOADED_AT, _SOURCE_DB)
VALUES (src.OPP_ID, src.ACCOUNT_ID, src.TITLE, src.STAGE, src.AMOUNT_USD,
        src.CLOSE_DATE, src._DMS_OPERATION, src._DMS_COMMIT_TS, src._LOADED_AT, src._SOURCE_DB);

-- ── INV_WAREHOUSES ─────────────────────────────────────────
MERGE INTO {BRZ}.INV_WAREHOUSES tgt
USING (
    SELECT DISTINCT
        v:WhId::NUMBER               AS WH_ID,
        v:WhCode::VARCHAR(20)        AS WH_CODE,
        v:Location::VARCHAR(200)     AS LOCATION,
        'I' AS _DMS_OPERATION, NULL::TIMESTAMP_NTZ AS _DMS_COMMIT_TS,
        CURRENT_TIMESTAMP() AS _LOADED_AT, 'LabInventory_DB' AS _SOURCE_DB
    FROM {RAW}.RAW_DMS_VARIANT
    WHERE v:WhId IS NOT NULL AND v:WhCode IS NOT NULL
) src ON tgt.WH_ID = src.WH_ID AND tgt._SOURCE_DB = src._SOURCE_DB
WHEN NOT MATCHED THEN INSERT
    (WH_ID, WH_CODE, LOCATION, _DMS_OPERATION, _DMS_COMMIT_TS, _LOADED_AT, _SOURCE_DB)
VALUES (src.WH_ID, src.WH_CODE, src.LOCATION,
        src._DMS_OPERATION, src._DMS_COMMIT_TS, src._LOADED_AT, src._SOURCE_DB);

-- ── INV_SKU ────────────────────────────────────────────────
MERGE INTO {BRZ}.INV_SKU tgt
USING (
    SELECT DISTINCT
        v:SkuId::NUMBER              AS SKU_ID,
        v:SkuCode::VARCHAR(40)       AS SKU_CODE,
        v:Descr::VARCHAR(200)        AS DESCR,
        v:UnitCost::NUMBER(18,4)     AS UNIT_COST,
        'I' AS _DMS_OPERATION, NULL::TIMESTAMP_NTZ AS _DMS_COMMIT_TS,
        CURRENT_TIMESTAMP() AS _LOADED_AT, 'LabInventory_DB' AS _SOURCE_DB
    FROM {RAW}.RAW_DMS_VARIANT
    WHERE v:SkuId IS NOT NULL AND v:SkuCode IS NOT NULL
) src ON tgt.SKU_ID = src.SKU_ID AND tgt._SOURCE_DB = src._SOURCE_DB
WHEN MATCHED AND src._LOADED_AT > tgt._LOADED_AT THEN UPDATE SET
    DESCR = src.DESCR, UNIT_COST = src.UNIT_COST, _LOADED_AT = src._LOADED_AT
WHEN NOT MATCHED THEN INSERT
    (SKU_ID, SKU_CODE, DESCR, UNIT_COST, _DMS_OPERATION, _DMS_COMMIT_TS, _LOADED_AT, _SOURCE_DB)
VALUES (src.SKU_ID, src.SKU_CODE, src.DESCR, src.UNIT_COST,
        src._DMS_OPERATION, src._DMS_COMMIT_TS, src._LOADED_AT, src._SOURCE_DB);

-- ── INV_STOCK_MOVEMENTS ────────────────────────────────────
MERGE INTO {BRZ}.INV_STOCK_MOVEMENTS tgt
USING (
    SELECT DISTINCT
        v:MovId::NUMBER              AS MOV_ID,
        v:WhId::NUMBER               AS WH_ID,
        v:SkuId::NUMBER              AS SKU_ID,
        v:QtyChange::NUMBER          AS QTY_CHANGE,
        v:Reason::VARCHAR(80)        AS REASON,
        v:MovDate::TIMESTAMP_NTZ     AS MOV_DATE,
        'I' AS _DMS_OPERATION, NULL::TIMESTAMP_NTZ AS _DMS_COMMIT_TS,
        CURRENT_TIMESTAMP() AS _LOADED_AT, 'LabInventory_DB' AS _SOURCE_DB
    FROM {RAW}.RAW_DMS_VARIANT
    WHERE v:MovId IS NOT NULL AND v:WhId IS NOT NULL
) src ON tgt.MOV_ID = src.MOV_ID AND tgt._SOURCE_DB = src._SOURCE_DB
WHEN NOT MATCHED THEN INSERT
    (MOV_ID, WH_ID, SKU_ID, QTY_CHANGE, REASON, MOV_DATE,
     _DMS_OPERATION, _DMS_COMMIT_TS, _LOADED_AT, _SOURCE_DB)
VALUES (src.MOV_ID, src.WH_ID, src.SKU_ID, src.QTY_CHANGE, src.REASON, src.MOV_DATE,
        src._DMS_OPERATION, src._DMS_COMMIT_TS, src._LOADED_AT, src._SOURCE_DB);
"""

# ─────────────────────────────────────────────────────────────────────────────
# SQL: cross-layer row count report
# ─────────────────────────────────────────────────────────────────────────────
REPORT_SQL = f"""
SELECT layer, table_name, row_count FROM (
    SELECT 'BRONZE' AS layer, 'CUSTOMERS'     AS table_name, COUNT(*) AS row_count FROM {DB}.{BRZ}.CUSTOMERS UNION ALL
    SELECT 'BRONZE', 'PRODUCTS',     COUNT(*) FROM {DB}.{BRZ}.PRODUCTS UNION ALL
    SELECT 'BRONZE', 'ORDERS',       COUNT(*) FROM {DB}.{BRZ}.ORDERS UNION ALL
    SELECT 'BRONZE', 'ORDER_ITEMS',  COUNT(*) FROM {DB}.{BRZ}.ORDER_ITEMS UNION ALL
    SELECT 'BRONZE', 'ERP_EMPLOYEES',COUNT(*) FROM {DB}.{BRZ}.ERP_EMPLOYEES UNION ALL
    SELECT 'BRONZE', 'CRM_ACCOUNTS', COUNT(*) FROM {DB}.{BRZ}.CRM_ACCOUNTS UNION ALL
    SELECT 'BRONZE', 'INV_SKU',      COUNT(*) FROM {DB}.{BRZ}.INV_SKU UNION ALL
    SELECT 'BRONZE', 'INV_STOCK_MOVEMENTS', COUNT(*) FROM {DB}.{BRZ}.INV_STOCK_MOVEMENTS
)
ORDER BY layer, table_name;
"""


# ─────────────────────────────────────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="mssql_bronze_silver_gold_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["sqlserver", "snowflake", "dbt", "mssql-migration"],
    doc_md=__doc__,
) as dag:

    # ── Stage 1: check Bronze tables have data ────────────────────────────
    check_bronze = SnowflakeOperator(
        task_id="check_bronze_tables",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=CHECK_BRONZE_SQL,
        warehouse=WH,
        database=DB,
        schema=BRZ,
        role=ROLE,
    )

    # ── Stage 2: flatten RAW VARIANT → typed Bronze (idempotent MERGE) ────
    flatten_bronze = SnowflakeOperator(
        task_id="flatten_bronze",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=FLATTEN_BRONZE_SQL,
        warehouse=WH,
        database=DB,
        schema=BRZ,
        role=ROLE,
    )

    # ── Stage 3–7: dbt pipeline (runs inside dms-dbt-runner Docker image) ─
    dbt_snapshot = build_dbt_task(
        "dbt_snapshot",
        "dbt snapshot",
    )
    # Override SNOWFLAKE_DATABASE for migration project
    dbt_snapshot.environment = DBT_ENV

    dbt_run_stg = build_dbt_task("dbt_run_stg", "dbt run --select stg")
    dbt_run_stg.environment = DBT_ENV

    dbt_run_int = build_dbt_task("dbt_run_int", "dbt run --select int")
    dbt_run_int.environment = DBT_ENV

    dbt_run_gold = build_dbt_task("dbt_run_gold", "dbt run --select gold")
    dbt_run_gold.environment = DBT_ENV

    dbt_test = build_dbt_task("dbt_test", "dbt test --select gold")
    dbt_test.environment = DBT_ENV

    # ── Stage 8: final report ─────────────────────────────────────────────
    report = SnowflakeOperator(
        task_id="report_row_counts",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=REPORT_SQL,
        warehouse=WH,
        database=DB,
    )

    # ── Dependency chain ─────────────────────────────────────────────────
    (
        check_bronze
        >> flatten_bronze
        >> dbt_snapshot
        >> dbt_run_stg
        >> dbt_run_int
        >> dbt_run_gold
        >> dbt_test
        >> report
    )
