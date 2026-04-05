-- ============================================================================
-- Snowflake University: External tables, stages, and lake-facing patterns
-- ============================================================================
-- What this teaches
--   * External stage + LIST: inventory files in object storage without loading.
--   * External table: query Parquet in place; metadata + partition pruning.
--   * INFER_SCHEMA: discover file shape before you commit to DDL.
--   * REFRESH: sync external table metadata when new files land (when not using events).
--
-- Prerequisites
--   * A storage integration that allows the S3 prefix below. This repo Terraform defines
--     S3_DMS_INT on s3://<bucket>/dms/ (see infra/snowflake/main.tf).
--   * If your integration name or bucket differs, change STORAGE_INTEGRATION and URL.
--   * DMS Parquet files should exist under dms/sales/orders/ (same layout as Snowpipe).
--
-- Cost note
--   * External tables avoid Snowflake storage for raw files (you pay S3).
--   * You still pay warehouse credits on query; large scans without pruning are expensive.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS EXT_DEMO;
CREATE SCHEMA IF NOT EXISTS EXT_DEMO.PUBLIC;
USE DATABASE EXT_DEMO;
USE SCHEMA PUBLIC;

CREATE OR REPLACE FILE FORMAT EXT_DEMO.PUBLIC.UNIV_PARQUET
  TYPE = PARQUET
  COMPRESSION = AUTO;

CREATE OR REPLACE STAGE EXT_DEMO.PUBLIC.DMS_ORDERS_EXT
  URL = 's3://dms-snowpipe-dev-05d6e64a/dms/sales/orders/'
  STORAGE_INTEGRATION = S3_DMS_INT;

LIST @EXT_DEMO.PUBLIC.DMS_ORDERS_EXT LIMIT 30;

SELECT *
FROM TABLE(
  INFER_SCHEMA(
    LOCATION => '@EXT_DEMO.PUBLIC.DMS_ORDERS_EXT',
    FILE_FORMAT => 'EXT_DEMO.PUBLIC.UNIV_PARQUET'
  )
);

CREATE OR REPLACE EXTERNAL TABLE EXT_DEMO.PUBLIC.EXT_DMS_ORDERS (
  ORDER_ID NUMBER AS ($1:order_id::NUMBER),
  CUSTOMER_ID NUMBER AS ($1:customer_id::NUMBER),
  ORDER_STATUS VARCHAR AS ($1:order_status::VARCHAR),
  ORDER_DATE TIMESTAMP_NTZ AS ($1:order_date::TIMESTAMP_NTZ),
  UPDATED_AT TIMESTAMP_NTZ AS ($1:updated_at::TIMESTAMP_NTZ),
  DMS_OP VARCHAR AS (COALESCE($1:dms_op, $1:Op)::VARCHAR),
  DMS_COMMIT_TS TIMESTAMP_LTZ AS ($1:dms_commit_ts::TIMESTAMP_LTZ),
  SRC_FILE VARCHAR AS (METADATA$FILENAME::VARCHAR)
)
PARTITION BY (
  LOAD_DT DATE AS (
    TRY_TO_DATE(
      SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'load_dt=', 2), '/', 1),
      'YYYYMMDD'
    )
  )
)
LOCATION = @EXT_DEMO.PUBLIC.DMS_ORDERS_EXT
AUTO_REFRESH = FALSE
FILE_FORMAT = EXT_DEMO.PUBLIC.UNIV_PARQUET;

ALTER EXTERNAL TABLE EXT_DEMO.PUBLIC.EXT_DMS_ORDERS REFRESH;

SELECT COUNT(*) AS row_count FROM EXT_DEMO.PUBLIC.EXT_DMS_ORDERS;
SELECT LOAD_DT, COUNT(*) AS n
FROM EXT_DEMO.PUBLIC.EXT_DMS_ORDERS
WHERE LOAD_DT >= '2024-01-01'
GROUP BY 1
ORDER BY 1 NULLS LAST
LIMIT 50;

SHOW EXTERNAL TABLES IN SCHEMA EXT_DEMO.PUBLIC;

-- Compare native Snowpipe-loaded table vs external scan (open Query Profile for each):
-- SELECT COUNT(*) FROM ANALYTICS.RAW.ORDERS;
-- SELECT COUNT(*) FROM EXT_DEMO.PUBLIC.EXT_DMS_ORDERS WHERE LOAD_DT = '20240115';

-- ============================================================================
-- Troubleshooting: if PARTITION BY fails on your path layout, create without it:
-- ============================================================================
-- CREATE OR REPLACE EXTERNAL TABLE EXT_DEMO.PUBLIC.EXT_DMS_ORDERS_FLAT (
--   ORDER_ID NUMBER AS ($1:order_id::NUMBER),
--   CUSTOMER_ID NUMBER AS ($1:customer_id::NUMBER),
--   ORDER_STATUS VARCHAR AS ($1:order_status::VARCHAR),
--   ORDER_DATE TIMESTAMP_NTZ AS ($1:order_date::TIMESTAMP_NTZ),
--   UPDATED_AT TIMESTAMP_NTZ AS ($1:updated_at::TIMESTAMP_NTZ),
--   DMS_OP VARCHAR AS (COALESCE($1:dms_op, $1:Op)::VARCHAR),
--   DMS_COMMIT_TS TIMESTAMP_LTZ AS ($1:dms_commit_ts::TIMESTAMP_LTZ),
--   SRC_FILE VARCHAR AS (METADATA$FILENAME::VARCHAR)
-- )
-- LOCATION = @EXT_DEMO.PUBLIC.DMS_ORDERS_EXT
-- AUTO_REFRESH = FALSE
-- FILE_FORMAT = EXT_DEMO.PUBLIC.UNIV_PARQUET;
-- ALTER EXTERNAL TABLE EXT_DEMO.PUBLIC.EXT_DMS_ORDERS_FLAT REFRESH;
