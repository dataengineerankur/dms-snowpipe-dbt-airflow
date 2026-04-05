-- ============================================================================
-- Snowflake University: Iceberg on S3 Lab
-- ============================================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS LAKEHOUSE_DB;
CREATE SCHEMA IF NOT EXISTS LAKEHOUSE_DB.PUBLIC;
USE DATABASE LAKEHOUSE_DB;

-- Replace <YOUR_AWS_ROLE_ARN> before running.
-- This starter creates a Snowflake-managed Iceberg table on S3.
-- If you want true multi-engine discovery and shared catalog behavior, you will also
-- need the appropriate external catalog or open catalog setup beyond this script.
CREATE OR REPLACE EXTERNAL VOLUME UNIVERSITY_ICEBERG_VOL
  STORAGE_LOCATIONS = (
    (
      NAME = 'primary'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://dms-snowpipe-dev-05d6e64a/snowflake-university/iceberg/'
      STORAGE_AWS_ROLE_ARN = '<YOUR_AWS_ROLE_ARN>'
    )
  );

DESC EXTERNAL VOLUME UNIVERSITY_ICEBERG_VOL;

CREATE OR REPLACE ICEBERG TABLE LAKEHOUSE_DB.PUBLIC.ICEBERG_RETAIL_TRANSACTIONS (
  ORDER_ID STRING,
  CUSTOMER_ID NUMBER,
  PRODUCT_ID NUMBER,
  ORDER_STATUS STRING,
  ORDER_DATE TIMESTAMP_NTZ,
  REGION STRING,
  AMOUNT NUMBER(12,2),
  QUANTITY NUMBER,
  CREATED_AT TIMESTAMP_NTZ
)
EXTERNAL_VOLUME = 'UNIVERSITY_ICEBERG_VOL'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'retail_transactions';

INSERT OVERWRITE INTO LAKEHOUSE_DB.PUBLIC.ICEBERG_RETAIL_TRANSACTIONS
SELECT ORDER_ID, CUSTOMER_ID, PRODUCT_ID, ORDER_STATUS, ORDER_DATE, REGION, AMOUNT, QUANTITY, CREATED_AT
FROM RAW_DB.PUBLIC.RAW_ORDERS
WHERE ORDER_DATE >= '2024-01-01'::TIMESTAMP_NTZ
  AND ORDER_DATE < '2024-04-01'::TIMESTAMP_NTZ;

-- This intentionally starts from the Snowflake University raw orders table so you can
-- compare native Snowflake storage against Snowflake-managed Iceberg storage on S3.
-- The date slice is deterministic so the lab still loads rows even if no recent CDC has been run.
-- It is a teaching starter, not yet a retail-live or curated gold publication flow.

SELECT COUNT(*) AS iceberg_rows FROM LAKEHOUSE_DB.PUBLIC.ICEBERG_RETAIL_TRANSACTIONS;
SELECT * FROM LAKEHOUSE_DB.PUBLIC.ICEBERG_RETAIL_TRANSACTIONS LIMIT 20;
SHOW ICEBERG TABLES IN SCHEMA LAKEHOUSE_DB.PUBLIC;
