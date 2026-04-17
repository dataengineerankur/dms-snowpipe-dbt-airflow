-- SF009 - Merge duplicates due ON condition
-- Category: modeling
-- Description: upsert key incomplete - FIXED
-- Fix: Deduplicate source data before MERGE using QUALIFY to keep only latest CDC event per key

-- Create staging stream table if not exists
CREATE TABLE IF NOT EXISTS RAW_DB.STAGING.STG_ORDERS_STREAM (
    ORDER_ID NUMBER,
    CUSTOMER_ID NUMBER,
    ORDER_STATUS VARCHAR(50),
    ORDER_DATE TIMESTAMP,
    ORDER_UPDATED_AT TIMESTAMP,
    DMS_OP VARCHAR(10),
    DMS_COMMIT_TS TIMESTAMP,
    DMS_LOAD_TS TIMESTAMP
);

-- Create target table if not exists
CREATE TABLE IF NOT EXISTS RAW_DB.GOLD.ORDERS_TARGET (
    ORDER_ID NUMBER PRIMARY KEY,
    CUSTOMER_ID NUMBER,
    ORDER_STATUS VARCHAR(50),
    ORDER_DATE TIMESTAMP,
    ORDER_UPDATED_AT TIMESTAMP,
    LAST_DMS_OP VARCHAR(10),
    LAST_COMMIT_TS TIMESTAMP
);

-- Deduplicate source before merge: keep only the latest record per ORDER_ID
-- This prevents "duplicate row detected during MERGE" error
WITH deduped_source AS (
    SELECT
        ORDER_ID,
        CUSTOMER_ID,
        ORDER_STATUS,
        ORDER_DATE,
        ORDER_UPDATED_AT,
        DMS_OP,
        DMS_COMMIT_TS
    FROM RAW_DB.STAGING.STG_ORDERS_STREAM
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ORDER_ID 
        ORDER BY DMS_COMMIT_TS DESC, DMS_LOAD_TS DESC
    ) = 1
)
MERGE INTO RAW_DB.GOLD.ORDERS_TARGET AS target
USING deduped_source AS source
    ON target.ORDER_ID = source.ORDER_ID
WHEN MATCHED AND source.DMS_OP = 'D' THEN
    DELETE
WHEN MATCHED THEN
    UPDATE SET
        target.CUSTOMER_ID = source.CUSTOMER_ID,
        target.ORDER_STATUS = source.ORDER_STATUS,
        target.ORDER_DATE = source.ORDER_DATE,
        target.ORDER_UPDATED_AT = source.ORDER_UPDATED_AT,
        target.LAST_DMS_OP = source.DMS_OP,
        target.LAST_COMMIT_TS = source.DMS_COMMIT_TS
WHEN NOT MATCHED AND source.DMS_OP != 'D' THEN
    INSERT (
        ORDER_ID,
        CUSTOMER_ID,
        ORDER_STATUS,
        ORDER_DATE,
        ORDER_UPDATED_AT,
        LAST_DMS_OP,
        LAST_COMMIT_TS
    )
    VALUES (
        source.ORDER_ID,
        source.CUSTOMER_ID,
        source.ORDER_STATUS,
        source.ORDER_DATE,
        source.ORDER_UPDATED_AT,
        source.DMS_OP,
        source.DMS_COMMIT_TS
    );
