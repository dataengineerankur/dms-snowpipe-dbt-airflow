-- SF009 - Merge duplicates due ON condition
-- Category: modeling
-- Description: upsert key incomplete - FIXED by deduplicating source before merge

-- This script demonstrates a CDC merge pattern that handles duplicate source keys
-- by deduplicating using QUALIFY with ROW_NUMBER() to keep only the latest record.

MERGE INTO ANALYTICS.RAW.ORDERS AS target
USING (
  -- Deduplicate source data by keeping only the latest record per ORDER_ID
  SELECT 
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_STATUS,
    ORDER_DATE,
    ORDER_UPDATED_AT,
    DMS_OP,
    DMS_COMMIT_TS
  FROM ANALYTICS.RAW.ORDERS_STAGING
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ORDER_ID 
    ORDER BY DMS_COMMIT_TS DESC, DMS_LOAD_TS DESC
  ) = 1
) AS source
ON target.ORDER_ID = source.ORDER_ID
WHEN MATCHED AND source.DMS_OP = 'D' THEN
  DELETE
WHEN MATCHED THEN
  UPDATE SET
    target.CUSTOMER_ID = source.CUSTOMER_ID,
    target.ORDER_STATUS = source.ORDER_STATUS,
    target.ORDER_DATE = source.ORDER_DATE,
    target.ORDER_UPDATED_AT = source.ORDER_UPDATED_AT,
    target.DMS_OP = source.DMS_OP,
    target.DMS_COMMIT_TS = source.DMS_COMMIT_TS
WHEN NOT MATCHED AND source.DMS_OP != 'D' THEN
  INSERT (
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_STATUS,
    ORDER_DATE,
    ORDER_UPDATED_AT,
    DMS_OP,
    DMS_COMMIT_TS
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
