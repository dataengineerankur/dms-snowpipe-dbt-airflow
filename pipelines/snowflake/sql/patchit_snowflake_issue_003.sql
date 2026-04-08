-- SF003 - Schema drift in staged files
-- Category: schema
-- Description: COPY INTO fails due column mismatch

COPY INTO ANALYTICS.RAW.CUSTOMERS
  (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, CREATED_AT, UPDATED_AT, DMS_OP, DMS_COMMIT_TS, DMS_LOAD_TS, DMS_FILE_NAME)
FROM (
  SELECT
    $1:customer_id::NUMBER,
    $1:first_name::VARCHAR,
    $1:last_name::VARCHAR,
    $1:email::VARCHAR,
    $1:created_at::TIMESTAMP_NTZ,
    $1:updated_at::TIMESTAMP_NTZ,
    $1:dms_op::VARCHAR,
    $1:dms_commit_ts::TIMESTAMP_LTZ,
    CURRENT_TIMESTAMP(),
    METADATA$FILENAME
  FROM @ANALYTICS.RAW.DMS_STAGE/sales/customers/
)
FILE_FORMAT = (FORMAT_NAME = ANALYTICS.RAW.DMS_PARQUET_FF);
