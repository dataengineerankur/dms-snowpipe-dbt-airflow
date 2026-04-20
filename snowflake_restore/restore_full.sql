-- =============================================================================
-- restore_full.sql
-- =============================================================================
-- PURPOSE
--   Full point-in-time restore of TWO Snowflake databases into a brand-new
--   Snowflake account.  Run this script top-to-bottom in a fresh account and
--   all warehouses, roles, databases, schemas, tables, file formats, stages,
--   pipes, streams, tasks, stored procedures, and UDFs will be re-created.
--
-- SOURCE ACCOUNT
--   Original account: QT63913  (us-east-1)
--   Source S3 bucket: dms-snowpipe-dev-05d6e64a  (AWS account 384177185145)
--
-- DATABASES COVERED
--   1. ANALYTICS           — Postgres → S3 DMS → Snowpipe ingestion pipeline
--   2. MSSQL_MIGRATION_LAB — SQL Server → Snowflake migration lab
--
-- HOW TO USE
--   1. Open a Snowsight worksheet (or SnowSQL) connected as ACCOUNTADMIN.
--   2. Search for every "TODO" comment and substitute your environment values
--      before running (at minimum: S3 bucket name, IAM role ARN, SQS ARNs).
--   3. Run the file in full — all statements use CREATE OR REPLACE so the
--      script is idempotent and safe to re-run.
--   4. After Snowpipes are created, recreate the S3→SQS event notifications
--      pointing to the new SQS ARNs reported by SYSTEM$PIPE_STATUS().
--   5. Tasks are created SUSPENDED.  Resume them once the pipeline is verified:
--        ALTER TASK <db>.<schema>.<task_name> RESUME;
--
-- GENERATED
--   Date: 2026-04-19
--   Sources: infra/snowflake/main.tf + terraform.tfstate
--            sqlserver-to-snowflake-migration/snowflake_ddl/**
-- =============================================================================


-- =============================================================================
-- SECTION 1: WAREHOUSES
-- =============================================================================

-- Ingest warehouse  (used by Snowpipe / ANALYTICS pipeline)
CREATE OR REPLACE WAREHOUSE INGEST_WH
    WAREHOUSE_SIZE      = 'XSMALL'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    WAREHOUSE_TYPE      = 'STANDARD'
    SCALING_POLICY      = 'STANDARD'
    MAX_CLUSTER_COUNT   = 1
    MIN_CLUSTER_COUNT   = 1
    MAX_CONCURRENCY_LEVEL = 8
    COMMENT = 'Snowpipe ingest warehouse — ANALYTICS pipeline';

-- Transform warehouse  (used by dbt / MSSQL_MIGRATION_LAB tasks)
CREATE OR REPLACE WAREHOUSE TRANSFORM_WH
    WAREHOUSE_SIZE      = 'SMALL'
    AUTO_SUSPEND        = 120
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    WAREHOUSE_TYPE      = 'STANDARD'
    SCALING_POLICY      = 'STANDARD'
    MAX_CLUSTER_COUNT   = 1
    MIN_CLUSTER_COUNT   = 1
    MAX_CONCURRENCY_LEVEL = 8
    COMMENT = 'dbt transform warehouse — ANALYTICS + MSSQL_MIGRATION_LAB';

-- Migration lab warehouse  (used by streams/tasks in MSSQL_MIGRATION_LAB)
CREATE OR REPLACE WAREHOUSE WH_MSSQL_MIGRATION
    WAREHOUSE_SIZE      = 'XSMALL'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    WAREHOUSE_TYPE      = 'STANDARD'
    COMMENT = 'Stream/task warehouse for MSSQL_MIGRATION_LAB';


-- =============================================================================
-- SECTION 2: ROLES
-- =============================================================================

CREATE OR REPLACE ROLE INGEST_ROLE
    COMMENT = 'Owns Snowpipe objects; granted to the DMS service user';

CREATE OR REPLACE ROLE TRANSFORM_ROLE
    COMMENT = 'Used by dbt and analytics consumers; SELECT on RAW, DML on STG/INT/GOLD';

-- Grant roles to SYSADMIN so they remain manageable
GRANT ROLE INGEST_ROLE    TO ROLE SYSADMIN;
GRANT ROLE TRANSFORM_ROLE TO ROLE SYSADMIN;


-- =============================================================================
-- SECTION 3: DATABASE ANALYTICS + SCHEMAS
-- =============================================================================

CREATE OR REPLACE DATABASE ANALYTICS
    COMMENT = 'Postgres → AWS DMS → S3 → Snowpipe ingestion pipeline';

CREATE OR REPLACE SCHEMA ANALYTICS.RAW
    COMMENT = 'Raw CDC events from DMS — owned by INGEST_ROLE';

CREATE OR REPLACE SCHEMA ANALYTICS.STG
    COMMENT = 'Staging / cleaned layer — written by dbt staging models';

CREATE OR REPLACE SCHEMA ANALYTICS.INT
    COMMENT = 'Intermediate / enrichment layer — written by dbt int models';

CREATE OR REPLACE SCHEMA ANALYTICS.GOLD
    COMMENT = 'Gold / presentation layer — written by dbt mart models';


-- =============================================================================
-- SECTION 4: FILE FORMAT — DMS_PARQUET_FF
-- =============================================================================

CREATE OR REPLACE FILE FORMAT ANALYTICS.RAW.DMS_PARQUET_FF
    TYPE               = 'PARQUET'
    SNAPPY_COMPRESSION = TRUE
    COMMENT = 'Parquet file format used by AWS DMS full-load and CDC files';


-- =============================================================================
-- SECTION 5: STORAGE INTEGRATION — S3_DMS_INT
-- =============================================================================

-- TODO: Update storage_aws_role_arn to your new IAM role
--       (original: arn:aws:iam::384177185145:role/dms-snowpipe-dev-snowflake-storage-role)
-- After creation run:
--   DESC INTEGRATION S3_DMS_INT;
-- and copy STORAGE_AWS_IAM_USER_ARN + STORAGE_AWS_EXTERNAL_ID into your IAM
-- trust policy for the new role.

CREATE OR REPLACE STORAGE INTEGRATION S3_DMS_INT
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    ENABLED                   = TRUE
    -- TODO: Update storage_aws_role_arn to your new IAM role
    STORAGE_AWS_ROLE_ARN       = 'arn:aws:iam::384177185145:role/dms-snowpipe-dev-snowflake-storage-role'
    -- TODO: Update S3 bucket to your new bucket name
    STORAGE_ALLOWED_LOCATIONS  = ('s3://dms-snowpipe-dev-05d6e64a/dms/')
    COMMENT = 'Snowflake → S3 trust for DMS Parquet landing bucket';


-- =============================================================================
-- SECTION 6: STAGE — DMS_STAGE
-- =============================================================================

-- TODO: Update S3 bucket to your new bucket name
--       (original URL: s3://dms-snowpipe-dev-05d6e64a/dms/)

CREATE OR REPLACE STAGE ANALYTICS.RAW.DMS_STAGE
    -- TODO: Update S3 bucket to your new bucket name
    URL                = 's3://dms-snowpipe-dev-05d6e64a/dms/'
    STORAGE_INTEGRATION = S3_DMS_INT
    FILE_FORMAT        = (FORMAT_NAME = ANALYTICS.RAW.DMS_PARQUET_FF)
    COMMENT = 'External stage pointing at the DMS landing prefix in S3';


-- =============================================================================
-- SECTION 7: TABLES — ANALYTICS.RAW
-- =============================================================================

CREATE OR REPLACE TABLE ANALYTICS.RAW.CUSTOMERS (
    CUSTOMER_ID    NUMBER,
    FIRST_NAME     VARCHAR,
    LAST_NAME      VARCHAR,
    EMAIL          VARCHAR,
    CREATED_AT     TIMESTAMP_NTZ,
    UPDATED_AT     TIMESTAMP_NTZ,
    DMS_OP         VARCHAR,
    DMS_COMMIT_TS  TIMESTAMP_LTZ,
    DMS_LOAD_TS    TIMESTAMP_LTZ,
    DMS_FILE_NAME  VARCHAR
);

CREATE OR REPLACE TABLE ANALYTICS.RAW.ORDERS (
    ORDER_ID       NUMBER,
    CUSTOMER_ID    NUMBER,
    ORDER_STATUS   VARCHAR,
    ORDER_DATE     TIMESTAMP_NTZ,
    UPDATED_AT     TIMESTAMP_NTZ,
    DMS_OP         VARCHAR,
    DMS_COMMIT_TS  TIMESTAMP_LTZ,
    DMS_LOAD_TS    TIMESTAMP_LTZ,
    DMS_FILE_NAME  VARCHAR
);

CREATE OR REPLACE TABLE ANALYTICS.RAW.ORDER_ITEMS (
    ORDER_ITEM_ID  NUMBER,
    ORDER_ID       NUMBER,
    PRODUCT_ID     NUMBER,
    QUANTITY       NUMBER,
    UNIT_PRICE     NUMBER(12,2),
    CREATED_AT     TIMESTAMP_NTZ,
    UPDATED_AT     TIMESTAMP_NTZ,
    DMS_OP         VARCHAR,
    DMS_COMMIT_TS  TIMESTAMP_LTZ,
    DMS_LOAD_TS    TIMESTAMP_LTZ,
    DMS_FILE_NAME  VARCHAR
);

CREATE OR REPLACE TABLE ANALYTICS.RAW.PRODUCTS (
    PRODUCT_ID     NUMBER,
    SKU            VARCHAR,
    PRODUCT_NAME   VARCHAR,
    CATEGORY       VARCHAR,
    PRICE          NUMBER(12,2),
    CREATED_AT     TIMESTAMP_NTZ,
    UPDATED_AT     TIMESTAMP_NTZ,
    DMS_OP         VARCHAR,
    DMS_COMMIT_TS  TIMESTAMP_LTZ,
    DMS_LOAD_TS    TIMESTAMP_LTZ,
    DMS_FILE_NAME  VARCHAR
);


-- =============================================================================
-- SECTION 8: SNOWPIPES — ANALYTICS.RAW
-- =============================================================================
-- NOTE: Pipes are created with AUTO_INGEST = TRUE.
--       After creation, run SYSTEM$PIPE_STATUS('<pipe_name>') to get the new
--       SQS ARN, then update your S3 bucket event notification to point at it.
-- TODO: Update notification_channel to your new SQS ARN after recreating Snowpipe
--       (original SQS: arn:aws:sqs:us-east-1:482885091138:sf-snowpipe-AIDAXA3RRX5BFYZ6RDRUW-ubVdzy18AhVxelCFSIPx2w)

CREATE OR REPLACE PIPE ANALYTICS.RAW.PIPE_CUSTOMERS
    AUTO_INGEST = TRUE
    -- TODO: Update notification_channel to your new SQS ARN after recreating Snowpipe
    COMMENT = 'Auto-ingest pipe: S3 DMS → ANALYTICS.RAW.CUSTOMERS'
AS
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
        COALESCE($1:dms_op, $1:Op)::VARCHAR,
        $1:dms_commit_ts::TIMESTAMP_LTZ,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
    FROM @ANALYTICS.RAW.DMS_STAGE/sales/customers/
)
FILE_FORMAT = (FORMAT_NAME = ANALYTICS.RAW.DMS_PARQUET_FF);

CREATE OR REPLACE PIPE ANALYTICS.RAW.PIPE_ORDERS
    AUTO_INGEST = TRUE
    -- TODO: Update notification_channel to your new SQS ARN after recreating Snowpipe
    COMMENT = 'Auto-ingest pipe: S3 DMS → ANALYTICS.RAW.ORDERS'
AS
COPY INTO ANALYTICS.RAW.ORDERS
    (ORDER_ID, CUSTOMER_ID, ORDER_STATUS, ORDER_DATE, UPDATED_AT, DMS_OP, DMS_COMMIT_TS, DMS_LOAD_TS, DMS_FILE_NAME)
FROM (
    SELECT
        $1:order_id::NUMBER,
        $1:customer_id::NUMBER,
        $1:order_status::VARCHAR,
        $1:order_date::TIMESTAMP_NTZ,
        $1:updated_at::TIMESTAMP_NTZ,
        COALESCE($1:dms_op, $1:Op)::VARCHAR,
        $1:dms_commit_ts::TIMESTAMP_LTZ,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
    FROM @ANALYTICS.RAW.DMS_STAGE/sales/orders/
)
FILE_FORMAT = (FORMAT_NAME = ANALYTICS.RAW.DMS_PARQUET_FF);

CREATE OR REPLACE PIPE ANALYTICS.RAW.PIPE_ORDER_ITEMS
    AUTO_INGEST = TRUE
    -- TODO: Update notification_channel to your new SQS ARN after recreating Snowpipe
    COMMENT = 'Auto-ingest pipe: S3 DMS → ANALYTICS.RAW.ORDER_ITEMS'
AS
COPY INTO ANALYTICS.RAW.ORDER_ITEMS
    (ORDER_ITEM_ID, ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, CREATED_AT, UPDATED_AT, DMS_OP, DMS_COMMIT_TS, DMS_LOAD_TS, DMS_FILE_NAME)
FROM (
    SELECT
        $1:order_item_id::NUMBER,
        $1:order_id::NUMBER,
        $1:product_id::NUMBER,
        $1:quantity::NUMBER,
        $1:unit_price::NUMBER(12,2),
        $1:created_at::TIMESTAMP_NTZ,
        $1:updated_at::TIMESTAMP_NTZ,
        COALESCE($1:dms_op, $1:Op)::VARCHAR,
        $1:dms_commit_ts::TIMESTAMP_LTZ,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
    FROM @ANALYTICS.RAW.DMS_STAGE/sales/order_items/
)
FILE_FORMAT = (FORMAT_NAME = ANALYTICS.RAW.DMS_PARQUET_FF);

CREATE OR REPLACE PIPE ANALYTICS.RAW.PIPE_PRODUCTS
    AUTO_INGEST = TRUE
    -- TODO: Update notification_channel to your new SQS ARN after recreating Snowpipe
    COMMENT = 'Auto-ingest pipe: S3 DMS → ANALYTICS.RAW.PRODUCTS'
AS
COPY INTO ANALYTICS.RAW.PRODUCTS
    (PRODUCT_ID, SKU, PRODUCT_NAME, CATEGORY, PRICE, CREATED_AT, UPDATED_AT, DMS_OP, DMS_COMMIT_TS, DMS_LOAD_TS, DMS_FILE_NAME)
FROM (
    SELECT
        $1:product_id::NUMBER,
        $1:sku::VARCHAR,
        $1:product_name::VARCHAR,
        $1:category::VARCHAR,
        $1:price::NUMBER(12,2),
        $1:created_at::TIMESTAMP_NTZ,
        $1:updated_at::TIMESTAMP_NTZ,
        COALESCE($1:dms_op, $1:Op)::VARCHAR,
        $1:dms_commit_ts::TIMESTAMP_LTZ,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
    FROM @ANALYTICS.RAW.DMS_STAGE/sales/products/
)
FILE_FORMAT = (FORMAT_NAME = ANALYTICS.RAW.DMS_PARQUET_FF);


-- =============================================================================
-- SECTION 9: GRANTS — ANALYTICS DATABASE
-- =============================================================================

-- Warehouse grants
GRANT USAGE ON WAREHOUSE INGEST_WH    TO ROLE INGEST_ROLE;
GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE TRANSFORM_ROLE;

-- Database grants
GRANT USAGE ON DATABASE ANALYTICS TO ROLE INGEST_ROLE;
GRANT USAGE ON DATABASE ANALYTICS TO ROLE TRANSFORM_ROLE;

-- Schema grants  (INGEST_ROLE owns RAW; TRANSFORM_ROLE reads all)
GRANT USAGE ON SCHEMA ANALYTICS.RAW  TO ROLE INGEST_ROLE;
GRANT USAGE ON SCHEMA ANALYTICS.RAW  TO ROLE TRANSFORM_ROLE;
GRANT USAGE ON SCHEMA ANALYTICS.STG  TO ROLE TRANSFORM_ROLE;
GRANT USAGE ON SCHEMA ANALYTICS.INT  TO ROLE TRANSFORM_ROLE;
GRANT USAGE ON SCHEMA ANALYTICS.GOLD TO ROLE TRANSFORM_ROLE;

-- Stage grant
GRANT USAGE ON STAGE ANALYTICS.RAW.DMS_STAGE TO ROLE INGEST_ROLE;

-- Table grants
GRANT INSERT ON ALL TABLES IN SCHEMA ANALYTICS.RAW TO ROLE INGEST_ROLE;
GRANT INSERT ON FUTURE TABLES IN SCHEMA ANALYTICS.RAW TO ROLE INGEST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS.RAW TO ROLE TRANSFORM_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ANALYTICS.RAW TO ROLE TRANSFORM_ROLE;

-- Pipe grants
GRANT OPERATE ON ALL PIPES IN SCHEMA ANALYTICS.RAW TO ROLE INGEST_ROLE;
GRANT OPERATE ON FUTURE PIPES IN SCHEMA ANALYTICS.RAW TO ROLE INGEST_ROLE;


-- =============================================================================
-- SECTION 10: DATABASE MSSQL_MIGRATION_LAB + SCHEMAS
-- =============================================================================

CREATE OR REPLACE DATABASE MSSQL_MIGRATION_LAB
    COMMENT = 'SQL Server → Snowflake migration lab (SnowConvertStressDB, LabERP_DB, LabCRM_DB, LabInventory_DB)';

CREATE OR REPLACE SCHEMA MSSQL_MIGRATION_LAB.RAW_MSSQL
    COMMENT = 'Landing zone — raw DMS Parquet variant rows from all SQL Server sources';

CREATE OR REPLACE SCHEMA MSSQL_MIGRATION_LAB.BRONZE
    COMMENT = 'Typed replicas of source tables + DMS metadata columns';

CREATE OR REPLACE SCHEMA MSSQL_MIGRATION_LAB.SILVER
    COMMENT = 'Cleansed / SCD-Type-2 tables (dbt snapshot managed) + append-only journals';

CREATE OR REPLACE SCHEMA MSSQL_MIGRATION_LAB.GOLD
    COMMENT = 'Cross-domain analytics facts and dimensions for BI';

CREATE OR REPLACE SCHEMA MSSQL_MIGRATION_LAB.AIRBYTE_RAW
    COMMENT = 'Reserved for Airbyte raw landing tables';


-- =============================================================================
-- SECTION 11: RAW_MSSQL LAYER
-- Source: snowflake_ddl/raw/01_raw_layer.sql
-- =============================================================================

USE DATABASE MSSQL_MIGRATION_LAB;

-- Landing zone for all DMS Parquet files from SQL Server via AWS DMS.
-- One table receives CDC events from all source tables across all 4 domains
-- (SnowConvertStressDB, LabERP_DB, LabCRM_DB, LabInventory_DB).
-- V holds the full source row payload; top-level columns carry DMS metadata.

-- Main landing table
CREATE OR REPLACE TABLE RAW_MSSQL.RAW_DMS_VARIANT (
    V               VARIANT        NOT NULL,   -- full source row as JSON/Parquet payload
    _DMS_OPERATION  VARCHAR(1),                -- I=Insert  U=Update  D=Delete  (null on full-load)
    _DMS_COMMIT_TS  TIMESTAMP_NTZ(6),          -- CDC commit timestamp from SQL Server log
    _DMS_SEQNO      VARCHAR(40),               -- DMS internal sequence number for ordering
    _LOADED_AT      TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- S3 stage for MSSQL_MIGRATION_LAB raw landing
-- TODO: Update S3 bucket to your new bucket name
-- TODO: Update storage_aws_role_arn to your new IAM role
CREATE OR REPLACE FILE FORMAT RAW_MSSQL.FF_DMS_PARQUET
    TYPE               = 'PARQUET'
    SNAPPY_COMPRESSION = TRUE;

CREATE OR REPLACE STAGE RAW_MSSQL.STG_DMS_MSSQL
    -- TODO: Update storage_aws_role_arn to your new IAM role
    STORAGE_INTEGRATION = S3_DMS_INT             -- replace with real storage integration name if separate
    -- TODO: Update S3 bucket to your new bucket name
    URL                 = 's3://YOUR-DMS-BUCKET/dms-output/'    -- replace with real bucket path
    FILE_FORMAT         = RAW_MSSQL.FF_DMS_PARQUET;


-- =============================================================================
-- SECTION 12: BRONZE LAYER — SnowConvertStressDB
-- Source: snowflake_ddl/bronze/01_stress_db.sql
-- =============================================================================

-- Pattern: faithful typed replica + DMS metadata columns
-- SCT type mapping applied — no business logic

CREATE OR REPLACE TABLE BRONZE.CATEGORIES (
    -- source columns (type-mapped from T-SQL)
    CATEGORY_ID     NUMBER          NOT NULL,   -- INT IDENTITY(1,1)
    CATEGORY_NAME   VARCHAR(100)    NOT NULL,   -- NVARCHAR(100)
    DESCRIPTION     VARCHAR(500),               -- NVARCHAR(500) NULL
    -- DMS / pipeline metadata
    _DMS_OPERATION  VARCHAR(1),                 -- I=Insert U=Update D=Delete
    _DMS_COMMIT_TS  TIMESTAMP_NTZ(6),
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'SnowConvertStressDB',
    CONSTRAINT PK_BRONZE_CATEGORIES PRIMARY KEY (_SOURCE_DB, CATEGORY_ID) NOT ENFORCED
);

CREATE OR REPLACE TABLE BRONZE.CUSTOMERS (
    CUSTOMER_ID     NUMBER          NOT NULL,   -- INT IDENTITY(1,1)
    CUSTOMER_CODE   VARCHAR(20)     NOT NULL,   -- NVARCHAR(20) UNIQUE
    FULL_NAME       VARCHAR(200)    NOT NULL,   -- NVARCHAR(200)
    EMAIL           VARCHAR(320),               -- NVARCHAR(320) NULL
    COUNTRY         VARCHAR(100)    NOT NULL,   -- NVARCHAR(100) DEFAULT 'US'
    CREATED_AT      TIMESTAMP_NTZ(3),           -- DATETIME2(3)
    _DMS_OPERATION  VARCHAR(1),
    _DMS_COMMIT_TS  TIMESTAMP_NTZ(6),
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'SnowConvertStressDB',
    CONSTRAINT PK_BRONZE_CUSTOMERS PRIMARY KEY (_SOURCE_DB, CUSTOMER_ID) NOT ENFORCED
);

CREATE OR REPLACE TABLE BRONZE.PRODUCTS (
    PRODUCT_ID      NUMBER          NOT NULL,   -- INT IDENTITY(1,1)
    CATEGORY_ID     NUMBER          NOT NULL,   -- INT FK → Categories
    SKU             VARCHAR(50)     NOT NULL,   -- NVARCHAR(50) UNIQUE
    PRODUCT_NAME    VARCHAR(200)    NOT NULL,   -- NVARCHAR(200)
    LIST_PRICE      NUMBER(18,4)    NOT NULL,   -- DECIMAL(18,4)
    IS_ACTIVE       BOOLEAN         NOT NULL,   -- BIT DEFAULT 1
    _DMS_OPERATION  VARCHAR(1),
    _DMS_COMMIT_TS  TIMESTAMP_NTZ(6),
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'SnowConvertStressDB'
    -- FK: CATEGORY_ID → CATEGORIES.CATEGORY_ID (not enforced)
);

CREATE OR REPLACE TABLE BRONZE.ORDERS (
    ORDER_ID        NUMBER          NOT NULL,   -- BIGINT IDENTITY(1,1)
    CUSTOMER_ID     NUMBER          NOT NULL,   -- INT FK → Customers
    ORDER_DATE      DATE            NOT NULL,   -- DATE
    STATUS          VARCHAR(30)     NOT NULL,   -- NVARCHAR(30) DEFAULT 'Open'
    TOTAL_AMOUNT    NUMBER(18,4)    NOT NULL,   -- DECIMAL(18,4)
    NOTES           VARCHAR(500),               -- NVARCHAR(500) NULL
    _DMS_OPERATION  VARCHAR(1),
    _DMS_COMMIT_TS  TIMESTAMP_NTZ(6),
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'SnowConvertStressDB'
);

CREATE OR REPLACE TABLE BRONZE.ORDER_ITEMS (
    ORDER_ITEM_ID   NUMBER          NOT NULL,   -- BIGINT IDENTITY(1,1)
    ORDER_ID        NUMBER          NOT NULL,   -- BIGINT FK → Orders
    PRODUCT_ID      NUMBER          NOT NULL,   -- INT FK → Products
    QUANTITY        NUMBER          NOT NULL,   -- INT CHECK (Quantity > 0)
    UNIT_PRICE      NUMBER(18,4)    NOT NULL,   -- DECIMAL(18,4)
    LINE_TOTAL      NUMBER(18,4),               -- AS (Qty*UnitPrice) PERSISTED — materialized at load
    _DMS_OPERATION  VARCHAR(1),
    _DMS_COMMIT_TS  TIMESTAMP_NTZ(6),
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'SnowConvertStressDB'
);

-- Stress / audit tables
CREATE OR REPLACE TABLE BRONZE.MIGRATION_AUDIT_LOG (
    AUDIT_ID        NUMBER          NOT NULL AUTOINCREMENT PRIMARY KEY,
    TABLE_NAME      VARCHAR(128)    NOT NULL,
    DML_ACTION      VARCHAR(10)     NOT NULL,
    ROW_PK          VARCHAR(200),
    OLD_PAYLOAD     VARIANT,
    NEW_PAYLOAD     VARIANT,
    SQL_TRIGGER     VARCHAR(256),
    NEST_LEVEL      NUMBER          NOT NULL DEFAULT 0,
    CHANGED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CHANGED_BY      VARCHAR(256)    NOT NULL DEFAULT CURRENT_USER()
);

CREATE OR REPLACE TABLE BRONZE.ORDER_EVENT_QUEUE (
    EVENT_ID        NUMBER          NOT NULL AUTOINCREMENT PRIMARY KEY,
    ORDER_ID        NUMBER          NOT NULL,
    EVENT_TYPE      VARCHAR(40)     NOT NULL,
    PAYLOAD         VARIANT,
    CREATED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PROCESSED       BOOLEAN         NOT NULL DEFAULT FALSE
);

CREATE OR REPLACE TABLE BRONZE.ORDERS_ARCHIVE (
    ARCHIVE_ID      NUMBER          NOT NULL AUTOINCREMENT PRIMARY KEY,
    ORDER_ID        NUMBER          NOT NULL,
    CUSTOMER_ID     NUMBER          NOT NULL,
    ORDER_DATE      DATE            NOT NULL,
    STATUS          VARCHAR(30)     NOT NULL,
    TOTAL_AMOUNT    NUMBER(18,4)    NOT NULL,
    NOTES           VARCHAR(500),
    ARCHIVED_AT     TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    ARCHIVE_REASON  VARCHAR(200)
);

CREATE OR REPLACE TABLE BRONZE.PRODUCT_PRICE_HISTORY (
    HISTORY_ID      NUMBER          NOT NULL AUTOINCREMENT PRIMARY KEY,
    PRODUCT_ID      NUMBER          NOT NULL,
    OLD_PRICE       NUMBER(18,4),
    NEW_PRICE       NUMBER(18,4)    NOT NULL,
    CHANGED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CHANGED_BY      VARCHAR(256)    NOT NULL DEFAULT CURRENT_USER()
);

CREATE OR REPLACE TABLE BRONZE.CATEGORY_CLOSURE (
    ANCESTOR_ID     NUMBER          NOT NULL,
    DESCENDANT_ID   NUMBER          NOT NULL,
    DEPTH           NUMBER          NOT NULL,
    CONSTRAINT PK_CATEGORY_CLOSURE PRIMARY KEY (ANCESTOR_ID, DESCENDANT_ID) NOT ENFORCED
);

CREATE INDEX IF NOT EXISTS IDX_ORDER_EVENT_QUEUE_ORDER_ID ON BRONZE.ORDER_EVENT_QUEUE (ORDER_ID);
CREATE INDEX IF NOT EXISTS IDX_MIGRATION_AUDIT_TABLE_DATE  ON BRONZE.MIGRATION_AUDIT_LOG (TABLE_NAME, CHANGED_AT);


-- =============================================================================
-- SECTION 13: BRONZE LAYER — LabERP_DB
-- Source: snowflake_ddl/bronze/02_erp_db.sql
-- =============================================================================

CREATE OR REPLACE TABLE BRONZE.ERP_DEPARTMENTS (
    DEPT_ID         NUMBER          NOT NULL,   -- INT IDENTITY
    DEPT_CODE       VARCHAR(20)     NOT NULL,   -- NVARCHAR(20) UNIQUE
    DEPT_NAME       VARCHAR(120)    NOT NULL,   -- NVARCHAR(120)
    BUDGET_USD      NUMBER(18,2)    NOT NULL,   -- DECIMAL(18,2)
    _DMS_OPERATION  VARCHAR(1),
    _DMS_COMMIT_TS  TIMESTAMP_NTZ(6),
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'LabERP_DB'
);

CREATE OR REPLACE TABLE BRONZE.ERP_EMPLOYEES (
    EMPLOYEE_ID     NUMBER          NOT NULL,   -- INT IDENTITY
    DEPT_ID         NUMBER          NOT NULL,   -- INT FK → Departments
    EMP_CODE        VARCHAR(20)     NOT NULL,   -- NVARCHAR(20) UNIQUE
    FULL_NAME       VARCHAR(200)    NOT NULL,
    HIRE_DATE       DATE            NOT NULL,
    SALARY          NUMBER(18,4)    NOT NULL,
    ROW_VER         BINARY(8),                  -- ROWVERSION → flagged, low value
    _DMS_OPERATION  VARCHAR(1),
    _DMS_COMMIT_TS  TIMESTAMP_NTZ(6),
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'LabERP_DB'
);

CREATE OR REPLACE TABLE BRONZE.ERP_PAYROLL_RUNS (
    RUN_ID          NUMBER          NOT NULL,   -- BIGINT IDENTITY
    RUN_MONTH       CHAR(7)         NOT NULL,   -- CHAR(7) e.g. '2025-01'
    STATUS          VARCHAR(20)     NOT NULL,
    CREATED_AT      TIMESTAMP_NTZ(3),
    _DMS_OPERATION  VARCHAR(1),
    _DMS_COMMIT_TS  TIMESTAMP_NTZ(6),
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'LabERP_DB'
);

CREATE OR REPLACE TABLE BRONZE.ERP_PAYROLL_LINES (
    LINE_ID         NUMBER          NOT NULL,   -- BIGINT IDENTITY
    RUN_ID          NUMBER          NOT NULL,   -- FK → PayrollRuns
    EMPLOYEE_ID     NUMBER          NOT NULL,   -- FK → Employees
    GROSS_PAY       NUMBER(18,4)    NOT NULL,
    NET_PAY         NUMBER(18,4),               -- AS (GrossPay * 0.92) PERSISTED — recomputed at load
    _DMS_OPERATION  VARCHAR(1),
    _DMS_COMMIT_TS  TIMESTAMP_NTZ(6),
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'LabERP_DB'
);

CREATE OR REPLACE TABLE BRONZE.ERP_AUDIT_LOG (
    ID              NUMBER          NOT NULL,
    TABLE_NAME      VARCHAR(128)    NOT NULL,
    ACTION_TYPE     VARCHAR(10)     NOT NULL,
    PAYLOAD         VARCHAR,
    LOGGED_AT       TIMESTAMP_NTZ(3),
    _DMS_OPERATION  VARCHAR(1),
    _DMS_COMMIT_TS  TIMESTAMP_NTZ(6),
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'LabERP_DB'
);


-- =============================================================================
-- SECTION 14: BRONZE LAYER — LabCRM_DB
-- Source: snowflake_ddl/bronze/03_crm_db.sql
-- =============================================================================

CREATE OR REPLACE TABLE BRONZE.CRM_ACCOUNTS (
    ACCOUNT_ID      NUMBER          NOT NULL,   -- INT IDENTITY
    ACCOUNT_CODE    VARCHAR(30)     NOT NULL,   -- NVARCHAR(30) UNIQUE
    NAME            VARCHAR(200)    NOT NULL,
    REGION          VARCHAR(50)     NOT NULL,
    CREATED_AT      TIMESTAMP_NTZ(3),
    _DMS_OPERATION  VARCHAR(1),
    _DMS_COMMIT_TS  TIMESTAMP_NTZ(6),
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'LabCRM_DB'
);

CREATE OR REPLACE TABLE BRONZE.CRM_CONTACTS (
    CONTACT_ID      NUMBER          NOT NULL,
    ACCOUNT_ID      NUMBER          NOT NULL,   -- FK → Accounts
    EMAIL           VARCHAR(320)    NOT NULL,
    FULL_NAME       VARCHAR(200)    NOT NULL,
    IS_PRIMARY      BOOLEAN         NOT NULL,   -- BIT DEFAULT 0
    _DMS_OPERATION  VARCHAR(1),
    _DMS_COMMIT_TS  TIMESTAMP_NTZ(6),
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'LabCRM_DB'
);

CREATE OR REPLACE TABLE BRONZE.CRM_OPPORTUNITIES (
    OPP_ID          NUMBER          NOT NULL,
    ACCOUNT_ID      NUMBER          NOT NULL,
    TITLE           VARCHAR(200)    NOT NULL,
    STAGE           VARCHAR(40)     NOT NULL,
    AMOUNT_USD      NUMBER(18,2)    NOT NULL,
    CLOSE_DATE      DATE,
    _DMS_OPERATION  VARCHAR(1),
    _DMS_COMMIT_TS  TIMESTAMP_NTZ(6),
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'LabCRM_DB'
);

CREATE OR REPLACE TABLE BRONZE.CRM_ACTIVITY_LOG (
    LOG_ID          NUMBER          NOT NULL,
    ENTITY          VARCHAR(50)     NOT NULL,
    ENTITY_KEY      VARCHAR(100)    NOT NULL,
    NOTE            VARCHAR(500),
    LOGGED_AT       TIMESTAMP_NTZ(3),
    _DMS_OPERATION  VARCHAR(1),
    _DMS_COMMIT_TS  TIMESTAMP_NTZ(6),
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'LabCRM_DB'
);


-- =============================================================================
-- SECTION 15: BRONZE LAYER — LabInventory_DB
-- Source: snowflake_ddl/bronze/04_inventory_db.sql
-- =============================================================================

CREATE OR REPLACE TABLE BRONZE.INV_WAREHOUSES (
    WH_ID           NUMBER          NOT NULL,   -- INT IDENTITY
    WH_CODE         VARCHAR(20)     NOT NULL,   -- NVARCHAR(20) UNIQUE
    LOCATION        VARCHAR(200)    NOT NULL,
    _DMS_OPERATION  VARCHAR(1),
    _DMS_COMMIT_TS  TIMESTAMP_NTZ(6),
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'LabInventory_DB'
);

CREATE OR REPLACE TABLE BRONZE.INV_SKU (
    SKU_ID          NUMBER          NOT NULL,   -- INT IDENTITY
    SKU_CODE        VARCHAR(40)     NOT NULL,   -- NVARCHAR(40) UNIQUE
    DESCR           VARCHAR(200)    NOT NULL,
    UNIT_COST       NUMBER(18,4)    NOT NULL,
    _DMS_OPERATION  VARCHAR(1),
    _DMS_COMMIT_TS  TIMESTAMP_NTZ(6),
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'LabInventory_DB'
);

CREATE OR REPLACE TABLE BRONZE.INV_STOCK_MOVEMENTS (
    MOV_ID          NUMBER          NOT NULL,   -- BIGINT IDENTITY
    WH_ID           NUMBER          NOT NULL,   -- FK → Warehouses
    SKU_ID          NUMBER          NOT NULL,   -- FK → Sku
    QTY_CHANGE      NUMBER          NOT NULL,   -- INT (positive=in, negative=out)
    REASON          VARCHAR(80)     NOT NULL,
    MOV_DATE        TIMESTAMP_NTZ(3),           -- DATETIME2(3)
    _DMS_OPERATION  VARCHAR(1),
    _DMS_COMMIT_TS  TIMESTAMP_NTZ(6),
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'LabInventory_DB'
);


-- =============================================================================
-- SECTION 16: SILVER LAYER — SnowConvertStressDB
-- Source: snowflake_ddl/silver/01_stress_db.sql
-- NOTE: SCD Type-2 tables (CUSTOMERS, PRODUCTS, ORDERS) are managed by dbt
--       snapshots. DDL below creates the shell; dbt will own it after first run.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS MSSQL_MIGRATION_LAB.SILVER;

CREATE TABLE IF NOT EXISTS SILVER.CATEGORIES (
    CATEGORY_ID     NUMBER          NOT NULL PRIMARY KEY,
    CATEGORY_NAME   VARCHAR(100)    NOT NULL,
    DESCRIPTION     VARCHAR(500),
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'SnowConvertStressDB'
);

-- SCD Type-2 — managed by dbt snapshot snp_customers
CREATE TABLE IF NOT EXISTS SILVER.CUSTOMERS (
    CUSTOMER_ID     NUMBER          NOT NULL,
    CUSTOMER_CODE   VARCHAR(20)     NOT NULL,
    FULL_NAME       VARCHAR(200)    NOT NULL,
    EMAIL           VARCHAR(320),
    COUNTRY         VARCHAR(100)    NOT NULL,
    CREATED_AT      TIMESTAMP_NTZ(3),
    _SOURCE_DB      VARCHAR(128)    NOT NULL,
    -- dbt snapshot columns (auto-managed):
    dbt_scd_id      VARCHAR         NOT NULL,
    dbt_valid_from  TIMESTAMP_NTZ   NOT NULL,
    dbt_valid_to    TIMESTAMP_NTZ,              -- NULL = current record
    dbt_updated_at  TIMESTAMP_NTZ   NOT NULL
);

-- SCD Type-2 — managed by dbt snapshot snp_products
CREATE TABLE IF NOT EXISTS SILVER.PRODUCTS (
    PRODUCT_ID      NUMBER          NOT NULL,
    CATEGORY_ID     NUMBER          NOT NULL,
    SKU             VARCHAR(50)     NOT NULL,
    PRODUCT_NAME    VARCHAR(200)    NOT NULL,
    LIST_PRICE      NUMBER(18,4)    NOT NULL,
    IS_ACTIVE       BOOLEAN         NOT NULL,
    _SOURCE_DB      VARCHAR(128)    NOT NULL,
    -- dbt snapshot columns (auto-managed):
    dbt_scd_id      VARCHAR         NOT NULL,
    dbt_valid_from  TIMESTAMP_NTZ   NOT NULL,
    dbt_valid_to    TIMESTAMP_NTZ,
    dbt_updated_at  TIMESTAMP_NTZ   NOT NULL
);

-- SCD Type-2 — managed by dbt snapshot snp_orders
CREATE TABLE IF NOT EXISTS SILVER.ORDERS (
    ORDER_ID        NUMBER          NOT NULL,
    CUSTOMER_ID     NUMBER          NOT NULL,
    ORDER_DATE      DATE            NOT NULL,
    STATUS          VARCHAR(30)     NOT NULL,
    TOTAL_AMOUNT    NUMBER(18,4)    NOT NULL,
    NOTES           VARCHAR(500),
    _SOURCE_DB      VARCHAR(128)    NOT NULL,
    -- dbt snapshot columns (auto-managed):
    dbt_scd_id      VARCHAR         NOT NULL,
    dbt_valid_from  TIMESTAMP_NTZ   NOT NULL,
    dbt_valid_to    TIMESTAMP_NTZ,
    dbt_updated_at  TIMESTAMP_NTZ   NOT NULL
);

-- Append-only — no SCD2
CREATE TABLE IF NOT EXISTS SILVER.ORDER_ITEMS (
    ORDER_ITEM_ID   NUMBER          NOT NULL PRIMARY KEY,
    ORDER_ID        NUMBER          NOT NULL,
    PRODUCT_ID      NUMBER          NOT NULL,
    QUANTITY        NUMBER          NOT NULL,
    UNIT_PRICE      NUMBER(18,4)    NOT NULL,
    LINE_TOTAL      NUMBER(18,4)    NOT NULL,   -- resolved computed col
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'SnowConvertStressDB'
);


-- =============================================================================
-- SECTION 17: SILVER LAYER — LabERP_DB
-- Source: snowflake_ddl/silver/02_erp_db.sql
-- NOTE: ERP_EMPLOYEES is managed by dbt snapshot snp_erp_employees.
-- =============================================================================

-- Reference / static — no SCD2
CREATE TABLE IF NOT EXISTS SILVER.ERP_DEPARTMENTS (
    DEPT_ID         NUMBER          NOT NULL PRIMARY KEY,
    DEPT_CODE       VARCHAR(20)     NOT NULL,
    DEPT_NAME       VARCHAR(120)    NOT NULL,
    BUDGET_USD      NUMBER(18,2)    NOT NULL,
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'LabERP_DB'
);

-- SCD Type-2 — managed by dbt snapshot snp_erp_employees
CREATE TABLE IF NOT EXISTS SILVER.ERP_EMPLOYEES (
    EMP_ID          NUMBER          NOT NULL,
    DEPT_ID         NUMBER          NOT NULL,
    EMP_CODE        VARCHAR(20)     NOT NULL,
    FULL_NAME       VARCHAR(200)    NOT NULL,
    HIRE_DATE       DATE            NOT NULL,
    SALARY          NUMBER(18,4)    NOT NULL,
    _SOURCE_DB      VARCHAR(128)    NOT NULL,
    -- dbt snapshot columns (auto-managed):
    dbt_scd_id      VARCHAR         NOT NULL,
    dbt_valid_from  TIMESTAMP_NTZ   NOT NULL,
    dbt_valid_to    TIMESTAMP_NTZ,              -- NULL = current record
    dbt_updated_at  TIMESTAMP_NTZ   NOT NULL
);

-- Append-only payroll journal
CREATE TABLE IF NOT EXISTS SILVER.ERP_PAYROLL_LINES (
    LINE_ID         NUMBER          NOT NULL PRIMARY KEY,
    RUN_ID          NUMBER          NOT NULL,
    EMP_ID          NUMBER          NOT NULL,
    RUN_MONTH       CHAR(7)         NOT NULL,   -- 'YYYY-MM' denormalised
    GROSS_PAY       NUMBER(18,4)    NOT NULL,
    NET_PAY         NUMBER(18,4)    NOT NULL,   -- resolved: GROSS_PAY * 0.92
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'LabERP_DB'
);


-- =============================================================================
-- SECTION 18: SILVER LAYER — LabCRM_DB
-- Source: snowflake_ddl/silver/03_crm_db.sql
-- NOTE: CRM_ACCOUNTS and CRM_OPPORTUNITIES managed by dbt snapshots.
-- =============================================================================

-- SCD Type-2 — managed by dbt snapshot snp_crm_accounts
CREATE TABLE IF NOT EXISTS SILVER.CRM_ACCOUNTS (
    ACCOUNT_ID      NUMBER          NOT NULL,
    ACCOUNT_CODE    VARCHAR(30)     NOT NULL,
    NAME            VARCHAR(200)    NOT NULL,
    REGION          VARCHAR(50)     NOT NULL,
    CREATED_AT      TIMESTAMP_NTZ(3),
    _SOURCE_DB      VARCHAR(128)    NOT NULL,
    -- dbt snapshot columns (auto-managed):
    dbt_scd_id      VARCHAR         NOT NULL,
    dbt_valid_from  TIMESTAMP_NTZ   NOT NULL,
    dbt_valid_to    TIMESTAMP_NTZ,              -- NULL = current record
    dbt_updated_at  TIMESTAMP_NTZ   NOT NULL
);

-- Append-only reference — no SCD2
CREATE TABLE IF NOT EXISTS SILVER.CRM_CONTACTS (
    CONTACT_ID      NUMBER          NOT NULL PRIMARY KEY,
    ACCOUNT_ID      NUMBER          NOT NULL,
    EMAIL           VARCHAR(320)    NOT NULL,
    FULL_NAME       VARCHAR(200)    NOT NULL,
    IS_PRIMARY      BOOLEAN         NOT NULL,
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'LabCRM_DB'
);

-- SCD Type-2 — managed by dbt snapshot snp_crm_opportunities
-- invalidate_hard_deletes=false — closed/lost opps are never expired
CREATE TABLE IF NOT EXISTS SILVER.CRM_OPPORTUNITIES (
    OPP_ID          NUMBER          NOT NULL,
    ACCOUNT_ID      NUMBER          NOT NULL,
    TITLE           VARCHAR(200)    NOT NULL,
    STAGE           VARCHAR(40)     NOT NULL,
    AMOUNT_USD      NUMBER(18,2)    NOT NULL,
    CLOSE_DATE      DATE,
    _SOURCE_DB      VARCHAR(128)    NOT NULL,
    -- dbt snapshot columns (auto-managed):
    dbt_scd_id      VARCHAR         NOT NULL,
    dbt_valid_from  TIMESTAMP_NTZ   NOT NULL,
    dbt_valid_to    TIMESTAMP_NTZ,
    dbt_updated_at  TIMESTAMP_NTZ   NOT NULL
);


-- =============================================================================
-- SECTION 19: SILVER LAYER — LabInventory_DB
-- Source: snowflake_ddl/silver/04_inventory_db.sql
-- NOTE: INV_SKU managed by dbt snapshot snp_inv_sku.
--       INV_STOCK_BALANCE removed — replaced by GOLD.FCT_INVENTORY_POSITION.
-- =============================================================================

-- Reference / static — no SCD2
CREATE TABLE IF NOT EXISTS SILVER.INV_WAREHOUSES (
    WH_ID           NUMBER          NOT NULL PRIMARY KEY,
    WH_CODE         VARCHAR(20)     NOT NULL,
    LOCATION        VARCHAR(200)    NOT NULL,
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'LabInventory_DB'
);

-- SCD Type-2 — managed by dbt snapshot snp_inv_sku
CREATE TABLE IF NOT EXISTS SILVER.INV_SKU (
    SKU_ID          NUMBER          NOT NULL,
    SKU_CODE        VARCHAR(40)     NOT NULL,
    DESCR           VARCHAR(200)    NOT NULL,
    UNIT_COST       NUMBER(18,4)    NOT NULL,
    _SOURCE_DB      VARCHAR(128)    NOT NULL,
    -- dbt snapshot columns (auto-managed):
    dbt_scd_id      VARCHAR         NOT NULL,
    dbt_valid_from  TIMESTAMP_NTZ   NOT NULL,
    dbt_valid_to    TIMESTAMP_NTZ,              -- NULL = current / active price
    dbt_updated_at  TIMESTAMP_NTZ   NOT NULL
);

-- Append-only movement journal
CREATE TABLE IF NOT EXISTS SILVER.INV_STOCK_MOVEMENTS (
    MOV_ID          NUMBER          NOT NULL PRIMARY KEY,
    WH_ID           NUMBER          NOT NULL,
    SKU_ID          NUMBER          NOT NULL,
    QTY_CHANGE      NUMBER          NOT NULL,
    REASON          VARCHAR(80)     NOT NULL,
    MOV_DATE        TIMESTAMP_NTZ(3),
    _LOADED_AT      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_DB      VARCHAR(128)    NOT NULL DEFAULT 'LabInventory_DB'
);


-- =============================================================================
-- SECTION 20: GOLD LAYER — Cross-domain analytics
-- Source: snowflake_ddl/gold/01_analytics.sql
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS MSSQL_MIGRATION_LAB.GOLD;

-- Sales summary (from SnowConvertStressDB)
CREATE OR REPLACE TABLE GOLD.FACT_ORDERS (
    ORDER_SK            NUMBER          NOT NULL,
    ORDER_ID            NUMBER          NOT NULL,
    ORDER_DATE          DATE            NOT NULL,
    ORDER_MONTH         CHAR(7)         NOT NULL,   -- derived: FORMAT(order_date, 'yyyy-MM')
    CUSTOMER_ID         NUMBER          NOT NULL,
    CUSTOMER_CODE       VARCHAR(20),
    CUSTOMER_NAME       VARCHAR(200),
    COUNTRY             VARCHAR(100),
    STATUS              VARCHAR(30)     NOT NULL,
    LINE_COUNT          NUMBER          NOT NULL DEFAULT 0,
    TOTAL_AMOUNT        NUMBER(18,4)    NOT NULL,
    _LOADED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE GOLD.FACT_ORDER_ITEMS (
    ORDER_ITEM_SK       NUMBER          NOT NULL,
    ORDER_ID            NUMBER          NOT NULL,
    ORDER_DATE          DATE            NOT NULL,
    PRODUCT_ID          NUMBER          NOT NULL,
    SKU                 VARCHAR(50),
    PRODUCT_NAME        VARCHAR(200),
    CATEGORY_NAME       VARCHAR(100),
    QUANTITY            NUMBER          NOT NULL,
    UNIT_PRICE          NUMBER(18,4)    NOT NULL,
    LINE_TOTAL          NUMBER(18,4)    NOT NULL,
    _LOADED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- ERP payroll summary
CREATE OR REPLACE TABLE GOLD.FACT_PAYROLL_SUMMARY (
    RUN_MONTH           CHAR(7)         NOT NULL,
    DEPT_CODE           VARCHAR(20),
    DEPT_NAME           VARCHAR(120),
    EMPLOYEE_COUNT      NUMBER          NOT NULL DEFAULT 0,
    TOTAL_GROSS_PAY     NUMBER(18,4)    NOT NULL DEFAULT 0,
    TOTAL_NET_PAY       NUMBER(18,4)    NOT NULL DEFAULT 0,
    _LOADED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- CRM pipeline summary
CREATE OR REPLACE TABLE GOLD.FACT_CRM_PIPELINE (
    REGION              VARCHAR(50)     NOT NULL,
    STAGE               VARCHAR(40)     NOT NULL,
    OPP_COUNT           NUMBER          NOT NULL DEFAULT 0,
    TOTAL_AMOUNT_USD    NUMBER(18,2)    NOT NULL DEFAULT 0,
    AVG_AMOUNT_USD      NUMBER(18,2),
    _LOADED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- Inventory position
CREATE OR REPLACE TABLE GOLD.FACT_INVENTORY_POSITION (
    WH_CODE             VARCHAR(20)     NOT NULL,
    SKU_CODE            VARCHAR(40)     NOT NULL,
    DESCR               VARCHAR(200),
    UNIT_COST           NUMBER(18,4),
    CURRENT_QTY         NUMBER          NOT NULL DEFAULT 0,
    STOCK_VALUE_USD     NUMBER(18,4),   -- CURRENT_QTY * UNIT_COST
    _LOADED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- Cross-domain dimension: DATE
CREATE OR REPLACE TABLE GOLD.DIM_DATE (
    DATE_KEY            NUMBER          NOT NULL PRIMARY KEY,   -- YYYYMMDD int
    FULL_DATE           DATE            NOT NULL,
    YEAR                NUMBER          NOT NULL,
    QUARTER             NUMBER          NOT NULL,
    MONTH               NUMBER          NOT NULL,
    MONTH_NAME          VARCHAR(12)     NOT NULL,
    WEEK_OF_YEAR        NUMBER          NOT NULL,
    DAY_OF_WEEK         NUMBER          NOT NULL,
    DAY_NAME            VARCHAR(12)     NOT NULL,
    IS_WEEKEND          BOOLEAN         NOT NULL
);


-- =============================================================================
-- SECTION 21: STORED PROCEDURES — Basic (SnowConvertStressDB)
-- Source: snowflake_ddl/procedures/01_basic_procs.sql
-- =============================================================================

USE DATABASE MSSQL_MIGRATION_LAB;
USE SCHEMA BRONZE;

CREATE OR REPLACE PROCEDURE BRONZE.SP_REFRESH_ORDER_TOTALS(ORDER_ID NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    IF (ORDER_ID IS NOT NULL) THEN
        UPDATE BRONZE.ORDERS o
        SET TOTAL_AMOUNT = (
            SELECT COALESCE(SUM(LINE_TOTAL), 0)
            FROM BRONZE.ORDER_ITEMS
            WHERE ORDER_ID = o.ORDER_ID
        )
        WHERE o.ORDER_ID = :ORDER_ID;
    ELSE
        UPDATE BRONZE.ORDERS o
        SET TOTAL_AMOUNT = (
            SELECT COALESCE(SUM(LINE_TOTAL), 0)
            FROM BRONZE.ORDER_ITEMS i
            WHERE i.ORDER_ID = o.ORDER_ID
        );
    END IF;

    RETURN 'OK';
END;
$$;


CREATE OR REPLACE PROCEDURE BRONZE.SP_LIST_OPEN_ORDERS()
RETURNS TABLE()
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET DEFAULT (
        SELECT ORDER_ID, CUSTOMER_ID, ORDER_DATE, STATUS, TOTAL_AMOUNT
        FROM BRONZE.ORDERS
        WHERE STATUS = 'Open'
        ORDER BY ORDER_DATE
    );
BEGIN
    RETURN TABLE(res);
END;
$$;


-- =============================================================================
-- SECTION 22: STORED PROCEDURES — Stress / SnowConvertStressDB
-- Source: snowflake_ddl/procedures/02_stress_procs.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE BRONZE.SP_DYNAMIC_SEARCH_ORDERS(
    P_STATUS     VARCHAR,
    P_COUNTRY    VARCHAR,
    P_MIN_TOTAL  NUMBER,
    P_SORT_COL   VARCHAR,
    P_SORT_DIR   VARCHAR
)
RETURNS TABLE()
LANGUAGE SQL
AS
$$
DECLARE
    v_safe_col  VARCHAR;
    v_safe_dir  VARCHAR;
    v_where     VARCHAR DEFAULT '';
    v_sql       VARCHAR;
    res         RESULTSET;
BEGIN
    v_safe_col := CASE
        WHEN UPPER(P_SORT_COL) IN ('ORDER_DATE','TOTAL_AMOUNT','ORDER_ID','STATUS')
            THEN UPPER(P_SORT_COL)
        ELSE 'ORDER_DATE'
    END;
    v_safe_dir := CASE WHEN UPPER(P_SORT_DIR) = 'ASC' THEN 'ASC' ELSE 'DESC' END;

    IF (P_STATUS IS NOT NULL) THEN
        v_where := v_where || ' AND o.STATUS = ''' || P_STATUS || '''';
    END IF;
    IF (P_COUNTRY IS NOT NULL) THEN
        v_where := v_where || ' AND c.COUNTRY = ''' || P_COUNTRY || '''';
    END IF;
    IF (P_MIN_TOTAL IS NOT NULL) THEN
        v_where := v_where || ' AND o.TOTAL_AMOUNT >= ' || P_MIN_TOTAL;
    END IF;

    v_sql := '
        SELECT o.ORDER_ID, o.ORDER_DATE, o.STATUS, o.TOTAL_AMOUNT,
               c.CUSTOMER_CODE, c.COUNTRY
        FROM BRONZE.ORDERS o
        JOIN BRONZE.CUSTOMERS c ON c.CUSTOMER_ID = o.CUSTOMER_ID
        WHERE 1=1' || v_where ||
        ' ORDER BY ' || v_safe_col || ' ' || v_safe_dir;

    res := (EXECUTE IMMEDIATE :v_sql);
    RETURN TABLE(res);
END;
$$;


CREATE OR REPLACE PROCEDURE BRONZE.SP_REPRICE_PRODUCTS_BY_CATEGORY(
    P_CATEGORY_ID  NUMBER,
    P_PCT_BUMP     NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE BRONZE.PRODUCTS
    SET LIST_PRICE = ROUND(LIST_PRICE * (1 + :P_PCT_BUMP), 4)
    WHERE CATEGORY_ID = :P_CATEGORY_ID
      AND IS_ACTIVE = TRUE;

    RETURN 'Updated ' || SQLROWCOUNT || ' products';
END;
$$;


CREATE OR REPLACE PROCEDURE BRONZE.SP_MERGE_UPSERT_CUSTOMERS(P_SOURCE_JSON VARIANT)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO BRONZE.CUSTOMERS tgt
    USING (
        SELECT
            value:CustomerCode::VARCHAR  AS CUSTOMER_CODE,
            value:FullName::VARCHAR      AS FULL_NAME,
            value:Email::VARCHAR         AS EMAIL,
            value:Country::VARCHAR       AS COUNTRY
        FROM TABLE(FLATTEN(input => :P_SOURCE_JSON))
    ) src ON tgt.CUSTOMER_CODE = src.CUSTOMER_CODE
    WHEN MATCHED THEN
        UPDATE SET
            FULL_NAME = src.FULL_NAME,
            EMAIL     = src.EMAIL,
            COUNTRY   = src.COUNTRY
    WHEN NOT MATCHED THEN
        INSERT (CUSTOMER_CODE, FULL_NAME, EMAIL, COUNTRY)
        VALUES (src.CUSTOMER_CODE, src.FULL_NAME, src.EMAIL,
                COALESCE(src.COUNTRY, 'US'));

    RETURN 'Merged ' || SQLROWCOUNT || ' customer rows';
END;
$$;


CREATE OR REPLACE PROCEDURE BRONZE.SP_JSON_ORDER_LINES(P_ORDER_ID NUMBER)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    v_result VARIANT;
BEGIN
    SELECT ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'order_item_id', oi.ORDER_ITEM_ID,
            'product_id',    oi.PRODUCT_ID,
            'quantity',      oi.QUANTITY,
            'unit_price',    oi.UNIT_PRICE,
            'line_total',    oi.LINE_TOTAL,
            'sku',           p.SKU
        )
    ) INTO v_result
    FROM BRONZE.ORDER_ITEMS oi
    JOIN BRONZE.PRODUCTS p ON p.PRODUCT_ID = oi.PRODUCT_ID
    WHERE oi.ORDER_ID = :P_ORDER_ID;

    RETURN v_result;
END;
$$;


CREATE OR REPLACE PROCEDURE BRONZE.SP_CHAINED_C_INNER(P_ORDER_ID NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE BRONZE.ORDERS
    SET NOTES = COALESCE(NOTES, '') || ' [C]'
    WHERE ORDER_ID = :P_ORDER_ID;

    RETURN 'C';
END;
$$;

CREATE OR REPLACE PROCEDURE BRONZE.SP_CHAINED_B_MIDDLE(P_ORDER_ID NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_result VARCHAR;
BEGIN
    CALL BRONZE.SP_CHAINED_C_INNER(:P_ORDER_ID) INTO v_result;

    UPDATE BRONZE.ORDERS
    SET NOTES = COALESCE(NOTES, '') || ' [B]'
    WHERE ORDER_ID = :P_ORDER_ID;

    RETURN v_result || 'B';
END;
$$;

CREATE OR REPLACE PROCEDURE BRONZE.SP_CHAINED_A_OUTER(P_ORDER_ID NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_result VARCHAR;
BEGIN
    CALL BRONZE.SP_CHAINED_B_MIDDLE(:P_ORDER_ID) INTO v_result;

    UPDATE BRONZE.ORDERS
    SET NOTES = COALESCE(NOTES, '') || ' [A]'
    WHERE ORDER_ID = :P_ORDER_ID;

    RETURN v_result || 'A';
END;
$$;


CREATE OR REPLACE PROCEDURE BRONZE.SP_BUILD_CATEGORY_CLOSURE()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    TRUNCATE TABLE BRONZE.CATEGORY_CLOSURE;

    INSERT INTO BRONZE.CATEGORY_CLOSURE (ANCESTOR_ID, DESCENDANT_ID, DEPTH)
    SELECT CATEGORY_ID, CATEGORY_ID, 0
    FROM BRONZE.CATEGORIES;

    INSERT INTO BRONZE.CATEGORY_CLOSURE (ANCESTOR_ID, DESCENDANT_ID, DEPTH)
    WITH edges AS (
        SELECT p.CATEGORY_ID AS parent_id, c.CATEGORY_ID AS child_id
        FROM BRONZE.CATEGORIES p
        JOIN BRONZE.CATEGORIES c ON c.CATEGORY_ID > p.CATEGORY_ID
        WHERE ABS(HASH(p.CATEGORY_ID, c.CATEGORY_ID)) % 3 = 0
    ),
    walk (ancestor_id, descendant_id, depth) AS (
        SELECT parent_id, child_id, 1 FROM edges
        UNION ALL
        SELECT w.ancestor_id, e.child_id, w.depth + 1
        FROM walk w
        JOIN edges e ON e.parent_id = w.descendant_id
        WHERE w.depth < 5
    )
    SELECT DISTINCT ancestor_id, descendant_id, depth
    FROM walk
    WHERE NOT EXISTS (
        SELECT 1 FROM BRONZE.CATEGORY_CLOSURE cc
        WHERE cc.ANCESTOR_ID = walk.ancestor_id
          AND cc.DESCENDANT_ID = walk.descendant_id
    );

    RETURN (SELECT COUNT(*)::VARCHAR FROM BRONZE.CATEGORY_CLOSURE) || ' closure rows';
END;
$$;


CREATE OR REPLACE PROCEDURE BRONZE.SP_THROW_CATCH_RETHROW()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    stress_error EXCEPTION (-20001, 'Stress: intentional error for migration harness');
BEGIN
    RAISE stress_error;
EXCEPTION
    WHEN stress_error THEN
        INSERT INTO BRONZE.MIGRATION_AUDIT_LOG
            (TABLE_NAME, DML_ACTION, ROW_PK, NEW_PAYLOAD, SQL_TRIGGER)
        VALUES
            ('_ERROR_', 'CATCH', '', PARSE_JSON('"' || SQLERRM || '"'),
             'SP_THROW_CATCH_RETHROW');
        RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE BRONZE.SP_UPDATE_PRICE_WITH_HISTORY(
    P_PRODUCT_ID  NUMBER,
    P_NEW_PRICE   NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_old_price NUMBER(18,4);
BEGIN
    SELECT LIST_PRICE INTO v_old_price
    FROM BRONZE.PRODUCTS
    WHERE PRODUCT_ID = :P_PRODUCT_ID;

    UPDATE BRONZE.PRODUCTS
    SET LIST_PRICE = :P_NEW_PRICE
    WHERE PRODUCT_ID = :P_PRODUCT_ID;

    INSERT INTO BRONZE.PRODUCT_PRICE_HISTORY (PRODUCT_ID, OLD_PRICE, NEW_PRICE)
    VALUES (:P_PRODUCT_ID, v_old_price, :P_NEW_PRICE);

    RETURN 'old=' || v_old_price || ' new=' || P_NEW_PRICE;
END;
$$;


CREATE OR REPLACE PROCEDURE BRONZE.SP_WHILE_BATCH_NUMBERS(P_ITERATIONS NUMBER)
RETURNS OBJECT
LANGUAGE SQL
AS
$$
DECLARE
    v_cap NUMBER DEFAULT 1000;
BEGIN
    v_cap := LEAST(COALESCE(:P_ITERATIONS, 1000), 5000);
    RETURN OBJECT_CONSTRUCT(
        'num_rows', v_cap,
        'sum_n',    v_cap * (v_cap + 1) / 2
    );
END;
$$;


CREATE OR REPLACE PROCEDURE BRONZE.SP_WAITFOR_SHORT()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    CALL SYSTEM$WAIT(1, 'MILLISECONDS');
    RETURN 'OK';
END;
$$;


CREATE OR REPLACE PROCEDURE BRONZE.SP_DYNAMIC_PIVOT()
RETURNS TABLE()
LANGUAGE SQL
AS
$$
DECLARE
    v_cols  VARCHAR;
    v_sql   VARCHAR;
    res     RESULTSET;
BEGIN
    SELECT LISTAGG(DISTINCT '''' || STATUS || '''', ',')
           WITHIN GROUP (ORDER BY STATUS)
    INTO v_cols
    FROM BRONZE.ORDERS;

    IF (v_cols IS NULL OR v_cols = '') THEN
        v_cols := '''Open''';
    END IF;

    v_sql := '
        SELECT *
        FROM BRONZE.ORDERS
        PIVOT (SUM(TOTAL_AMOUNT) FOR STATUS IN (' || v_cols || ')) p';

    res := (EXECUTE IMMEDIATE :v_sql);
    RETURN TABLE(res);
END;
$$;


CREATE OR REPLACE PROCEDURE BRONZE.SP_TVP_APPEND_ORDER_LINES(
    P_ORDER_ID   NUMBER,
    P_LINES      VARIANT
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO BRONZE.ORDER_ITEMS (ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE)
    SELECT
        :P_ORDER_ID,
        value:ProductId::NUMBER,
        value:Quantity::NUMBER,
        value:UnitPrice::NUMBER(18,4)
    FROM TABLE(FLATTEN(input => :P_LINES));

    CALL BRONZE.SP_REFRESH_ORDER_TOTALS(:P_ORDER_ID);

    RETURN 'Inserted ' || SQLROWCOUNT || ' lines';
END;
$$;


CREATE OR REPLACE PROCEDURE BRONZE.SP_PATCH_PRODUCTS_FROM_JSON(P_PATCH_JSON VARIANT)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE BRONZE.PRODUCTS p
    SET
        LIST_PRICE = COALESCE(src.NEW_PRICE::NUMBER(18,4), p.LIST_PRICE),
        IS_ACTIVE  = COALESCE(src.IS_ACTIVE::BOOLEAN, p.IS_ACTIVE)
    FROM (
        SELECT
            value:ProductId::NUMBER     AS PRODUCT_ID,
            value:NewPrice              AS NEW_PRICE,
            value:IsActive              AS IS_ACTIVE
        FROM TABLE(FLATTEN(input => :P_PATCH_JSON))
    ) src
    WHERE p.PRODUCT_ID = src.PRODUCT_ID;

    RETURN 'Patched ' || SQLROWCOUNT || ' products';
END;
$$;


CREATE OR REPLACE PROCEDURE BRONZE.SP_MULTI_RESULT_DEMO(P_CUSTOMER_ID NUMBER)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    v_customer  VARIANT;
    v_orders    VARIANT;
    v_items     VARIANT;
BEGIN
    SELECT OBJECT_CONSTRUCT(
        'CUSTOMER_ID',   CUSTOMER_ID,
        'CUSTOMER_CODE', CUSTOMER_CODE,
        'FULL_NAME',     FULL_NAME,
        'COUNTRY',       COUNTRY
    ) INTO :v_customer
    FROM BRONZE.CUSTOMERS
    WHERE CUSTOMER_ID = :P_CUSTOMER_ID;

    SELECT ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'ORDER_ID',     ORDER_ID,
            'ORDER_DATE',   ORDER_DATE,
            'STATUS',       STATUS,
            'TOTAL_AMOUNT', TOTAL_AMOUNT
        )
    ) WITHIN GROUP (ORDER BY ORDER_DATE DESC)
    INTO :v_orders
    FROM BRONZE.ORDERS
    WHERE CUSTOMER_ID = :P_CUSTOMER_ID;

    SELECT ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'ORDER_ITEM_ID', oi.ORDER_ITEM_ID,
            'ORDER_ID',      oi.ORDER_ID,
            'PRODUCT_ID',    oi.PRODUCT_ID,
            'QUANTITY',      oi.QUANTITY,
            'LINE_TOTAL',    oi.LINE_TOTAL
        )
    ) INTO :v_items
    FROM BRONZE.ORDER_ITEMS oi
    JOIN BRONZE.ORDERS o ON o.ORDER_ID = oi.ORDER_ID
    WHERE o.CUSTOMER_ID = :P_CUSTOMER_ID;

    RETURN OBJECT_CONSTRUCT(
        'customer', v_customer,
        'orders',   v_orders,
        'items',    v_items
    );
END;
$$;


-- =============================================================================
-- SECTION 23: STORED PROCEDURES — ERP
-- Source: snowflake_ddl/procedures/03_erp_procs.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE BRONZE.SP_ERP_DYNAMIC_DEPT_REPORT(
    P_MIN_SALARY  NUMBER,
    P_ORDER_BY    VARCHAR
)
RETURNS TABLE()
LANGUAGE SQL
AS
$$
DECLARE
    v_safe_ord  VARCHAR;
    v_where     VARCHAR DEFAULT '';
    v_sql       VARCHAR;
    res         RESULTSET;
BEGIN
    v_safe_ord := CASE
        WHEN UPPER(P_ORDER_BY) IN ('SALARY', 'HIRE_DATE', 'EMP_CODE')
            THEN UPPER(P_ORDER_BY)
        ELSE 'SALARY'
    END;

    IF (P_MIN_SALARY IS NOT NULL) THEN
        v_where := ' AND e.SALARY >= ' || P_MIN_SALARY;
    END IF;

    v_sql := '
        SELECT e.EMP_CODE, e.FULL_NAME, d.DEPT_CODE, e.SALARY
        FROM BRONZE.ERP_EMPLOYEES e
        JOIN BRONZE.ERP_DEPARTMENTS d ON d.DEPT_ID = e.DEPT_ID
        WHERE 1=1' || v_where ||
        ' ORDER BY ' || v_safe_ord;

    res := (EXECUTE IMMEDIATE :v_sql);
    RETURN TABLE(res);
END;
$$;


CREATE OR REPLACE PROCEDURE BRONZE.SP_ERP_CLOSE_PAYROLL_RUN(P_RUN_ID NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE BRONZE.ERP_PAYROLL_RUNS
    SET STATUS = 'Closed'
    WHERE RUN_ID = :P_RUN_ID;

    RETURN CASE WHEN SQLROWCOUNT > 0 THEN 'Closed' ELSE 'Not found' END;
END;
$$;


-- =============================================================================
-- SECTION 24: STORED PROCEDURES — CRM
-- Source: snowflake_ddl/procedures/04_crm_procs.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE BRONZE.SP_CRM_MERGE_ACCOUNTS_FROM_JSON(P_JSON VARIANT)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO BRONZE.CRM_ACCOUNTS tgt
    USING (
        SELECT
            value:AccountCode::VARCHAR  AS ACCOUNT_CODE,
            value:Name::VARCHAR         AS NAME,
            value:Region::VARCHAR       AS REGION
        FROM TABLE(FLATTEN(input => :P_JSON))
    ) src ON tgt.ACCOUNT_CODE = src.ACCOUNT_CODE
    WHEN MATCHED THEN
        UPDATE SET NAME = src.NAME, REGION = src.REGION
    WHEN NOT MATCHED THEN
        INSERT (ACCOUNT_CODE, NAME, REGION)
        VALUES (src.ACCOUNT_CODE, src.NAME, src.REGION);

    RETURN 'Merged ' || SQLROWCOUNT || ' accounts';
END;
$$;


CREATE OR REPLACE PROCEDURE BRONZE.SP_CRM_LIST_PIPELINE(P_REGION VARCHAR)
RETURNS TABLE()
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET DEFAULT (
        SELECT o.OPP_ID, a.ACCOUNT_CODE, o.TITLE, o.STAGE, o.AMOUNT_USD
        FROM BRONZE.CRM_OPPORTUNITIES o
        JOIN BRONZE.CRM_ACCOUNTS a ON a.ACCOUNT_ID = o.ACCOUNT_ID
        WHERE :P_REGION IS NULL OR a.REGION = :P_REGION
        ORDER BY o.AMOUNT_USD DESC
    );
BEGIN
    RETURN TABLE(res);
END;
$$;


CREATE OR REPLACE PROCEDURE BRONZE.SP_CRM_UPDATE_OPPORTUNITY_STAGE(
    P_OPP_ID    NUMBER,
    P_NEW_STAGE VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_current VARCHAR;
    v_exists  NUMBER;
BEGIN
    SELECT COUNT(*) INTO :v_exists
    FROM BRONZE.CRM_OPPORTUNITIES
    WHERE OPP_ID = :P_OPP_ID;

    IF (v_exists = 0) THEN
        RETURN 'ERROR: opportunity not found';
    END IF;

    SELECT STAGE INTO :v_current
    FROM BRONZE.CRM_OPPORTUNITIES
    WHERE OPP_ID = :P_OPP_ID;

    IF (v_current = 'Won' AND :P_NEW_STAGE != 'Won') THEN
        RETURN 'ERROR: cannot move backwards from Won';
    END IF;

    UPDATE BRONZE.CRM_OPPORTUNITIES
    SET STAGE = :P_NEW_STAGE
    WHERE OPP_ID = :P_OPP_ID;

    RETURN 'Stage updated to ' || P_NEW_STAGE;
END;
$$;


-- =============================================================================
-- SECTION 25: STORED PROCEDURES — Inventory
-- Source: snowflake_ddl/procedures/05_inventory_procs.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE BRONZE.SP_INV_DYNAMIC_WH_FILTER(P_WH_CODE VARCHAR)
RETURNS TABLE()
LANGUAGE SQL
AS
$$
DECLARE
    v_sql VARCHAR;
    res   RESULTSET;
BEGIN
    v_sql := '
        SELECT m.MOV_ID, w.WH_CODE, s.SKU_CODE, m.QTY_CHANGE
        FROM BRONZE.INV_STOCK_MOVEMENTS m
        JOIN BRONZE.INV_WAREHOUSES w ON w.WH_ID = m.WH_ID
        JOIN BRONZE.INV_SKU s ON s.SKU_ID = m.SKU_ID
        WHERE w.WH_CODE = ''' || REPLACE(:P_WH_CODE, '''', '''''') || '''';

    res := (EXECUTE IMMEDIATE :v_sql);
    RETURN TABLE(res);
END;
$$;


CREATE OR REPLACE PROCEDURE BRONZE.SP_INV_SOFT_INSERT_STOCK_ORDER(
    P_WH_ID      NUMBER,
    P_SKU_ID     NUMBER,
    P_QTY_CHANGE NUMBER,
    P_REASON     VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO BRONZE.INV_STOCK_MOVEMENTS (WH_ID, SKU_ID, QTY_CHANGE, REASON)
    VALUES (:P_WH_ID, :P_SKU_ID, :P_QTY_CHANGE, COALESCE(:P_REASON, 'via view'));

    RETURN 'Inserted movement id=' || LAST_QUERY_ID();
END;
$$;


CREATE OR REPLACE PROCEDURE BRONZE.SP_INV_UPDATE_SKU_COST(
    P_SKU_ID   NUMBER,
    P_NEW_COST NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    IF (:P_NEW_COST < 0) THEN
        RETURN 'ERROR: SKU unit cost cannot be negative';
    END IF;

    UPDATE BRONZE.INV_SKU
    SET UNIT_COST = :P_NEW_COST
    WHERE SKU_ID = :P_SKU_ID;

    RETURN 'Updated sku_id=' || P_SKU_ID || ' cost=' || P_NEW_COST;
END;
$$;


-- =============================================================================
-- SECTION 26: STORED PROCEDURES — DML Guard (INSTEAD OF trigger replacements)
-- Source: snowflake_ddl/procedures/06_dml_guard_procs.sql
-- SQL Server had INSTEAD OF triggers on views; logic moved into SPs.
-- =============================================================================

CREATE OR REPLACE PROCEDURE BRONZE.SP_SOFT_DELETE_ORDER(P_ORDER_ID NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_status VARCHAR;
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO :v_exists
    FROM BRONZE.ORDERS
    WHERE ORDER_ID = :P_ORDER_ID;

    IF (v_exists = 0) THEN
        RETURN 'ERROR: order not found';
    END IF;

    SELECT STATUS INTO :v_status
    FROM BRONZE.ORDERS
    WHERE ORDER_ID = :P_ORDER_ID;

    IF (v_status = 'Closed') THEN
        RETURN 'ERROR: cannot delete closed orders (archive pattern)';
    END IF;

    INSERT INTO BRONZE.ORDERS_ARCHIVE
        (ORDER_ID, CUSTOMER_ID, ORDER_DATE, STATUS, TOTAL_AMOUNT, NOTES, ARCHIVE_REASON)
    SELECT ORDER_ID, CUSTOMER_ID, ORDER_DATE, STATUS, TOTAL_AMOUNT, NOTES,
           'SOFT DELETE via SP_SOFT_DELETE_ORDER'
    FROM BRONZE.ORDERS
    WHERE ORDER_ID = :P_ORDER_ID;

    DELETE FROM BRONZE.ORDERS WHERE ORDER_ID = :P_ORDER_ID;

    RETURN 'Archived and deleted order_id=' || :P_ORDER_ID;
END;
$$;


CREATE OR REPLACE PROCEDURE BRONZE.SP_UPDATE_ORDER(
    P_ORDER_ID    NUMBER,
    P_STATUS      VARCHAR,
    P_NOTES       VARCHAR,
    P_TOTAL_AMOUNT NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_exists
    FROM BRONZE.ORDERS
    WHERE ORDER_ID = :P_ORDER_ID;

    IF (v_exists = 0) THEN
        RETURN 'ERROR: order not found';
    END IF;

    UPDATE BRONZE.ORDERS
    SET
        STATUS       = COALESCE(:P_STATUS,       STATUS),
        NOTES        = COALESCE(:P_NOTES,        NOTES),
        TOTAL_AMOUNT = COALESCE(:P_TOTAL_AMOUNT, TOTAL_AMOUNT)
    WHERE ORDER_ID = :P_ORDER_ID;

    RETURN 'Updated order_id=' || P_ORDER_ID;
END;
$$;


-- =============================================================================
-- SECTION 27: STREAMS & TASKS — SnowConvertStressDB
-- Source: snowflake_ddl/streams_tasks/01_stressdb_streams.sql
-- SQL Server DML triggers replaced by append-only streams + scheduled tasks:
--   tr_Orders_Audit_IU           → STREAM_ORDERS_CHANGES + TASK_ORDERS_AUDIT
--   tr_OrderItems_RecalcAndQueue → STREAM_ORDER_ITEMS_CHANGES + TASK_ORDER_ITEMS_RECALC
--   tr_Products_ListPriceAudit   → STREAM_PRODUCTS_CHANGES + TASK_PRODUCTS_PRICE_AUDIT
-- NOTE: Tasks are created SUSPENDED. Resume after pipeline verification.
-- =============================================================================

CREATE OR REPLACE STREAM BRONZE.STREAM_ORDERS_CHANGES
    ON TABLE BRONZE.ORDERS
    APPEND_ONLY = FALSE
    COMMENT = 'Captures INSERT and UPDATE on BRONZE.ORDERS (replaces tr_Orders_Audit_IU)';

CREATE OR REPLACE TASK BRONZE.TASK_ORDERS_AUDIT
    WAREHOUSE = WH_MSSQL_MIGRATION
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.STREAM_ORDERS_CHANGES')
AS
INSERT INTO BRONZE.MIGRATION_AUDIT_LOG
    (TABLE_NAME, DML_ACTION, ROW_PK, OLD_PAYLOAD, NEW_PAYLOAD, SQL_TRIGGER)
SELECT
    'Orders',
    CASE
        WHEN METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE THEN 'INSERT'
        WHEN METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = TRUE  THEN 'UPDATE'
        ELSE 'DELETE'
    END,
    ORDER_ID::VARCHAR,
    NULL,
    OBJECT_CONSTRUCT(
        'ORDER_ID',     ORDER_ID,
        'CUSTOMER_ID',  CUSTOMER_ID,
        'ORDER_DATE',   ORDER_DATE,
        'STATUS',       STATUS,
        'TOTAL_AMOUNT', TOTAL_AMOUNT,
        'NOTES',        NOTES
    ),
    'TASK_ORDERS_AUDIT'
FROM BRONZE.STREAM_ORDERS_CHANGES
WHERE METADATA$ACTION = 'INSERT';

-- Task created SUSPENDED — resume after pipeline verification
ALTER TASK BRONZE.TASK_ORDERS_AUDIT SUSPEND;


CREATE OR REPLACE STREAM BRONZE.STREAM_ORDER_ITEMS_CHANGES
    ON TABLE BRONZE.ORDER_ITEMS
    APPEND_ONLY = FALSE
    COMMENT = 'Captures DML on ORDER_ITEMS; drives header recalc and event queue';

CREATE OR REPLACE TASK BRONZE.TASK_ORDER_ITEMS_RECALC
    WAREHOUSE = WH_MSSQL_MIGRATION
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.STREAM_ORDER_ITEMS_CHANGES')
AS
BEGIN
    INSERT INTO BRONZE.ORDER_EVENT_QUEUE (ORDER_ID, EVENT_TYPE, PAYLOAD)
    SELECT DISTINCT
        ORDER_ID,
        'LINE_ITEMS_CHANGED',
        OBJECT_CONSTRUCT('order_id', ORDER_ID)
    FROM BRONZE.STREAM_ORDER_ITEMS_CHANGES;

    UPDATE BRONZE.ORDERS o
    SET TOTAL_AMOUNT = (
        SELECT COALESCE(SUM(LINE_TOTAL), 0)
        FROM BRONZE.ORDER_ITEMS i
        WHERE i.ORDER_ID = o.ORDER_ID
    )
    WHERE o.ORDER_ID IN (
        SELECT DISTINCT ORDER_ID FROM BRONZE.STREAM_ORDER_ITEMS_CHANGES
    );
END;

-- Task created SUSPENDED — resume after pipeline verification
ALTER TASK BRONZE.TASK_ORDER_ITEMS_RECALC SUSPEND;


CREATE OR REPLACE STREAM BRONZE.STREAM_PRODUCTS_CHANGES
    ON TABLE BRONZE.PRODUCTS
    APPEND_ONLY = FALSE
    COMMENT = 'Captures price changes on PRODUCTS; drives price history log';

CREATE OR REPLACE TASK BRONZE.TASK_PRODUCTS_PRICE_AUDIT
    WAREHOUSE = WH_MSSQL_MIGRATION
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.STREAM_PRODUCTS_CHANGES')
AS
INSERT INTO BRONZE.PRODUCT_PRICE_HISTORY (PRODUCT_ID, OLD_PRICE, NEW_PRICE)
SELECT
    new_row.PRODUCT_ID,
    old_row.LIST_PRICE AS OLD_PRICE,
    new_row.LIST_PRICE AS NEW_PRICE
FROM BRONZE.STREAM_PRODUCTS_CHANGES new_row
JOIN BRONZE.PRODUCTS old_row
    ON old_row.PRODUCT_ID = new_row.PRODUCT_ID
WHERE new_row.METADATA$ACTION = 'INSERT'
  AND new_row.METADATA$ISUPDATE = TRUE
  AND new_row.LIST_PRICE IS DISTINCT FROM old_row.LIST_PRICE;

-- Task created SUSPENDED — resume after pipeline verification
ALTER TASK BRONZE.TASK_PRODUCTS_PRICE_AUDIT SUSPEND;


-- =============================================================================
-- SECTION 28: STREAMS & TASKS — LabERP_DB
-- Source: snowflake_ddl/streams_tasks/02_erp_streams.sql
-- SQL Server ERP triggers replaced:
--   tr_Employees_Audit     → STREAM_ERP_EMPLOYEES_CHANGES + TASK_ERP_EMPLOYEES_AUDIT
--   tr_PayrollLines_Recalc → STREAM_ERP_PAYROLL_LINES_CHANGES + TASK_ERP_PAYROLL_RECALC
-- NOTE: Tasks are created SUSPENDED. Resume after pipeline verification.
-- =============================================================================

CREATE OR REPLACE STREAM BRONZE.STREAM_ERP_EMPLOYEES_CHANGES
    ON TABLE BRONZE.ERP_EMPLOYEES
    APPEND_ONLY = FALSE
    COMMENT = 'Feeds audit log for employee inserts and updates (replaces tr_Employees_Audit)';

CREATE OR REPLACE TASK BRONZE.TASK_ERP_EMPLOYEES_AUDIT
    WAREHOUSE = WH_MSSQL_MIGRATION
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.STREAM_ERP_EMPLOYEES_CHANGES')
AS
INSERT INTO BRONZE.MIGRATION_AUDIT_LOG
    (TABLE_NAME, DML_ACTION, ROW_PK, NEW_PAYLOAD, SQL_TRIGGER)
SELECT
    'ERP_EMPLOYEES',
    CASE
        WHEN METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE THEN 'INSERT'
        WHEN METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = TRUE  THEN 'UPDATE'
        ELSE 'DELETE'
    END,
    EMPLOYEE_ID::VARCHAR,
    OBJECT_CONSTRUCT(
        'EMP_CODE', EMP_CODE,
        'SALARY',   SALARY,
        'DEPT_ID',  DEPT_ID
    ),
    'TASK_ERP_EMPLOYEES_AUDIT'
FROM BRONZE.STREAM_ERP_EMPLOYEES_CHANGES
WHERE METADATA$ACTION = 'INSERT';

-- Task created SUSPENDED — resume after pipeline verification
ALTER TASK BRONZE.TASK_ERP_EMPLOYEES_AUDIT SUSPEND;


CREATE OR REPLACE STREAM BRONZE.STREAM_ERP_PAYROLL_LINES_CHANGES
    ON TABLE BRONZE.ERP_PAYROLL_LINES
    APPEND_ONLY = FALSE
    COMMENT = 'Triggers payroll run status update on line insert/delete (replaces tr_PayrollLines_Recalc)';

CREATE OR REPLACE TASK BRONZE.TASK_ERP_PAYROLL_RECALC
    WAREHOUSE = WH_MSSQL_MIGRATION
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.STREAM_ERP_PAYROLL_LINES_CHANGES')
AS
UPDATE BRONZE.ERP_PAYROLL_RUNS pr
SET STATUS = 'Adjusted'
WHERE pr.RUN_ID IN (
    SELECT DISTINCT RUN_ID
    FROM BRONZE.STREAM_ERP_PAYROLL_LINES_CHANGES
);

-- Task created SUSPENDED — resume after pipeline verification
ALTER TASK BRONZE.TASK_ERP_PAYROLL_RECALC SUSPEND;


-- =============================================================================
-- SECTION 29: STREAMS & TASKS — LabCRM_DB
-- Source: snowflake_ddl/streams_tasks/03_crm_streams.sql
-- SQL Server CRM triggers replaced:
--   tr_Accounts_Activity        → STREAM_CRM_ACCOUNTS_CHANGES + TASK_CRM_ACCOUNTS_ACTIVITY
--   tr_Opportunities_StageGuard → Enforcement in SP_CRM_UPDATE_OPPORTUNITY_STAGE;
--                                  task below logs stage transitions for auditing.
-- NOTE: Tasks are created SUSPENDED. Resume after pipeline verification.
-- =============================================================================

CREATE OR REPLACE STREAM BRONZE.STREAM_CRM_ACCOUNTS_CHANGES
    ON TABLE BRONZE.CRM_ACCOUNTS
    APPEND_ONLY = FALSE
    COMMENT = 'Feeds CRM activity log on account insert/update (replaces tr_Accounts_Activity)';

CREATE OR REPLACE TASK BRONZE.TASK_CRM_ACCOUNTS_ACTIVITY
    WAREHOUSE = WH_MSSQL_MIGRATION
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.STREAM_CRM_ACCOUNTS_CHANGES')
AS
INSERT INTO BRONZE.MIGRATION_AUDIT_LOG
    (TABLE_NAME, DML_ACTION, ROW_PK, NEW_PAYLOAD, SQL_TRIGGER)
SELECT
    'CRM_ACCOUNTS',
    CASE
        WHEN METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE THEN 'INSERT'
        ELSE 'UPDATE'
    END,
    ACCOUNT_CODE,
    OBJECT_CONSTRUCT('ACCOUNT_CODE', ACCOUNT_CODE, 'NAME', NAME, 'REGION', REGION),
    'TASK_CRM_ACCOUNTS_ACTIVITY'
FROM BRONZE.STREAM_CRM_ACCOUNTS_CHANGES
WHERE METADATA$ACTION = 'INSERT';

-- Task created SUSPENDED — resume after pipeline verification
ALTER TASK BRONZE.TASK_CRM_ACCOUNTS_ACTIVITY SUSPEND;


CREATE OR REPLACE STREAM BRONZE.STREAM_CRM_OPPORTUNITIES_CHANGES
    ON TABLE BRONZE.CRM_OPPORTUNITIES
    APPEND_ONLY = FALSE
    COMMENT = 'Logs opportunity stage transitions for audit trail';

CREATE OR REPLACE TASK BRONZE.TASK_CRM_OPPORTUNITIES_AUDIT
    WAREHOUSE = WH_MSSQL_MIGRATION
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.STREAM_CRM_OPPORTUNITIES_CHANGES')
AS
INSERT INTO BRONZE.MIGRATION_AUDIT_LOG
    (TABLE_NAME, DML_ACTION, ROW_PK, NEW_PAYLOAD, SQL_TRIGGER)
SELECT
    'CRM_OPPORTUNITIES',
    CASE
        WHEN METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE THEN 'INSERT'
        ELSE 'UPDATE'
    END,
    OPP_ID::VARCHAR,
    OBJECT_CONSTRUCT('OPP_ID', OPP_ID, 'STAGE', STAGE, 'AMOUNT_USD', AMOUNT_USD),
    'TASK_CRM_OPPORTUNITIES_AUDIT'
FROM BRONZE.STREAM_CRM_OPPORTUNITIES_CHANGES
WHERE METADATA$ACTION = 'INSERT';

-- Task created SUSPENDED — resume after pipeline verification
ALTER TASK BRONZE.TASK_CRM_OPPORTUNITIES_AUDIT SUSPEND;


-- =============================================================================
-- SECTION 30: STREAMS & TASKS — LabInventory_DB
-- Source: snowflake_ddl/streams_tasks/04_inventory_streams.sql
-- SQL Server Inventory triggers replaced:
--   tr_Sku_NoNegativeCost  → Validation in SP_INV_UPDATE_SKU_COST; stream flags bypasses
--   tr_vw_StockOrders_IOI  → INSTEAD OF INSERT on view → SP_INV_SOFT_INSERT_STOCK_ORDER
-- NOTE: Tasks are created SUSPENDED. Resume after pipeline verification.
-- =============================================================================

CREATE OR REPLACE STREAM BRONZE.STREAM_INV_SKU_CHANGES
    ON TABLE BRONZE.INV_SKU
    APPEND_ONLY = FALSE
    COMMENT = 'Monitors SKU cost changes; flags negative values that bypass the SP layer';

CREATE OR REPLACE TASK BRONZE.TASK_INV_SKU_COST_GUARD
    WAREHOUSE = WH_MSSQL_MIGRATION
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.STREAM_INV_SKU_CHANGES')
AS
INSERT INTO BRONZE.MIGRATION_AUDIT_LOG
    (TABLE_NAME, DML_ACTION, ROW_PK, NEW_PAYLOAD, SQL_TRIGGER)
SELECT
    'INV_SKU',
    'CONSTRAINT_VIOLATION',
    SKU_ID::VARCHAR,
    OBJECT_CONSTRUCT('SKU_ID', SKU_ID, 'UNIT_COST', UNIT_COST, 'NOTE', 'negative cost detected'),
    'TASK_INV_SKU_COST_GUARD'
FROM BRONZE.STREAM_INV_SKU_CHANGES
WHERE METADATA$ACTION = 'INSERT'
  AND UNIT_COST < 0;

-- Task created SUSPENDED — resume after pipeline verification
ALTER TASK BRONZE.TASK_INV_SKU_COST_GUARD SUSPEND;


CREATE OR REPLACE STREAM BRONZE.STREAM_INV_STOCK_MOVEMENTS_CHANGES
    ON TABLE BRONZE.INV_STOCK_MOVEMENTS
    APPEND_ONLY = TRUE
    COMMENT = 'Append-only stream for new stock movement events';

CREATE OR REPLACE TASK BRONZE.TASK_INV_STOCK_AUDIT
    WAREHOUSE = WH_MSSQL_MIGRATION
    SCHEDULE  = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.STREAM_INV_STOCK_MOVEMENTS_CHANGES')
AS
INSERT INTO BRONZE.MIGRATION_AUDIT_LOG
    (TABLE_NAME, DML_ACTION, ROW_PK, NEW_PAYLOAD, SQL_TRIGGER)
SELECT
    'INV_STOCK_MOVEMENTS',
    'INSERT',
    MOV_ID::VARCHAR,
    OBJECT_CONSTRUCT(
        'MOV_ID',     MOV_ID,
        'WH_ID',      WH_ID,
        'SKU_ID',     SKU_ID,
        'QTY_CHANGE', QTY_CHANGE
    ),
    'TASK_INV_STOCK_AUDIT'
FROM BRONZE.STREAM_INV_STOCK_MOVEMENTS_CHANGES;

-- Task created SUSPENDED — resume after pipeline verification
ALTER TASK BRONZE.TASK_INV_STOCK_AUDIT SUSPEND;


-- =============================================================================
-- SECTION 31: SCALAR UDFs
-- Source: snowflake_ddl/udfs/01_scalar_udfs.sql
-- =============================================================================

CREATE OR REPLACE FUNCTION BRONZE.FN_FORMAT_MONEY(AMOUNT NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    '$' || TO_VARCHAR(AMOUNT, '999,999,999,990.00')
$$;


CREATE OR REPLACE FUNCTION BRONZE.FN_ORDER_LINE_COUNT(P_ORDER_ID NUMBER)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
    (SELECT COUNT(*) FROM BRONZE.ORDER_ITEMS WHERE ORDER_ID = P_ORDER_ID)
$$;


-- =============================================================================
-- POST-RESTORE CHECKLIST
-- =============================================================================
--
-- 1. STORAGE INTEGRATION  (run as ACCOUNTADMIN)
--    After creating S3_DMS_INT, run:
--      DESC INTEGRATION S3_DMS_INT;
--    Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID.
--    Update the trust policy on your new IAM role:
--      Principal: { "AWS": "<STORAGE_AWS_IAM_USER_ARN>" }
--      Condition: StringEquals: sts:ExternalId: "<STORAGE_AWS_EXTERNAL_ID>"
--
-- 2. S3 BUCKET / STAGE
--    Replace every occurrence of 'dms-snowpipe-dev-05d6e64a' with your new
--    bucket name in both the storage integration STORAGE_ALLOWED_LOCATIONS
--    and the stage URL.  Re-run those two statements.
--
-- 3. SNOWPIPE SQS NOTIFICATIONS
--    After pipes are created, run for each pipe:
--      SELECT SYSTEM$PIPE_STATUS('ANALYTICS.RAW.PIPE_CUSTOMERS');
--    Grab the new notificationChannelName SQS ARN.
--    In the S3 console, update the bucket event notification to send
--    s3:ObjectCreated:* events to that ARN for each prefix:
--      sales/customers/  → PIPE_CUSTOMERS
--      sales/orders/     → PIPE_ORDERS
--      sales/order_items/→ PIPE_ORDER_ITEMS
--      sales/products/   → PIPE_PRODUCTS
--
-- 4. TASKS — RESUME WHEN READY
--    After verifying data flows end-to-end, resume tasks:
--      ALTER TASK MSSQL_MIGRATION_LAB.BRONZE.TASK_ORDERS_AUDIT             RESUME;
--      ALTER TASK MSSQL_MIGRATION_LAB.BRONZE.TASK_ORDER_ITEMS_RECALC       RESUME;
--      ALTER TASK MSSQL_MIGRATION_LAB.BRONZE.TASK_PRODUCTS_PRICE_AUDIT     RESUME;
--      ALTER TASK MSSQL_MIGRATION_LAB.BRONZE.TASK_ERP_EMPLOYEES_AUDIT      RESUME;
--      ALTER TASK MSSQL_MIGRATION_LAB.BRONZE.TASK_ERP_PAYROLL_RECALC       RESUME;
--      ALTER TASK MSSQL_MIGRATION_LAB.BRONZE.TASK_CRM_ACCOUNTS_ACTIVITY    RESUME;
--      ALTER TASK MSSQL_MIGRATION_LAB.BRONZE.TASK_CRM_OPPORTUNITIES_AUDIT  RESUME;
--      ALTER TASK MSSQL_MIGRATION_LAB.BRONZE.TASK_INV_SKU_COST_GUARD       RESUME;
--      ALTER TASK MSSQL_MIGRATION_LAB.BRONZE.TASK_INV_STOCK_AUDIT          RESUME;
--
-- 5. DBT SNAPSHOTS
--    Silver SCD-2 tables are owned by dbt.  Run `dbt snapshot` after pointing
--    dbt profiles.yml at the new account to re-create the snapshot tables with
--    the correct dbt metadata columns.
--
-- 6. AIRBYTE_RAW schema
--    Reserved. Configure Airbyte destination to use
--    MSSQL_MIGRATION_LAB.AIRBYTE_RAW when re-connecting the source.
--
-- 7. VERIFY
--    SELECT COUNT(*) FROM ANALYTICS.RAW.CUSTOMERS;
--    SELECT SYSTEM$STREAM_HAS_DATA('MSSQL_MIGRATION_LAB.BRONZE.STREAM_ORDERS_CHANGES');
--    SHOW PIPES IN SCHEMA ANALYTICS.RAW;
--    SHOW TASKS IN SCHEMA MSSQL_MIGRATION_LAB.BRONZE;
--
-- =============================================================================
-- END OF restore_full.sql
-- =============================================================================
