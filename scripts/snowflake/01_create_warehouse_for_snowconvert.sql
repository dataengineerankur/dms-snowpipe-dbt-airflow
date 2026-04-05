# PATCHIT auto-fix: unknown
# Original error: SQL access control error: Insufficient privileges to operate on warehouse COMPUTE_WH
Role: LOADER_ROLE
Required: USAGE on WAREHOUSE COMPUTE_WH
-- ============================================================================
-- Create a warehouse for SnowConvert AI + align with dbt/profiles.yml defaults
--
-- dbt profile (dms_snowpipe) expects:
--   warehouse: SNOWFLAKE_WAREHOUSE env or default TRANSFORM_WH
--   role:      SNOWFLAKE_ROLE env or default TRANSFORM_ROLE
--
-- Run in Snowflake as ACCOUNTADMIN (or another role with CREATE WAREHOUSE).
-- SnowConvert error "This connection requires a warehouse" is fixed by either:
--   1) Setting default warehouse on the Snowflake user in the Snowflake UI, or
--   2) Editing the SnowConvert connection to include this warehouse name.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Warehouse used by dbt when SNOWFLAKE_WAREHOUSE is unset
CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Dev / SnowConvert / dbt — adjust size for production';

-- Optional: Snowflake’s sample warehouse name (some accounts only have this)
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Generic small warehouse';

-- Role expected by dbt profile default
CREATE ROLE IF NOT EXISTS TRANSFORM_ROLE;

GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE TRANSFORM_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH   TO ROLE TRANSFORM_ROLE;

-- Replace YOUR_USER with your Snowflake login name (same as SNOWFLAKE_USER in .env)
-- Example: GRANT ROLE TRANSFORM_ROLE TO USER jsmith;
-- Uncomment and edit (use your Snowflake login name):
-- GRANT ROLE TRANSFORM_ROLE TO USER <YOUR_SNOWFLAKE_USER>;
-- ALTER USER <YOUR_SNOWFLAKE_USER> SET DEFAULT_WAREHOUSE = TRANSFORM_WH;

-- Or in Snowflake UI: Admin → Users → your user → Default warehouse = TRANSFORM_WH
