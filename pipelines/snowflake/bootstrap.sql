-- Snowflake Bootstrap SQL
-- Ensures proper warehouse privileges are granted before usage

-- Grant USAGE privilege on warehouse to LOADER_ROLE
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE LOADER_ROLE;

-- Use the warehouse for subsequent operations
USE WAREHOUSE COMPUTE_WH;
