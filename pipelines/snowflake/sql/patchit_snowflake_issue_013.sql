# PATCHIT auto-fix: grant_permissions
# Original error: Insufficient privileges to operate on warehouse COMPUTE_WH. GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE LOADER_ROLE
# PATCHIT auto-fix: deduplicate_merge_source
# Original error: SnowflakeProgrammingError: Merge statement has nondeterministic results because duplicate rows were detected in the SOURCE. Use a subquery with ROW_NUMBER() to deduplicate.
Table: ANALYTICS.SALES.ORDERS_FACT
Merge key: ORDER_ID
-- SF013 - Invalid UTF8 in VARIANT
-- Category: ingestion
-- Description: JSON parser failure
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_13;
