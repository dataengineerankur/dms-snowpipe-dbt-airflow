# PATCHIT auto-fix: unknown
# Original error: SQL access control error: Insufficient privileges to operate on warehouse COMPUTE_WH
Role: LOADER_ROLE
Required: USAGE on WAREHOUSE COMPUTE_WH
-- SF006 - Task suspended unexpectedly
-- Category: orchestration
-- Description: root task disabled
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_6;
