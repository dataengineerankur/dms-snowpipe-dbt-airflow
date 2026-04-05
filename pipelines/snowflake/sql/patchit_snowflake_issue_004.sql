# PATCHIT auto-fix: unknown
# Original error: SQL access control error: Insufficient privileges to operate on warehouse COMPUTE_WH
Role: LOADER_ROLE
Required: USAGE on WAREHOUSE COMPUTE_WH
-- SF004 - Stage credentials expired
-- Category: security
-- Description: external stage integration token expired
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_4;
