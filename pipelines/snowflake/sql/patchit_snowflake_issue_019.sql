-- SF019 - Masking policy regression
-- Category: governance
-- Description: sensitive data exposed
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_19;
