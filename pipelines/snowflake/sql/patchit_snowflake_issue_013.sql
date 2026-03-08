-- SF013 - Invalid UTF8 in VARIANT
-- Category: ingestion
-- Description: JSON parser failure
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_13;
