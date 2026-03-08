-- SF005 - File format mismatch
-- Category: ingestion
-- Description: CSV quoted fields parsed incorrectly
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_5;
