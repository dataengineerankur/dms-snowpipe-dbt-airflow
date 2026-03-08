-- SF017 - Stage path missing daily folder
-- Category: ingestion
-- Description: date partition absent
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_17;
