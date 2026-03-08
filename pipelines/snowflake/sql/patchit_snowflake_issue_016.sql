-- SF016 - CDC delete tombstone ignored
-- Category: cdc
-- Description: delete op filtered out
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_16;
