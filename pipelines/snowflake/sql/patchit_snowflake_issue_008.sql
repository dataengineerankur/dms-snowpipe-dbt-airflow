-- SF008 - Stream stale
-- Category: cdc
-- Description: stream offset aged out
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_8;
