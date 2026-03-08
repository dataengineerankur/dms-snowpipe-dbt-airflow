-- SF004 - Stage credentials expired
-- Category: security
-- Description: external stage integration token expired
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_4;
