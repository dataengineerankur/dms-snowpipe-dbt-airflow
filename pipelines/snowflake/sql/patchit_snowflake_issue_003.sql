-- SF003 - Schema drift in staged files
-- Category: schema
-- Description: COPY INTO fails due column mismatch
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_3;
