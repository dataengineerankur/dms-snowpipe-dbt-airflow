-- SF021 - Long running query timeout
-- Category: warehouse
-- Description: statement_timeout exceeded
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_21;
