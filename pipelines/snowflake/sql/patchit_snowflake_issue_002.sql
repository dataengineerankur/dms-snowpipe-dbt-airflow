-- SF002 - Role missing privilege
-- Category: security
-- Description: role lacks USAGE on database
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_2;
