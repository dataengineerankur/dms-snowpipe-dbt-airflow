-- SF015 - SCD2 close/open bug
-- Category: modeling
-- Description: current flag set on multiple rows
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_15;
