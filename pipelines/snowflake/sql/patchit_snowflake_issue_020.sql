-- SF020 - Row access policy filters all rows
-- Category: governance
-- Description: service role sees empty dataset
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_20;
