-- SF023 - External function endpoint down
-- Category: integration
-- Description: API integration unreachable
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_23;
