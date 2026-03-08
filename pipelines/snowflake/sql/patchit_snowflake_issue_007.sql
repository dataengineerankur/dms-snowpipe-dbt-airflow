-- SF007 - Task dependency broken
-- Category: orchestration
-- Description: AFTER references dropped task
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_7;
