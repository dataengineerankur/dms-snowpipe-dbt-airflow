-- SF006 - Task suspended unexpectedly
-- Category: orchestration
-- Description: root task disabled
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_6;
