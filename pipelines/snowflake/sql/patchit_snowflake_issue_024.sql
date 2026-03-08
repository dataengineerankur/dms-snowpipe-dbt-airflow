-- SF024 - Clone/restore confusion
-- Category: release
-- Description: job points to stale clone
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_24;
