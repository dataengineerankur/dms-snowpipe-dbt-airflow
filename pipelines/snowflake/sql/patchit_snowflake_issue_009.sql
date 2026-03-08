-- SF009 - Merge duplicates due ON condition
-- Category: modeling
-- Description: upsert key incomplete
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_9;
