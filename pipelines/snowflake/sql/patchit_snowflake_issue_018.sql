-- SF018 - Network policy blocks connector
-- Category: platform
-- Description: client IP not allowed
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_18;
