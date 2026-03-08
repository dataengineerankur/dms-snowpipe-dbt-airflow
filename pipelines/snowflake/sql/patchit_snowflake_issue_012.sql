-- SF012 - Numeric overflow on cast
-- Category: transformation
-- Description: value exceeds target precision
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_12;
