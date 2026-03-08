-- SF025 - Task history query permission error
-- Category: observability
-- Description: ACCOUNT_USAGE access missing
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_25;
