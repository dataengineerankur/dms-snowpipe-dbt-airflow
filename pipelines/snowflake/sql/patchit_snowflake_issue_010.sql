-- SF010 - Primary key not enforced logically
-- Category: data_quality
-- Description: duplicates in business key
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_10;
