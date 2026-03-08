-- SF011 - Null in not-null business field
-- Category: data_quality
-- Description: upstream null spikes
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_11;
