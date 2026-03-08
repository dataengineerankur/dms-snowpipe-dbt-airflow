-- SF014 - Timezone conversion bug
-- Category: transformation
-- Description: timestamp_ntz interpreted as local
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_14;
