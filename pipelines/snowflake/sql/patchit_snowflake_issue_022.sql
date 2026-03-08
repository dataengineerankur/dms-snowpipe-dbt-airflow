-- SF022 - Snowpipe notification misconfigured
-- Category: ingestion
-- Description: auto-ingest not firing
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_22;
