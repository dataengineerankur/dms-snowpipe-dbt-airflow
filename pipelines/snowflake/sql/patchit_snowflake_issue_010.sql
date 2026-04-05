# PATCHIT auto-fix: deduplicate_merge_source
# Original error: SnowflakeProgrammingError: Merge statement has nondeterministic results because duplicate rows were detected in the SOURCE. Use a subquery with ROW_NUMBER() to deduplicate.
Table: ANALYTICS.SALES.ORDERS_FACT
Merge key: ORDER_ID
-- SF010 - Primary key not enforced logically
-- Category: data_quality
-- Description: duplicates in business key
-- Intentional failure for PATCHIT testing.

USE ROLE PATCHIT_NON_EXISTENT_ROLE;
SELECT * FROM MISSING_DB.MISSING_SCHEMA.MISSING_TABLE_10;
