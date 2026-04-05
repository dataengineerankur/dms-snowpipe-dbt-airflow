# Materialized Views vs Dynamic Tables

## Use Materialized Views when
- query shape is stable
- no joins are required in the MV definition
- you want transparent acceleration for repeated query patterns

## Use Dynamic Tables when
- joins are required
- lag targets matter more than transparent rewrite
- you want explicit persisted intermediate layers

```sql
SELECT TABLE_NAME, TARGET_LAG, LAST_REFRESH_TIME FROM INFORMATION_SCHEMA.DYNAMIC_TABLES;
SELECT * FROM INFORMATION_SCHEMA.MATERIALIZED_VIEW_REFRESH_HISTORY ORDER BY REFRESH_END_TIME DESC;
```
