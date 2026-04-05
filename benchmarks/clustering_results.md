# Clustering Results

This file is intended to be updated after running `scripts/09_clustering_benchmark.sql`.

Suggested export query:
```sql
SELECT * FROM BENCHMARKS.PUBLIC.CLUSTERING_RESULTS ORDER BY query_type, partitions_scanned;
```
