# Snowflake Iceberg Lab

## Goal
Use the existing S3 bucket `dms-snowpipe-dev-05d6e64a` to add an open-table-format teaching section on top of the Snowflake University lab.

Suggested base prefix:
- `s3://dms-snowpipe-dev-05d6e64a/snowflake-university/iceberg/`

## Why add Iceberg here
Iceberg is the right follow-on concept after transient tables, streams, tasks, dynamic tables, materialized views, and native Snowflake tables because it answers a different question:

- Native Snowflake tables: best when Snowflake owns the table end to end.
- Iceberg tables: best when data and metadata should stay open on S3 and be shareable across engines.

## Good use cases in this repo
- Publish a Snowflake-managed Iceberg table whose files live on S3, so you can compare native Snowflake tables with Iceberg-backed storage.
- Create a lakehouse-style table layout that can later be extended into broader multi-engine sharing if you also add the right external catalog or discovery setup.
- Teach when open table format is useful, even when your first step is still Snowflake-managed.

## What you need
1. AWS credentials with permission to work with the bucket.
2. An IAM role Snowflake can assume for the Iceberg prefix.
3. `ACCOUNTADMIN` in Snowflake.
4. A dedicated prefix for Iceberg metadata and data.

## Practical build order
1. Create the S3 prefix and IAM role for Snowflake access.
2. Create the external volume in Snowflake.
3. Run `DESC EXTERNAL VOLUME UNIVERSITY_ICEBERG_VOL;` and capture the Snowflake-generated IAM user ARN / external ID details.
4. Update the IAM role trust policy and permissions so Snowflake can actually assume the role for that S3 prefix.
5. Re-run `DESC EXTERNAL VOLUME UNIVERSITY_ICEBERG_VOL;` or a simple validation query to confirm the setup.
6. Create a Snowflake-managed Iceberg table on that external volume.
7. Load or materialize data into the Iceberg table.
8. Query it from Snowflake and compare operational tradeoffs against native tables.

## First SQL file to use
- `scripts/11_iceberg_s3_lab.sql`

## What to observe
- Iceberg data files live on S3, and this starter lab keeps table management in Snowflake with `CATALOG = SNOWFLAKE`.
- That means the current starter is best understood as Snowflake-managed Iceberg on S3, not a complete cross-engine sharing setup by itself.
- The starter SQL loads a deterministic slice of the university orders table so the lab stays reproducible.
- This is a lakehouse pattern, not a replacement for every native Snowflake table.
