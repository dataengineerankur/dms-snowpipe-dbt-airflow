FROM --platform=linux/amd64 ghcr.io/dbt-labs/dbt-snowflake:1.7.5

WORKDIR /opt/dbt
# Build context must be project root (dms-snowpipe-dbt-airflow/).
# The dbt/ directory at the root contains dbt_project.yml, models/, snapshots/, etc.
COPY dbt/ /opt/dbt/

ENV DBT_PROFILES_DIR=/opt/dbt

# Override entrypoint: drop the broken --profiles-dir global flag.
# DBT_PROFILES_DIR env var is used instead (supported since dbt 0.19).
ENTRYPOINT ["dbt"]
