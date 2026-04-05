from datetime import datetime, timedelta
from airflow import DAG
from snowflake_university_utils import build_sql_task
with DAG(dag_id='transient_table_pipeline', start_date=datetime(2024, 1, 1), schedule_interval=None, catchup=False, max_active_runs=1, tags=['snowflake', 'transient'], default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)}) as dag:
    create_transient = build_sql_task('create_transient_staging', 'scripts/01_transient_tables.sql')
    quality_check = build_sql_task('quality_check_transient', 'scripts/01_transient_tables.sql')
    promote_clean_rows = build_sql_task('promote_clean_rows', 'scripts/01_transient_tables.sql')
    cleanup = build_sql_task('cleanup_transient', 'scripts/01_transient_tables.sql')
    create_transient >> quality_check >> promote_clean_rows >> cleanup
