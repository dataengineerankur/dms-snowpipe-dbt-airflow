from datetime import datetime, timedelta
from airflow import DAG
from snowflake_university_utils import build_inline_sql_task, build_sql_task

with DAG(
    dag_id='task_dag_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['snowflake', 'tasks'],
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)},
) as dag:
    deploy_tasks = build_sql_task('deploy_task_graph', 'scripts/03_tasks.sql')
    execute_root = build_inline_sql_task('execute_root_task', 'EXECUTE TASK AUDIT_DB.PUBLIC.ROOT_TASK_INGEST;')
    inspect_history = build_inline_sql_task(
        'inspect_task_history',
        """
        USE DATABASE AUDIT_DB;
        SELECT *
        FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
          SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP),
          RESULT_LIMIT => 100
        ));
        """,
    )

    deploy_tasks >> execute_root >> inspect_history
