from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from dbt_utils import build_dbt_task
from snowflake_university_utils import branch_on_stream, build_inline_sql_task, build_python_task, build_sql_task

with DAG(
    dag_id='snowflake_master_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['snowflake', 'production', 'daily'],
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)},
) as dag:
    check_stream_data = branch_on_stream('check_stream_data', 'RAW_DB.PUBLIC.ORDERS_CDC_STREAM', 'consume_stream_delta', 'skip_pipeline')
    skip_pipeline = EmptyOperator(task_id='skip_pipeline')
    consume_stream_delta = build_sql_task('consume_stream_delta', 'scripts/02_streams_consume.sql')
    run_snowpark_logic = build_python_task('run_snowpark_logic', 'snowpark/snowpark_pipelines.py')
    run_snapshots = build_dbt_task('dbt_snapshot_university', 'dbt snapshot --select snp_products snp_customers snp_orders_status')
    run_models = build_dbt_task('dbt_run_university', 'dbt run --select university+ orders_with_status_history')
    validate_counts = build_inline_sql_task(
        'validate_counts',
        """
        SELECT 'raw_orders' AS table_name, COUNT(*) AS row_count FROM RAW_DB.PUBLIC.RAW_ORDERS
        UNION ALL
        SELECT 'silver_orders', COUNT(*) FROM SILVER_DB.PUBLIC.ORDERS_SILVER
        UNION ALL
        SELECT 'orders_audit', COUNT(*) FROM AUDIT_DB.PUBLIC.ORDERS_AUDIT;
        """,
    )
    refresh_mvs = build_sql_task('refresh_materialized_views', 'scripts/05_materialized_views.sql')
    clustering_maintenance = build_sql_task('clustering_maintenance', 'scripts/09_clustering_benchmark.sql')

    check_stream_data >> [consume_stream_delta, skip_pipeline]
    consume_stream_delta >> run_snowpark_logic >> run_snapshots >> run_models >> validate_counts >> refresh_mvs >> clustering_maintenance
