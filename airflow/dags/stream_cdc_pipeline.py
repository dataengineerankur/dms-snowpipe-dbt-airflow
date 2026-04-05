from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from snowflake_university_utils import branch_on_stream, build_sql_task
with DAG(dag_id='stream_cdc_pipeline', start_date=datetime(2024, 1, 1), schedule_interval=None, catchup=False, max_active_runs=1, tags=['snowflake', 'streams', 'cdc'], default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)}) as dag:
    has_data = branch_on_stream('check_orders_stream', 'RAW_DB.PUBLIC.ORDERS_CDC_STREAM', 'consume_streams', 'skip_stream_run')
    consume_streams = build_sql_task('consume_streams', 'scripts/02_streams_consume.sql')
    skip_stream_run = EmptyOperator(task_id='skip_stream_run')
    has_data >> [consume_streams, skip_stream_run]
