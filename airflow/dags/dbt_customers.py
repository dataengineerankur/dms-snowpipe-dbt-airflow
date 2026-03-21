from datetime import datetime, timedelta

dag = DAG(
    'dbt_customers',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2026, 3, 21),
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    schedule_interval=None
)
