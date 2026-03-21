from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    'dbt_orders',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 3, 20),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    max_active_runs=1
)

# Rest of the DAG definition...
