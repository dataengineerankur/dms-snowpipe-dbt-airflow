"""PATCHIT test case: MX302 - Upstream task failure handling."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


def upstream_task():
    """Upstream task that succeeds."""
    print("Upstream task completed successfully")
    return "success"


def downstream_task():
    """Downstream task that waits for upstream."""
    print("Downstream task executing after upstream success")
    return "completed"


default_args = {
    "owner": "patchit",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="patchit_airflow_upstream_case",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "upstream", "mx302"]
) as dag:
    upstream = PythonOperator(
        task_id="upstream_task",
        python_callable=upstream_task,
    )
    
    downstream = PythonOperator(
        task_id="wait_for_upstream",
        python_callable=downstream_task,
    )
    
    upstream >> downstream
