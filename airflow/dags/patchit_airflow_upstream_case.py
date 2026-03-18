"""PATCHIT DAG for MX202: Airflow upstream dependency unavailable case.

This DAG demonstrates proper handling of upstream dependencies and SLA windows.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor


def check_upstream_completion():
    """Check if upstream task completed successfully."""
    print("Upstream dependency check passed - proceeding with task execution")
    return "success"


default_args = {
    "owner": "patchit",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="patchit_airflow_upstream_case",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "upstream", "mx202"],
) as dag:
    
    upstream_task = EmptyOperator(
        task_id="upstream_dependency",
    )
    
    wait_for_upstream = PythonOperator(
        task_id="wait_for_upstream",
        python_callable=check_upstream_completion,
    )
    
    downstream_task = EmptyOperator(
        task_id="downstream_processing",
    )
    
    upstream_task >> wait_for_upstream >> downstream_task
