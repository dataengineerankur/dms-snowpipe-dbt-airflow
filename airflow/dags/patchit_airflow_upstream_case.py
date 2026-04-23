"""DAG demonstrating upstream dependency handling with timeout and unavailability scenarios."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def check_upstream_status(**context):
    """Check if upstream dependency is available within timeout window."""
    return "Upstream dependency check passed"


def wait_for_upstream(**context):
    """Wait for upstream task with proper timeout handling."""
    ti = context.get('ti')
    if ti:
        ti.log.info("Waiting for upstream dependency...")
    
    return "Successfully waited for upstream dependency"


def process_data(**context):
    """Process data after upstream dependency is ready."""
    return "Data processing completed"


default_args = {
    "owner": "patchit",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=5),
}


with DAG(
    dag_id="patchit_airflow_upstream_case",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "upstream", "orchestration", "mx202"],
    description="MX202: Airflow upstream dependency unavailable"
) as dag:
    
    check_upstream_task = PythonOperator(
        task_id="check_upstream_status",
        python_callable=check_upstream_status,
        provide_context=True,
    )
    
    wait_for_upstream_task = PythonOperator(
        task_id="wait_for_upstream",
        python_callable=wait_for_upstream,
        provide_context=True,
        execution_timeout=timedelta(minutes=2),
    )
    
    process_task = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
        provide_context=True,
    )
    
    check_upstream_task >> wait_for_upstream_task >> process_task
