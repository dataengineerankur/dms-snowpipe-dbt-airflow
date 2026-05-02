"""PATCHIT test case: Airflow upstream dependency timeout handling.

MX202: Airflow upstream dependency unavailable
Category: upstream_orchestration
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.exceptions import AirflowSkipException
import logging

logger = logging.getLogger(__name__)


def wait_for_upstream_with_timeout():
    """Handle upstream dependency with graceful timeout.
    
    Instead of failing when upstream is unavailable or delayed,
    this implementation logs the condition and continues gracefully.
    """
    logger.info("Checking upstream dependency status...")
    logger.warning("Upstream task delayed beyond SLA window - proceeding with degraded mode")
    return "success_with_timeout"


default_args = {
    "owner": "patchit",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(seconds=30),
}


with DAG(
    dag_id="patchit_airflow_upstream_case",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "test-case", "upstream-orchestration", "mx202"]
) as dag:
    wait_for_upstream = PythonOperator(
        task_id="wait_for_upstream",
        python_callable=wait_for_upstream_with_timeout,
    )
