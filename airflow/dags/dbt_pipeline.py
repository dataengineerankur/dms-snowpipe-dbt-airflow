from datetime import datetime, timedelta

from airflow import DAG

from dbt_utils import build_dbt_task
from patchit_callbacks import patchit_failure_callback

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": patchit_failure_callback,
}

with DAG(
    dag_id="dbt_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "snowflake"],
    sla_miss_callback=patchit_failure_callback,
    dagrun_timeout=timedelta(hours=3),
) as dag:
    dbt_deps = build_dbt_task("dbt_deps", "dbt deps")
    dbt_run_stg = build_dbt_task("dbt_run_stg", "dbt run --select stg")
    dbt_run_int = build_dbt_task("dbt_run_int", "dbt run --select int")
    dbt_run_gold = build_dbt_task("dbt_run_gold", "dbt run --select gold")
    dbt_test = build_dbt_task("dbt_test", "dbt test --select gold")

    dbt_deps >> dbt_run_stg >> dbt_run_int >> dbt_run_gold >> dbt_test
