# PATCHIT auto-fix: fix_trigger_rule
# Original error: Task 'send_notification' skipped: upstream 'run_dbt' is in 'failed' state.
Trigger rule: all_success
DAG: dbt_pipeline
Consider using trigger_rule='all_done' if notification should always run.
from datetime import datetime

from airflow import DAG

from dbt_utils import build_dbt_task

with DAG(
    dag_id="dbt_customers",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "snowflake", "customers"],
) as dag:
    dbt_deps = build_dbt_task("dbt_deps", "dbt deps")
    dbt_run_stg = build_dbt_task(
        "dbt_run_stg", "dbt run --select path:models/customers/stg"
    )
    dbt_run_int = build_dbt_task(
        "dbt_run_int", "dbt run --select path:models/customers/int"
    )
    dbt_run_gold = build_dbt_task(
        "dbt_run_gold", "dbt run --select path:models/customers/gold"
    )
    dbt_test = build_dbt_task(
        "dbt_test", "dbt test --select path:models/customers"
    )

    dbt_deps >> dbt_run_stg >> dbt_run_int >> dbt_run_gold >> dbt_test
