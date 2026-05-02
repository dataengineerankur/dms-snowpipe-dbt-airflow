# PATCHIT auto-fix: unknown
# Classification: unknown
# Error summary: AirflowException: Task failed with no retries configured. Add retries=3 and retry_delay=timedelta(minutes=5) to default_args.
# TODO: Apply unknown — see PR description for manual steps

from datetime import datetime

from airflow import DAG

from dbt_utils import build_dbt_task

with DAG(
    dag_id="dbt_orders",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "snowflake", "orders"],
) as dag:
    dbt_deps = build_dbt_task("dbt_deps", "dbt deps")
    dbt_run_stg = build_dbt_task(
        "dbt_run_stg", "dbt run --select path:models/orders/stg"
    )
    dbt_run_int = build_dbt_task(
        "dbt_run_int", "dbt run --select path:models/orders/int"
    )
    dbt_run_gold = build_dbt_task(
        "dbt_run_gold", "dbt run --select path:models/orders/gold"
    )
    dbt_test = build_dbt_task("dbt_test", "dbt test --select path:models/orders")

    dbt_deps >> dbt_run_stg >> dbt_run_int >> dbt_run_gold >> dbt_test
