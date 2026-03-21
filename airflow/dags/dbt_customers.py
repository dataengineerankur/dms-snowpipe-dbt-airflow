from datetime import datetime, timedelta

from airflow import DAG

from dbt_utils import build_dbt_task

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-alerts@company.com"],
}

with DAG(
    dag_id="dbt_customers",
    default_args=default_args,
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
