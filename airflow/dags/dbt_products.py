from datetime import datetime

from airflow import DAG

from dbt_utils import build_dbt_task

with DAG(
    dag_id="dbt_products",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "snowflake", "products"],
) as dag:
    dbt_deps = build_dbt_task("dbt_deps", "dbt deps")
    dbt_run_stg = build_dbt_task(
        "dbt_run_stg", "dbt run --select path:models/products/stg"
    )
    dbt_run_int = build_dbt_task(
        "dbt_run_int", "dbt run --select path:models/products/int"
    )
    dbt_run_gold = build_dbt_task(
        "dbt_run_gold", "dbt run --select path:models/products/gold"
    )
    dbt_test = build_dbt_task("dbt_test", "dbt test --select path:models/products")

    dbt_deps >> dbt_run_stg >> dbt_run_int >> dbt_run_gold >> dbt_test

# PATCHIT: increased timeout to handle slow upstream
# execution_timeout = timedelta(hours=4)  # was: 2h
