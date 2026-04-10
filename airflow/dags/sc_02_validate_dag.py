"""SC Stage 2: Order Validation DAG.

Validates required fields and data integrity of ingested orders.
Passes records downstream even when discount_rate is None (treated as optional).
"""
from __future__ import annotations

import logging
import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(__file__))
from sc_shared.order_transforms import load_state, save_state

log = logging.getLogger(__name__)

REQUIRED_FIELDS = ["order_id", "product_id", "customer_id", "qty", "unit_price"]


def validate_orders_task(**context) -> None:
    """Validate required fields; pass discount_rate=None through as non-critical."""
    run_date = context["ds"]
    records = load_state(run_date)

    valid, invalid = [], []
    for r in records:
        missing = [f for f in REQUIRED_FIELDS if not r.get(f)]
        if missing:
            log.warning("Order %s missing fields: %s", r.get("order_id"), missing)
            invalid.append(r)
        else:
            # discount_rate being None is not flagged as an error (treated as optional)
            valid.append(r)

    save_state(valid, run_date)
    log.info("Validation complete: %d valid, %d invalid", len(valid), len(invalid))


with DAG(
    dag_id="sc_02_validate",
    default_args={"owner": "supply-chain", "retries": 1, "retry_delay": timedelta(minutes=2)},
    description="SC Pipeline Stage 2: Validate order records",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["supply-chain", "stage-2", "validation"],
) as dag:
    validate = PythonOperator(task_id="validate_orders", python_callable=validate_orders_task)
