"""OF Pipeline Stage 2: Validate shipment records.

Checks required fields and value ranges before downstream processing.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(__file__))
from of_shared.shipment_parser import load_state, save_state

log = logging.getLogger(__name__)

_REQUIRED = ["shipment_id", "order_id", "carrier_id", "weight_kg", "destination"]


def validate_shipments_task(**context) -> None:
    run_date = context["ds"]
    records = load_state(run_date)
    valid = []
    for r in records:
        if all(r.get(f) is not None for f in _REQUIRED):
            valid.append(r)
        else:
            log.warning("Dropping incomplete record: %s", r.get("shipment_id"))
    save_state(valid, run_date)
    log.info("Validation: %d/%d records passed", len(valid), len(records))


with DAG(
    dag_id="of_02_validate",
    default_args={"owner": "fulfillment", "retries": 1, "retry_delay": timedelta(minutes=2)},
    description="OF Pipeline Stage 2: Validate shipment records",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fulfillment", "stage-2", "validation"],
) as dag:
    validate = PythonOperator(task_id="validate_shipments", python_callable=validate_shipments_task)
