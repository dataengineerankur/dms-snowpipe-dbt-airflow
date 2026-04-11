"""OF Pipeline Stage 5: Generate shipping labels and assign tracking IDs."""
from __future__ import annotations

import hashlib
import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(__file__))
from of_shared.shipment_parser import load_state, save_state

log = logging.getLogger(__name__)

_LABEL_PREFIXES = {101: "FX", 102: "UP", 103: "DH", 104: "US"}


def generate_labels_task(**context) -> None:
    run_date = context["ds"]
    records = load_state(run_date)
    for r in records:
        prefix = _LABEL_PREFIXES.get(r["carrier_id"], "XX")
        digest = hashlib.md5(f"{r['shipment_id']}{run_date}".encode()).hexdigest()[:10].upper()
        r["tracking_id"] = f"{prefix}{digest}"
        r["label_generated"] = True
    save_state(records, run_date)
    log.info("Labels generated for %d shipments", len(records))


with DAG(
    dag_id="of_05_label",
    default_args={"owner": "fulfillment", "retries": 1, "retry_delay": timedelta(minutes=2)},
    description="OF Pipeline Stage 5: Generate shipping labels",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fulfillment", "stage-5", "labels"],
) as dag:
    label = PythonOperator(task_id="generate_labels", python_callable=generate_labels_task)
