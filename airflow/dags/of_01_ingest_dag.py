"""OF Pipeline Stage 1: Ingest raw shipment data from WMS export.

Reads the daily WMS batch export and writes normalized shipment
records to the shared pipeline state for downstream processing.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(__file__))
from of_shared.shipment_parser import parse_shipment, save_state

log = logging.getLogger(__name__)

_WMS_EXPORT = [
    {"shipment_id": "SHP-5001", "order_id": "ORD-9001", "carrier_id": "101",
     "weight_kg": "2.3", "destination": "New York, NY", "customer_id": "C700",
     "service_level": "ground"},
    {"shipment_id": "SHP-5002", "order_id": "ORD-9002", "carrier_id": "103",
     "weight_kg": "0.8", "destination": "London, UK", "customer_id": "C701",
     "service_level": "express"},
    {"shipment_id": "SHP-5003", "order_id": "ORD-9003", "carrier_id": "102",
     "weight_kg": "5.1", "destination": "Chicago, IL", "customer_id": "C702",
     "service_level": "standard"},
    {"shipment_id": "SHP-5004", "order_id": "ORD-9004", "carrier_id": "104",
     "weight_kg": "1.2", "destination": "Austin, TX", "customer_id": "C703",
     "service_level": "priority"},
    {"shipment_id": "SHP-5005", "order_id": "ORD-9005", "carrier_id": "101",
     "weight_kg": "3.7", "destination": "Seattle, WA", "customer_id": "C704",
     "service_level": "ground"},
]


def ingest_shipments_task(**context) -> None:
    run_date = context["ds"]
    records = [parse_shipment(row) for row in _WMS_EXPORT]
    save_state(records, run_date)
    log.info("Ingested %d shipment records for %s", len(records), run_date)


with DAG(
    dag_id="of_01_ingest",
    default_args={"owner": "fulfillment", "retries": 2, "retry_delay": timedelta(minutes=3)},
    description="OF Pipeline Stage 1: Ingest WMS shipment data",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fulfillment", "stage-1", "ingestion"],
) as dag:
    ingest = PythonOperator(task_id="ingest_shipments", python_callable=ingest_shipments_task)
