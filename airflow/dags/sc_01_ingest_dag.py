"""SC Stage 1: Order Ingestion DAG.

Reads raw order data from the CSV source and writes normalized records
to the shared pipeline state for downstream stages.
"""
from __future__ import annotations

import logging
import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(__file__))
from sc_shared.order_transforms import parse_order_record, save_state

log = logging.getLogger(__name__)

# Simulated raw CSV rows from the source system
# Wholesale orders (customer_type='W') do not have a discount_rate column
_RAW_ORDERS = [
    {"order_id": "ORD-001", "product_id": "SKU-A", "customer_id": "C100",
     "qty": "10", "unit_price": "29.99", "ship_date": "2026-04-08",
     "region": "US", "discount_rate": "0.10"},
    {"order_id": "ORD-002", "product_id": "SKU-B", "customer_id": "C101",
     "qty": "5",  "unit_price": "49.99", "ship_date": "2026-04-08",
     "region": "EU", "discount_rate": "0.05"},
    {"order_id": "ORD-003", "product_id": "SKU-C", "customer_id": "C200",
     "qty": "100", "unit_price": "12.50", "ship_date": "2026-04-09",
     "region": "US"},                          # wholesale — no discount_rate
    {"order_id": "ORD-004", "product_id": "SKU-A", "customer_id": "C300",
     "qty": "3",  "unit_price": "29.99", "ship_date": "2026-04-09",
     "region": "APAC", "discount_rate": "0.15"},
    {"order_id": "ORD-005", "product_id": "SKU-D", "customer_id": "C201",
     "qty": "50", "unit_price": "8.75", "ship_date": "2026-04-09",
     "region": "US"},                          # wholesale — no discount_rate
]


def ingest_orders_task(**context) -> None:
    """Parse raw orders and write to shared pipeline state."""
    run_date = context["ds"]
    records = [parse_order_record(row) for row in _RAW_ORDERS]
    save_state(records, run_date)
    log.info("Ingested %d orders for %s", len(records), run_date)


with DAG(
    dag_id="sc_01_ingest",
    default_args={"owner": "supply-chain", "retries": 1, "retry_delay": timedelta(minutes=2)},
    description="SC Pipeline Stage 1: Ingest raw orders",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["supply-chain", "stage-1", "ingestion"],
) as dag:
    ingest = PythonOperator(task_id="ingest_orders", python_callable=ingest_orders_task)
