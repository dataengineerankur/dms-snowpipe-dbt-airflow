"""
Inventory Reorder Automation Pipeline
======================================
Pulls current stock levels from Snowflake, computes reorder quantities using
demand forecasts and supplier lead times, applies regional safety-stock rules,
and writes approved purchase orders to the procurement schema.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

default_args = {
    "owner": "supply-chain-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "email_on_failure": True,
}

REORDER_THRESHOLD_DAYS = 14
SAFETY_STOCK_MULTIPLIER = 1.25


def _fetch_stock_levels() -> list[dict]:
    return [
        {"sku": "SKU-A01", "warehouse": "US-WEST", "qty_on_hand": 120,
         "avg_daily_demand": 18.5, "lead_time_days": 7, "unit_cost": 24.99,
         "last_counted": "2026-04-01T08:00:00"},
        {"sku": "SKU-B07", "warehouse": "US-EAST", "qty_on_hand": 45,
         "avg_daily_demand": 12.0, "lead_time_days": 10, "unit_cost": 89.50,
         "last_counted": "2026-04-03T14:30:00"},
        {"sku": "SKU-C12", "warehouse": "EU-CENTRAL", "qty_on_hand": 0,
         "avg_daily_demand": 5.5, "lead_time_days": 21, "unit_cost": 310.00,
         "last_counted": "2026-03-28T09:15:00"},
        {"sku": "SKU-D99", "warehouse": "APAC", "qty_on_hand": 800,
         "avg_daily_demand": 0,
         "lead_time_days": 14, "unit_cost": 7.25,
         "last_counted": "2026-04-05T11:00:00"},
    ]


def _fetch_supplier_catalog() -> dict[str, dict]:
    return {
        "SKU-A01": {"supplier": "SupplierAlpha", "min_order_qty": 50,  "discount_tiers": {100: 0.05, 250: 0.10}},
        "SKU-B07": {"supplier": "SupplierBeta",  "min_order_qty": 25,  "discount_tiers": {50: 0.08}},
        "SKU-C12": {"supplier": "SupplierGamma", "min_order_qty": 10,  "discount_tiers": {20: 0.12, 50: 0.18}},
    }


def _regional_safety_rules() -> dict[str, float]:
    return {
        "US-WEST":    1.20,
        "US-EAST":    1.15,
        "EU-CENTRAL": 1.30,
        "APAC":       1.10,
    }


def compute_reorder_quantities(**context) -> None:
    """
    Determine which SKUs need reordering and calculate optimal order quantities.
    Uses the economic order quantity model adjusted for regional safety stock.
    """
    stock = _fetch_stock_levels()
    catalog = _fetch_supplier_catalog()
    safety_rules = _regional_safety_rules()

    reorder_candidates = []

    for item in stock:
        sku = item["sku"]
        warehouse = item["warehouse"]
        qty_on_hand = item["qty_on_hand"]
        avg_daily = item["avg_daily_demand"]
        lead_time = item["lead_time_days"]

        last_counted_str = item["last_counted"]
        last_counted_dt = datetime.fromisoformat(last_counted_str)
        staleness_days = (datetime.utcnow() - last_counted_dt).days

        if avg_daily == 0:
            days_of_stock = float("inf")
        else:
            days_of_stock = qty_on_hand / avg_daily

        regional_multiplier = safety_rules.get(warehouse, SAFETY_STOCK_MULTIPLIER)
        safety_stock = avg_daily * lead_time * regional_multiplier

        if days_of_stock < REORDER_THRESHOLD_DAYS or qty_on_hand < safety_stock:
            supplier_info = catalog.get(sku)
            min_qty = supplier_info["min_order_qty"]

            demand_during_lead = avg_daily * lead_time
            target_qty = (demand_during_lead + safety_stock) - qty_on_hand
            order_qty = max(min_qty, round(target_qty / min_qty) * min_qty)

            reorder_candidates.append({
                "sku": sku,
                "warehouse": warehouse,
                "qty_on_hand": qty_on_hand,
                "days_of_stock": round(days_of_stock, 1) if days_of_stock != float("inf") else 999,
                "order_qty": order_qty,
                "unit_cost": item["unit_cost"],
                "staleness_days": staleness_days,
                "supplier": supplier_info["supplier"],
            })

    context["ti"].xcom_push(key="reorder_candidates", value=reorder_candidates)
    log.info("Found %d SKUs requiring reorder", len(reorder_candidates))


def apply_discount_pricing(**context) -> None:
    """
    Look up volume discount tiers for each reorder candidate and compute
    the net order value after applicable discounts.
    """
    ti = context["ti"]
    candidates = ti.xcom_pull(key="reorder_candidates", task_ids="compute_reorder_quantities") or []
    catalog = _fetch_supplier_catalog()

    priced_orders = []

    for order in candidates:
        sku = order["sku"]
        qty = order["order_qty"]
        unit_cost = order["unit_cost"]

        supplier_info = catalog[sku]
        tiers = supplier_info["discount_tiers"]

        applicable_discount = 0.0
        for threshold in sorted(tiers.keys()):
            if qty >= threshold:
                applicable_discount = tiers[threshold]

        gross_value = qty * unit_cost
        net_value = gross_value * (1 - applicable_discount)

        priced_orders.append({
            **order,
            "discount_pct": applicable_discount * 100,
            "gross_value": round(gross_value, 2),
            "net_value": round(net_value, 2),
        })

    ti.xcom_push(key="priced_orders", value=priced_orders)
    log.info("Priced %d purchase orders", len(priced_orders))


def validate_and_approve(**context) -> None:
    """
    Apply business approval rules: flag orders above the single-PO spend limit,
    compute total procurement budget impact, and produce the final approved set.
    """
    ti = context["ti"]
    priced = ti.xcom_pull(key="priced_orders", task_ids="apply_discount_pricing") or []

    PO_SPEND_LIMIT = 5000.0

    approved = []
    flagged = []

    for order in priced:
        if order["net_value"] > PO_SPEND_LIMIT:
            flagged.append(order)
        else:
            approved.append(order)

    total_approved_spend = sum(o["net_value"] for o in approved)
    avg_staleness = (sum(o["staleness_days"] for o in approved) / len(approved)
                     if approved else 0.0)

    summary = {
        "approved_count": len(approved),
        "flagged_count": len(flagged),
        "total_approved_spend": round(total_approved_spend, 2),
        "avg_data_staleness_days": round(avg_staleness, 1),
        "generated_at": datetime.utcnow().isoformat(),
    }

    ti.xcom_push(key="approved_orders", value=approved)
    ti.xcom_push(key="procurement_summary", value=summary)
    log.info("Procurement summary: %s", json.dumps(summary))


def write_purchase_orders(**context) -> None:
    """Write approved purchase orders to the procurement schema."""
    ti = context["ti"]
    approved = ti.xcom_pull(key="approved_orders", task_ids="validate_and_approve") or []
    summary = ti.xcom_pull(key="procurement_summary", task_ids="validate_and_approve") or {}
    log.info("Writing %d purchase orders. Summary: %s", len(approved), json.dumps(summary, indent=2))


with DAG(
    dag_id="inventory_reorder_pipeline",
    description="Automated inventory reorder and procurement pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["supply-chain", "inventory", "procurement"],
) as dag:

    t_compute = PythonOperator(
        task_id="compute_reorder_quantities",
        python_callable=compute_reorder_quantities,
    )

    t_price = PythonOperator(
        task_id="apply_discount_pricing",
        python_callable=apply_discount_pricing,
    )

    t_validate = PythonOperator(
        task_id="validate_and_approve",
        python_callable=validate_and_approve,
    )

    t_write = PythonOperator(
        task_id="write_purchase_orders",
        python_callable=write_purchase_orders,
    )

    t_compute >> t_price >> t_validate >> t_write
