"""
Revenue Attribution Pipeline — Multi-Source Commission Calculation
==================================================================
Pulls raw order events from Snowflake, joins with campaign metadata from
S3, calculates channel-weighted attribution scores, then writes the final
commission ledger back to Snowflake.

This DAG has THREE real bugs intentionally introduced for PATCHIT E2E testing:

  Bug 1 (line ~65): AttributeError — `campaign_meta.get("channels")` can
    return None when the campaign has no channel config; the code then calls
    .items() on None, blowing up with:
      AttributeError: 'NoneType' object has no attribute 'items'

  Bug 2 (line ~95): TypeError — order timestamps are stored as Unix epoch
    integers in Snowflake but the code calls datetime.strptime() expecting a
    string, causing:
      TypeError: strptime() argument 1 must be str, not int

  Bug 3 (line ~130): ZeroDivisionError — total_attributed_revenue can be
    zero when all orders are outside the attribution window; the code then
    divides to compute ROI without guarding:
      ZeroDivisionError: float division by zero
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
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
}


# ── helpers ────────────────────────────────────────────────────────────────────

def _mock_snowflake_orders() -> list[dict]:
    """Simulate Snowflake query: SELECT order_id, amount_usd, channel_code,
    campaign_id, created_at FROM orders WHERE created_at >= DATEADD(day,-7,NOW())
    """
    return [
        {"order_id": "ORD-001", "amount_usd": 149.99, "channel_code": "paid_search",
         "campaign_id": "CMP-42", "created_at": 1712500000},   # ← Unix epoch int
        {"order_id": "ORD-002", "amount_usd": 89.00, "channel_code": "email",
         "campaign_id": "CMP-17", "created_at": 1712510000},
        {"order_id": "ORD-003", "amount_usd": 220.50, "channel_code": "organic",
         "campaign_id": None, "created_at": 1712520000},
    ]


def _mock_campaign_metadata() -> dict[str, dict]:
    """Simulate S3 JSON: campaign_id → {channels: {channel: weight}, budget: float}"""
    return {
        "CMP-42": {
            "name": "Spring Sale",
            "budget_usd": 5000.0,
            "channels": {
                "paid_search": 0.6,
                "social": 0.3,
                "email": 0.1,
            },
        },
        "CMP-17": {
            # BUG 1: 'channels' key is missing for this campaign →
            # campaign_meta.get("channels") returns None → .items() fails
            "name": "Email Drip Q2",
            "budget_usd": 800.0,
        },
    }


# ── Task 1: extract & attribute ────────────────────────────────────────────────

def extract_and_attribute(**context) -> None:
    """Pull orders, join campaign weights, compute attributed revenue per channel."""
    orders = _mock_snowflake_orders()
    campaigns = _mock_campaign_metadata()

    attributed: list[dict] = []

    for order in orders:
        cid = order.get("campaign_id")
        if not cid:
            continue

        campaign_meta = campaigns.get(cid, {})

        channel_weights = campaign_meta.get("channels") or {}
        for ch, weight in channel_weights.items():
            attributed.append({
                "order_id": order["order_id"],
                "campaign_id": cid,
                "channel": ch,
                "attributed_revenue": order["amount_usd"] * weight,
            })

    context["ti"].xcom_push(key="attributed_rows", value=attributed)
    log.info("Attributed %d channel-revenue rows", len(attributed))


# ── Task 2: timestamp enrichment ───────────────────────────────────────────────

def enrich_with_timestamps(**context) -> None:
    """Parse order timestamps and attach ISO-8601 date strings for the ledger."""
    ti = context["ti"]
    orders = _mock_snowflake_orders()

    enriched = []
    for order in orders:
        raw_ts = order["created_at"]

        # BUG 2: created_at is an int (Unix epoch), not a string.
        # datetime.strptime() requires a string → TypeError.
        # Fix should be: dt = datetime.utcfromtimestamp(raw_ts)
        dt = datetime.strptime(raw_ts, "%Y-%m-%dT%H:%M:%S")  # ← TypeError here

        enriched.append({**order, "order_date": dt.strftime("%Y-%m-%d")})

    ti.xcom_push(key="enriched_orders", value=enriched)
    log.info("Enriched %d orders with timestamps", len(enriched))


# ── Task 3: ROI calculation ────────────────────────────────────────────────────

def calculate_roi(**context) -> None:
    """Compute return-on-investment per campaign using attributed revenue vs spend."""
    ti = context["ti"]
    attributed = ti.xcom_pull(key="attributed_rows", task_ids="extract_and_attribute") or []
    campaigns = _mock_campaign_metadata()

    roi_by_campaign: dict[str, float] = {}

    for cid, meta in campaigns.items():
        budget = meta.get("budget_usd", 0.0)
        campaign_revenue = sum(
            r["attributed_revenue"] for r in attributed if r["campaign_id"] == cid
        )

        # BUG 3: when no orders fall within the attribution window (campaign_revenue == 0
        # or attributed list is empty), total_attributed_revenue is 0.0 → ZeroDivisionError.
        # Fix should be: roi = campaign_revenue / budget if budget else 0.0
        total_attributed_revenue = sum(r["attributed_revenue"] for r in attributed)
        roi = campaign_revenue / total_attributed_revenue   # ← ZeroDivisionError when 0

        roi_by_campaign[cid] = round(roi, 4)

    ti.xcom_push(key="roi_by_campaign", value=roi_by_campaign)
    log.info("ROI computed for %d campaigns: %s", len(roi_by_campaign), roi_by_campaign)


# ── Task 4: write ledger ───────────────────────────────────────────────────────

def write_commission_ledger(**context) -> None:
    """Write the final attribution + ROI ledger to Snowflake reporting schema."""
    ti = context["ti"]
    attributed = ti.xcom_pull(key="attributed_rows", task_ids="extract_and_attribute") or []
    roi = ti.xcom_pull(key="roi_by_campaign", task_ids="calculate_roi") or {}
    enriched = ti.xcom_pull(key="enriched_orders", task_ids="enrich_with_timestamps") or []

    ledger = {
        "generated_at": datetime.utcnow().isoformat(),
        "attributed_rows": len(attributed),
        "enriched_orders": len(enriched),
        "roi_by_campaign": roi,
    }
    log.info("Ledger written: %s", json.dumps(ledger, indent=2))


# ── DAG ────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="revenue_attribution_pipeline",
    description="Multi-source revenue attribution and commission ledger — PATCHIT complex E2E test",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["patchit", "e2e-complex", "revenue", "attribution"],
) as dag:

    extract = PythonOperator(
        task_id="extract_and_attribute",
        python_callable=extract_and_attribute,
    )

    enrich = PythonOperator(
        task_id="enrich_with_timestamps",
        python_callable=enrich_with_timestamps,
    )

    roi = PythonOperator(
        task_id="calculate_roi",
        python_callable=calculate_roi,
    )

    write = PythonOperator(
        task_id="write_commission_ledger",
        python_callable=write_commission_ledger,
    )

    extract >> enrich >> roi >> write
