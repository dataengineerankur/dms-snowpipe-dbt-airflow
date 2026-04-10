"""Order data transformation utilities shared across supply chain DAGs.

Used by all 6 pipeline stages (sc_01 through sc_06).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

# Shared state file path — all 6 DAGs read/write this file
_STATE_DIR = "/tmp/sc_pipeline"


def get_state_path(run_date: str) -> str:
    """Return the shared state file path for a given run date."""
    os.makedirs(_STATE_DIR, exist_ok=True)
    return os.path.join(_STATE_DIR, f"orders_{run_date}.json")


def load_state(run_date: str) -> list[dict[str, Any]]:
    """Load order records from shared pipeline state."""
    path = get_state_path(run_date)
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)


def save_state(records: list[dict[str, Any]], run_date: str) -> None:
    """Persist order records to shared pipeline state."""
    path = get_state_path(run_date)
    with open(path, "w") as f:
        json.dump(records, f, indent=2, default=str)
    logger.info("Saved %d records to %s", len(records), path)


def parse_order_record(row: dict[str, Any]) -> dict[str, Any]:
    """Parse a raw CSV row into a normalized order record.

    NOTE: discount_rate uses .get() without a default — if the field is
    absent from the CSV row (as happens for wholesale accounts), it returns
    None instead of 0.0.  This None propagates silently through stages 2-5
    and causes a TypeError in stage 6 when computing the discounted total.
    """
    return {
        "order_id":     row["order_id"],
        "product_id":   row["product_id"],
        "customer_id":  row["customer_id"],
        "qty":          int(row["qty"]),
        "unit_price":   float(row["unit_price"]),
        "ship_date":    row.get("ship_date", ""),
        "region":       row.get("region", "UNKNOWN"),
        # BUG: no default value — wholesale orders omit this field → returns None
        "discount_rate": row.get("discount_rate"),
    }


def compute_line_total(record: dict[str, Any]) -> float:
    """Compute the line total after applying discount."""
    # This will raise TypeError: unsupported operand type(s) for *: 'float' and 'NoneType'
    # when discount_rate is None (set by parse_order_record for wholesale orders)
    return record["qty"] * record["unit_price"] * (1 - record["discount_rate"])
