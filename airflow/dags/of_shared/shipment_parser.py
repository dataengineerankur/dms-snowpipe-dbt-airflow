"""Shipment record parsing and shared state utilities for the Order Fulfillment pipeline."""
from __future__ import annotations

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

_STATE_DIR = "/tmp/of_pipeline"


def get_state_path(run_date: str) -> str:
    os.makedirs(_STATE_DIR, exist_ok=True)
    return os.path.join(_STATE_DIR, f"shipments_{run_date}.json")


def load_state(run_date: str) -> list[dict[str, Any]]:
    """Load shipment records from shared pipeline state."""
    path = get_state_path(run_date)
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)


def save_state(records: list[dict[str, Any]], run_date: str) -> None:
    """Persist shipment records to shared pipeline state."""
    path = get_state_path(run_date)
    with open(path, "w") as f:
        json.dump(records, f, indent=2)
    logger.info("Saved %d shipment records to %s", len(records), path)


def parse_shipment(row: dict[str, Any]) -> dict[str, Any]:
    """Parse a raw WMS export row into a normalized shipment record."""
    return {
        "shipment_id":   row["shipment_id"],
        "order_id":      row["order_id"],
        "carrier_id":    int(row["carrier_id"]),
        "weight_kg":     float(row["weight_kg"]),
        "destination":   row["destination"],
        "service_level": row.get("service_level", "standard"),
        "customer_id":   row["customer_id"],
    }
