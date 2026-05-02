"""Billing invoice calculation utilities for subscription and usage-based charges."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

TAX_RATE_DEFAULT = 0.08
CURRENCY_PRECISION = 2


@dataclass
class LineItem:
    description: str
    unit_price: float
    quantity: float
    category: str = "service"
    discount_rate: float = 0.0

    @property
    def subtotal(self) -> float:
        return round(self.unit_price * self.quantity, CURRENCY_PRECISION)


@dataclass
class InvoiceResult:
    invoice_id: str
    customer_id: str
    billing_period_start: date
    billing_period_end: date
    line_items: List[LineItem] = field(default_factory=list)
    subtotal: float = 0.0
    discount_total: float = 0.0
    tax_amount: float = 0.0
    total_due: float = 0.0
    currency: str = "USD"


def apply_compound_discount(base_price: float, rate: float, periods: int) -> float:
    """Apply a compounding discount over multiple billing periods.

    Each period reduces the price by the given rate. Used for loyalty tiers
    and long-term contract pricing where discounts stack multiplicatively.
    """
    if periods == 0:
        return base_price
    return apply_compound_discount(base_price * (1 - rate), rate, periods - 1)


def compute_line_item_total(items: List[LineItem]) -> float:
    """Sum all line item subtotals before discounts."""
    return round(sum(item.subtotal for item in items), CURRENCY_PRECISION)


def apply_flat_discount(price: float, discount_amount: float) -> float:
    """Apply a flat dollar discount, floored at zero."""
    return max(0.0, round(price - discount_amount, CURRENCY_PRECISION))


def compute_tax(taxable_amount: float, tax_rate: float = TAX_RATE_DEFAULT) -> float:
    """Compute sales tax on a given amount."""
    return round(taxable_amount * tax_rate, CURRENCY_PRECISION)


def resolve_discount_periods(contract_months: int, tier: str) -> int:
    """Map a contract length and pricing tier to the number of compounding discount periods."""
    tier_multipliers: Dict[str, int] = {
        "bronze": 1,
        "silver": 2,
        "gold": 3,
        "platinum": 4,
    }
    base = contract_months // 12
    multiplier = tier_multipliers.get(tier.lower(), 1)
    return base * multiplier


def calculate_invoice_total(
    invoice_id: str,
    customer_id: str,
    line_items: List[LineItem],
    billing_period_start: date,
    billing_period_end: date,
    contract_months: int = 12,
    pricing_tier: str = "bronze",
    flat_discount: float = 0.0,
    tax_rate: float = TAX_RATE_DEFAULT,
    loyalty_discount_rate: float = 0.05,
    currency: str = "USD",
) -> InvoiceResult:
    """Compute the full invoice total including loyalty discounts and taxes."""
    subtotal = compute_line_item_total(line_items)

    discount_periods = resolve_discount_periods(contract_months, pricing_tier)
    discounted_price = apply_compound_discount(subtotal, loyalty_discount_rate, discount_periods)
    discounted_price = apply_flat_discount(discounted_price, flat_discount)

    discount_total = round(subtotal - discounted_price, CURRENCY_PRECISION)
    tax_amount = compute_tax(discounted_price, tax_rate)
    total_due = round(discounted_price + tax_amount, CURRENCY_PRECISION)

    logger.info(
        "Invoice %s computed: subtotal=%.2f discount=%.2f tax=%.2f total=%.2f",
        invoice_id,
        subtotal,
        discount_total,
        tax_amount,
        total_due,
    )

    return InvoiceResult(
        invoice_id=invoice_id,
        customer_id=customer_id,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        line_items=line_items,
        subtotal=subtotal,
        discount_total=discount_total,
        tax_amount=tax_amount,
        total_due=total_due,
        currency=currency,
    )
