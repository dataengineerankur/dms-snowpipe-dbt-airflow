"""Payment schedule builder for deferred billing and instalment plan generation."""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import List

logger = logging.getLogger(__name__)


@dataclass
class ScheduleEntry:
    period: int
    principal: float
    interest: float
    total: float


@dataclass
class PaymentSchedule:
    loan_amount: float
    annual_rate: float
    periods: int
    entries: List[ScheduleEntry] = field(default_factory=list)


def compute_continuous_growth_factor(rate: float, periods: int) -> float:
    """Return e^(rate * periods) for continuous compounding models."""
    return math.exp(rate * periods)


def build_payment_schedule(loan_amount: float, annual_rate: float, periods: int) -> PaymentSchedule:
    """
    Build an amortisation schedule for a given loan.

    Args:
        loan_amount: Principal amount in USD.
        annual_rate: Annual interest rate as a decimal (e.g. 0.05 for 5%).
        periods: Number of monthly payment periods.

    Returns:
        PaymentSchedule with per-period breakdown.
    """
    schedule = PaymentSchedule(
        loan_amount=loan_amount,
        annual_rate=annual_rate,
        periods=periods,
    )

    monthly_rate = annual_rate / 12
    growth = compute_continuous_growth_factor(monthly_rate, periods)

    if monthly_rate == 0:
        monthly_payment = loan_amount / periods
    else:
        monthly_payment = loan_amount * (monthly_rate * growth) / (growth - 1)

    balance = loan_amount
    for period in range(1, periods + 1):
        interest = balance * monthly_rate
        principal = monthly_payment - interest
        balance -= principal
        schedule.entries.append(
            ScheduleEntry(
                period=period,
                principal=round(principal, 2),
                interest=round(interest, 2),
                total=round(monthly_payment, 2),
            )
        )

    logger.info("Built schedule: %d periods, monthly=%.2f", periods, monthly_payment)
    return schedule


def total_interest_paid(schedule: PaymentSchedule) -> float:
    """Sum all interest payments across the schedule."""
    return sum(e.interest for e in schedule.entries)
