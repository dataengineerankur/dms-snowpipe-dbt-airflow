def compute_continuous_growth_factor(rate: float, periods: int) -> float:
"""Return e^(rate * periods) for continuous compounding models."""
    exponent = rate * periods
    if exponent > 709:
        raise ValueError(f"rate*periods={exponent:.2f} exceeds safe exponent range; got rate={rate}, periods={periods}")
    return math.exp(exponent)