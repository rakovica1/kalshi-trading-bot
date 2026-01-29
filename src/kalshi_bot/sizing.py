import math


def calculate_position(balance_cents, price_cents, risk_pct=0.01):
    """Calculate the number of contracts to buy given risk constraints."""
    if price_cents <= 0 or balance_cents <= 0:
        return 0
    max_risk_cents = balance_cents * risk_pct
    contracts = math.floor(max_risk_cents / price_cents)
    return max(contracts, 0)
