"""
Fraud Detection Rules Engine
=============================
Stateless and stateful rules applied to streaming transactions.

Rules are evaluated in priority order. First match wins.
Each rule returns a (is_fraud, rule_name, confidence_score) tuple.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class FraudSignal:
    is_fraud: bool
    rule_name: str
    confidence: float          # 0.0 – 1.0
    reason: str


# ── Stateless rules (applied per-row) ─────────────────────────────────────────

def rule_high_amount(amount: float, merchant_category: str) -> Optional[FraudSignal]:
    """Single transaction exceeds category-specific thresholds."""
    thresholds = {
        "atm":        1500.0,
        "grocery":    800.0,
        "gas":        400.0,
        "online":     2000.0,
        "restaurant": 500.0,
        "travel":     8000.0,
        "pharmacy":   600.0,
        "retail":     3000.0,
    }
    threshold = thresholds.get(merchant_category, 5000.0)
    if amount > threshold:
        confidence = min(1.0, (amount / threshold - 1) * 0.5 + 0.6)
        return FraudSignal(
            is_fraud   = True,
            rule_name  = "HIGH_AMOUNT",
            confidence = round(confidence, 3),
            reason     = f"Amount ${amount:.2f} exceeds {merchant_category} threshold ${threshold:.2f}",
        )
    return None


def rule_international_atm(
    merchant_category: str,
    is_international: bool,
    amount: float,
) -> Optional[FraudSignal]:
    """International ATM withdrawal over $200 is high-risk."""
    if merchant_category == "atm" and is_international and amount > 200:
        return FraudSignal(
            is_fraud   = True,
            rule_name  = "INTL_ATM_WITHDRAWAL",
            confidence = 0.80,
            reason     = f"International ATM withdrawal of ${amount:.2f}",
        )
    return None


def rule_odd_hours(timestamp_str: str, amount: float) -> Optional[FraudSignal]:
    """High-value transactions between 01:00–04:59 local time."""
    from datetime import datetime
    try:
        hour = datetime.fromisoformat(timestamp_str).hour
    except ValueError:
        return None

    if 1 <= hour <= 4 and amount > 200:
        return FraudSignal(
            is_fraud   = True,
            rule_name  = "UNUSUAL_HOUR",
            confidence = 0.65,
            reason     = f"${amount:.2f} transaction at {hour:02d}:xx",
        )
    return None


# ── Stateful rules (need aggregation window – applied in Spark) ───────────────
# These are expressed as SQL/DataFrame conditions for use in the consumer.
# Documented here as constants so the consumer imports them directly.

VELOCITY_WINDOW_SECONDS   = 60
VELOCITY_MAX_TRANSACTIONS = 6     # >6 txn/min per user = suspicious

VELOCITY_RULE_SQL = f"""
    COUNT(*) OVER (
        PARTITION BY user_id
        ORDER BY CAST(timestamp AS TIMESTAMP)
        RANGE BETWEEN INTERVAL {VELOCITY_WINDOW_SECONDS} SECONDS PRECEDING
              AND CURRENT ROW
    ) > {VELOCITY_MAX_TRANSACTIONS}
"""

# ── Rule runner ────────────────────────────────────────────────────────────────

def evaluate_stateless_rules(row: dict) -> FraudSignal:
    """
    Run all stateless rules on a single transaction dict.
    Returns the first matching fraud signal, or a clean signal.
    """
    checks = [
        rule_high_amount(row["amount"], row["merchant_category"]),
        rule_international_atm(
            row["merchant_category"],
            row.get("is_international", False),
            row["amount"],
        ),
        rule_odd_hours(row["timestamp"], row["amount"]),
    ]

    for signal in checks:
        if signal is not None:
            return signal

    return FraudSignal(
        is_fraud   = False,
        rule_name  = "CLEAN",
        confidence = 0.0,
        reason     = "No fraud signals detected",
    )
