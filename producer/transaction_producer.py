"""
Transaction Producer
====================
Generates realistic financial transaction events and publishes to Kafka.

Simulation parameters:
- ~23 transactions/second  →  ~2M/day
- ~0.3% base fraud rate
- Fraud patterns: high-amount, velocity spike, geo-anomaly, unusual hours
"""

import os
import sys
import uuid
import json
import time
import random
import logging
import signal
from datetime import datetime, timezone
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from schemas.transaction_schema import Transaction

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "transactions")
TPS                     = int(os.getenv("TRANSACTIONS_PER_SECOND", "23"))

# ── Reference data ────────────────────────────────────────────────────────────
MERCHANTS = [
    ("MCH_001", "Whole Foods",      "grocery",    "US"),
    ("MCH_002", "Shell Gas",        "gas",        "US"),
    ("MCH_003", "Amazon",           "online",     "US"),
    ("MCH_004", "Chase ATM",        "atm",        "US"),
    ("MCH_005", "Chipotle",         "restaurant", "US"),
    ("MCH_006", "Delta Airlines",   "travel",     "US"),
    ("MCH_007", "Walgreens",        "pharmacy",   "US"),
    ("MCH_008", "Best Buy",         "retail",     "US"),
    ("MCH_009", "BP Gas Station",   "gas",        "UK"),
    ("MCH_010", "Booking.com",      "travel",     "NL"),
    ("MCH_011", "LocalMart",        "grocery",    "US"),
    ("MCH_012", "Uber Eats",        "restaurant", "US"),
]

USER_POOL   = [f"USR_{i:05d}" for i in range(1, 5001)]   # 5,000 synthetic users
DEVICE_POOL = [f"DEV_{i:06d}" for i in range(1, 10001)]

# Known home country per user (first 4,000 → US, rest travel frequently)
def user_home_country(user_id: str) -> str:
    uid_num = int(user_id.split("_")[1])
    return "US" if uid_num <= 4000 else random.choice(["US", "UK", "CA"])


def generate_ip() -> str:
    return f"{random.randint(1,254)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"


def generate_coords(country: str) -> tuple[float, float]:
    """Rough bounding boxes per country."""
    boxes = {
        "US": ((25.0, 49.0), (-125.0, -67.0)),
        "UK": ((50.0, 58.5), (-5.5,   2.0)),
        "CA": ((43.0, 60.0), (-141.0, -53.0)),
        "NL": ((51.5, 53.5), (3.5,    7.0)),
    }
    lat_r, lon_r = boxes.get(country, ((25.0, 49.0), (-125.0, -67.0)))
    return (
        round(random.uniform(*lat_r), 6),
        round(random.uniform(*lon_r), 6),
    )


# ── Fraud injection patterns ──────────────────────────────────────────────────
class FraudInjector:
    """
    Injects realistic fraud patterns at configurable rates.

    Patterns:
      1. High-amount single transaction   (card not present / account takeover)
      2. Velocity burst                   (card stolen, rapid spend)
      3. Geographic anomaly               (transaction far from home country)
      4. Unusual hours (2-5 AM local)
    """

    def __init__(self):
        # Track recent transactions per user for velocity detection
        self._user_txn_counts: dict[str, list[float]] = {}

    def _record(self, user_id: str, ts: float):
        bucket = self._user_txn_counts.setdefault(user_id, [])
        bucket.append(ts)
        # Keep only last 60 seconds
        now = time.time()
        self._user_txn_counts[user_id] = [t for t in bucket if now - t < 60]

    def _velocity(self, user_id: str) -> int:
        return len(self._user_txn_counts.get(user_id, []))

    def inject(self, txn: Transaction, user_id: str) -> Transaction:
        now_ts = time.time()
        self._record(user_id, now_ts)
        hour = datetime.now().hour

        # Pattern 1: high-amount (0.15% of all transactions)
        if random.random() < 0.0015:
            txn.amount    = round(random.uniform(5001, 25000), 2)
            txn.is_fraud  = True
            return txn

        # Pattern 2: velocity burst (simulate card theft – 8+ txn/min)
        if self._velocity(user_id) >= 8:
            txn.is_fraud = True
            return txn

        # Pattern 3: geo-anomaly (home country US, txn in NL at > $500)
        if user_home_country(user_id) == "US" and txn.merchant_country != "US" and txn.amount > 500:
            if random.random() < 0.4:    # 40% of such events are fraud
                txn.is_fraud = True
                return txn

        # Pattern 4: unusual hours (2-5 AM) + ATM + high amount
        if 2 <= hour <= 5 and txn.merchant_category == "atm" and txn.amount > 300:
            txn.is_fraud = True
            return txn

        return txn


# ── Generator ─────────────────────────────────────────────────────────────────
_injector = FraudInjector()


def generate_transaction() -> Transaction:
    merchant_id, merchant_name, category, country = random.choice(MERCHANTS)
    user_id   = random.choice(USER_POOL)
    lat, lon  = generate_coords(country)

    # Realistic amount distribution: exponential with category-based shift
    base_means = {
        "grocery": 85, "gas": 60, "online": 120, "atm": 250,
        "restaurant": 45, "travel": 650, "pharmacy": 35, "retail": 180,
    }
    amount = round(abs(random.expovariate(1 / base_means.get(category, 100))), 2)
    amount = max(1.0, min(amount, 4999.99))   # cap before fraud injection

    txn = Transaction(
        transaction_id   = str(uuid.uuid4()),
        user_id          = user_id,
        amount           = amount,
        currency         = "USD" if country == "US" else ("GBP" if country == "UK" else "EUR"),
        merchant_id      = merchant_id,
        merchant_name    = merchant_name,
        merchant_category= category,
        merchant_country = country,
        card_type        = random.choice(["credit", "credit", "debit"]),   # 2:1 credit
        card_last4       = f"{random.randint(1000, 9999)}",
        timestamp        = datetime.now(timezone.utc).isoformat(),
        device_id        = random.choice(DEVICE_POOL) if random.random() > 0.05 else None,
        ip_address       = generate_ip() if category == "online" else None,
        latitude         = lat,
        longitude        = lon,
        is_international = country != user_home_country(user_id),
        is_fraud         = False,
    )

    return _injector.inject(txn, user_id)


# ── Kafka producer ────────────────────────────────────────────────────────────
def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",                    # wait for all replicas
        retries=5,
        compression_type="gzip",
        linger_ms=5,                   # small batching for throughput
        batch_size=32768,
    )


def on_send_error(exc):
    log.error("Failed to deliver message: %s", exc)


# ── Main loop ─────────────────────────────────────────────────────────────────
def run():
    log.info("Connecting to Kafka at %s …", KAFKA_BOOTSTRAP_SERVERS)
    producer = create_producer()
    log.info("Producer ready. Streaming to topic '%s' at %d txn/sec", KAFKA_TOPIC, TPS)

    interval      = 1.0 / TPS
    total_sent    = 0
    total_fraud   = 0
    running       = True

    def shutdown(sig, frame):
        nonlocal running
        log.info("Shutting down …")
        running = False

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    report_every = 1000   # log a summary every N messages
    start_time   = time.time()

    while running:
        loop_start = time.time()
        txn        = generate_transaction()

        producer.send(
            KAFKA_TOPIC,
            key=txn.user_id,       # partition by user_id for ordering
            value=txn.to_dict(),
        ).add_errback(on_send_error)

        total_sent  += 1
        total_fraud += int(txn.is_fraud)

        if total_sent % report_every == 0:
            elapsed      = time.time() - start_time
            actual_tps   = total_sent / elapsed
            fraud_rate   = (total_fraud / total_sent) * 100
            log.info(
                "Sent: %7d | Fraud: %5d (%.2f%%) | TPS: %.1f",
                total_sent, total_fraud, fraud_rate, actual_tps,
            )

        # Pace to target TPS
        elapsed = time.time() - loop_start
        sleep   = interval - elapsed
        if sleep > 0:
            time.sleep(sleep)

    producer.flush()
    producer.close()
    log.info("Producer stopped. Total sent: %d", total_sent)


if __name__ == "__main__":
    run()
