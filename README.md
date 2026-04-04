# Real-Time Fraud Detection Pipeline

A production-pattern Kafka + Spark Streaming pipeline that ingests simulated
financial transactions, applies multi-layer fraud detection rules, and writes
alerts to PostgreSQL — all runnable locally with a single command.

---

## Architecture

**[Live architecture diagram](https://saumya-maurya.github.io/kafka-fraud-detection-pipeline/)** — interactive, click any component

## Key Stats

| Metric | Value |
|---|---|
| Throughput | 23 transactions/second (~2M/day) |
| Fraud rate | ~0.3% (configurable) |
| Alert latency | < 800ms end-to-end |
| Detection rules | 4 (3 stateless + 1 stateful velocity) |
| Storage | PostgreSQL with indexed views |

## Tech Stack

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka 7.4 (Confluent) |
| Stream processing | Apache Spark 3.4 Structured Streaming |
| Language | Python 3.11 + PySpark |
| Storage | PostgreSQL 15 |
| Orchestration | Docker Compose |
| Monitoring | Kafka UI (Provectus) |

---

## Getting Started

### Prerequisites

- Docker Desktop (Mac/Windows) or Docker Engine + Compose (Linux)
- 4 GB RAM available for containers
- Ports 9092, 8090, 5432 free

### Run (3 commands)

```bash
git clone https://github.com/Saumya-Maurya/kafka-fraud-detection-pipeline
cd kafka-fraud-detection-pipeline
make up
```

Watch the stream:
```bash
make logs
```

Open Kafka UI to see messages flowing:
```
http://localhost:8090
```

Check fraud alerts in PostgreSQL:
```bash
make db-check
```

### Stop

```bash
make down          # stop containers, keep data
make reset         # stop containers, wipe data
```

---

## Fraud Detection Rules

### Stateless Rules (applied per transaction, sub-millisecond)

| Rule | Condition | Confidence |
|---|---|---|
| `HIGH_AMOUNT` | Amount exceeds category threshold (e.g. ATM > $1,500) | 0.60–1.00 |
| `INTL_ATM_WITHDRAWAL` | International ATM > $200 | 0.80 |
| `UNUSUAL_HOUR` | 01:00–04:59 + amount > $200 | 0.65 |

### Stateful Rule (sliding window aggregation)

| Rule | Condition | Window |
|---|---|---|
| `VELOCITY_BURST` | User makes > 6 transactions in 60 seconds | 60s sliding, 10s slide |

### Adding a New Rule

1. Add a function to `consumer/fraud_rules.py`:
```python
def rule_my_new_rule(amount: float, merchant_category: str) -> Optional[FraudSignal]:
    if ...:
        return FraudSignal(is_fraud=True, rule_name="MY_RULE", confidence=0.75, reason="...")
    return None
```

2. Register it in `evaluate_stateless_rules()`.

3. No changes needed in the consumer — the UDF picks it up automatically.

---

## Project Structure

```
kafka-fraud-detection-pipeline/
├── docker-compose.yml            # Full stack: Kafka + Spark + Postgres + UI
├── Dockerfile.producer           # Transaction generator image
├── Dockerfile.consumer           # Spark consumer image
├── Makefile                      # Convenience commands
├── requirements.txt
│
├── schemas/
│   └── transaction_schema.py     # Single source of truth for data model
│
├── producer/
│   └── transaction_producer.py   # Kafka producer + fraud injection patterns
│
├── consumer/
│   ├── fraud_detection_consumer.py  # Spark Structured Streaming pipeline
│   └── fraud_rules.py               # Stateless + stateful rule definitions
│
├── scripts/
│   └── init_db.sql               # PostgreSQL schema + views
│
└── notebooks/
    └── fraud_analysis.py         # Results analysis (convert to .ipynb)
```

---

## Configuration

All parameters are controlled via environment variables in `docker-compose.yml`:

| Variable | Default | Description |
|---|---|---|
| `TRANSACTIONS_PER_SECOND` | `23` | Producer throughput |
| `KAFKA_TOPIC` | `transactions` | Kafka topic name |
| `VELOCITY_WINDOW_SECONDS` | `60` | Velocity detection window |
| `VELOCITY_MAX_TRANSACTIONS` | `6` | Max txn/window before alert |

---

## Results Analysis

After the pipeline has run for ~5 minutes:

```bash
make notebook
```

This opens the analysis notebook showing:
- Fraud rate over time
- Rule performance breakdown
- Amount distribution (fraud vs legitimate)
- Top high-risk users

---

## Interview Talking Points

**"Walk me through the architecture."**
> Transactions are generated at 23/sec to simulate real card volume (~2M/day). They're published to Kafka keyed by user_id for ordered processing. Spark Structured Streaming reads from Kafka, parses the JSON, and applies two layers of detection: stateless rules via a UDF (instant, per-row) and a stateful velocity check using a 60-second sliding window with a 10-second slide. Both layers write to PostgreSQL — all transactions to a `transactions` table, and only flagged events to `fraud_alerts`. Latency is under 800ms.

**"Why Kafka instead of just reading from a database?"**
> Kafka decouples the producer (card terminal / payment gateway) from the consumer (fraud engine). Multiple consumers can read the same stream independently — for example, a real-time rules engine and a batch ML model can both consume without interfering. It also provides durability and replay — if the Spark consumer crashes, it resumes from the last checkpoint.

**"What would you change to make this production-ready?"**
> Three things: (1) replace the rule-based system with an ML model trained on historical labelled data — the rule engine provides training labels and a safety net; (2) add a Schema Registry to enforce Avro schema on producers so bad events are rejected at the broker; (3) move the sink from PostgreSQL to a dedicated alert system (PagerDuty / SNS) with PostgreSQL as the audit log only.

---

## License

MIT
