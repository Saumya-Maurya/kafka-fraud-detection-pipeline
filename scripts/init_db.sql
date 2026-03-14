-- Fraud Detection Pipeline — Database Initialization
-- Run automatically by Docker on first start (via /docker-entrypoint-initdb.d/)

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── Transactions (all events) ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS transactions (
    id                  BIGSERIAL PRIMARY KEY,
    transaction_id      UUID         NOT NULL UNIQUE,
    user_id             VARCHAR(20)  NOT NULL,
    amount              NUMERIC(12,2) NOT NULL,
    currency            CHAR(3)      NOT NULL DEFAULT 'USD',
    merchant_id         VARCHAR(20)  NOT NULL,
    merchant_name       VARCHAR(100) NOT NULL,
    merchant_category   VARCHAR(30)  NOT NULL,
    merchant_country    CHAR(2)      NOT NULL,
    card_type           VARCHAR(10)  NOT NULL,
    card_last4          CHAR(4)      NOT NULL,
    event_time          TIMESTAMPTZ  NOT NULL,
    is_international    BOOLEAN      NOT NULL DEFAULT FALSE,
    final_is_fraud      BOOLEAN      NOT NULL DEFAULT FALSE,
    final_rule          VARCHAR(40),
    fraud_confidence    NUMERIC(5,3),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_txn_user_id   ON transactions(user_id);
CREATE INDEX idx_txn_event_time ON transactions(event_time DESC);
CREATE INDEX idx_txn_is_fraud  ON transactions(final_is_fraud) WHERE final_is_fraud = TRUE;

-- ── Fraud Alerts (fraud only, denormalised for fast dashboard queries) ─────────
CREATE TABLE IF NOT EXISTS fraud_alerts (
    id                  BIGSERIAL PRIMARY KEY,
    transaction_id      UUID         NOT NULL UNIQUE,
    user_id             VARCHAR(20)  NOT NULL,
    amount              NUMERIC(12,2) NOT NULL,
    merchant_category   VARCHAR(30)  NOT NULL,
    merchant_country    CHAR(2)      NOT NULL,
    final_rule          VARCHAR(40)  NOT NULL,
    fraud_confidence    NUMERIC(5,3),
    fraud_reason        TEXT,
    alert_time          TIMESTAMPTZ  NOT NULL,
    reviewed            BOOLEAN      NOT NULL DEFAULT FALSE,
    reviewed_at         TIMESTAMPTZ,
    true_positive       BOOLEAN,                -- set by analyst
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_alert_user_id   ON fraud_alerts(user_id);
CREATE INDEX idx_alert_rule      ON fraud_alerts(final_rule);
CREATE INDEX idx_alert_time      ON fraud_alerts(alert_time DESC);
CREATE INDEX idx_alert_reviewed  ON fraud_alerts(reviewed) WHERE reviewed = FALSE;

-- ── Metrics view (used in the analysis notebook) ────────────────────────────
CREATE VIEW fraud_metrics AS
SELECT
    DATE_TRUNC('hour', event_time)  AS hour,
    COUNT(*)                        AS total_transactions,
    SUM(CASE WHEN final_is_fraud THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(
        100.0 * SUM(CASE WHEN final_is_fraud THEN 1 ELSE 0 END) / COUNT(*),
        3
    )                               AS fraud_rate_pct,
    SUM(amount)                     AS total_volume_usd,
    SUM(CASE WHEN final_is_fraud THEN amount ELSE 0 END) AS fraud_volume_usd
FROM transactions
GROUP BY 1
ORDER BY 1 DESC;

-- ── Rule performance view ────────────────────────────────────────────────────
CREATE VIEW rule_performance AS
SELECT
    final_rule,
    COUNT(*)                          AS alert_count,
    ROUND(AVG(fraud_confidence), 3)   AS avg_confidence,
    ROUND(AVG(amount), 2)             AS avg_amount,
    MIN(alert_time)                   AS first_seen,
    MAX(alert_time)                   AS last_seen
FROM fraud_alerts
GROUP BY final_rule
ORDER BY alert_count DESC;

-- Seed a few test users for the notebook demo
INSERT INTO transactions (
    transaction_id, user_id, amount, currency,
    merchant_id, merchant_name, merchant_category, merchant_country,
    card_type, card_last4, event_time, is_international,
    final_is_fraud, final_rule, fraud_confidence
) VALUES
    (uuid_generate_v4(), 'USR_00001', 45.99,  'USD', 'MCH_001', 'Whole Foods', 'grocery',    'US', 'credit', '4821', NOW() - INTERVAL '2 hours', FALSE, FALSE, 'CLEAN', 0),
    (uuid_generate_v4(), 'USR_00001', 5250.00,'USD', 'MCH_008', 'Best Buy',    'retail',     'US', 'credit', '4821', NOW() - INTERVAL '1 hour',  TRUE,  TRUE,  'HIGH_AMOUNT', 0.875),
    (uuid_generate_v4(), 'USR_00002', 280.00, 'EUR', 'MCH_004', 'Chase ATM',   'atm',        'NL', 'debit',  '9134', NOW() - INTERVAL '30 mins', TRUE,  TRUE,  'INTL_ATM_WITHDRAWAL', 0.80),
    (uuid_generate_v4(), 'USR_00003', 12.50,  'USD', 'MCH_005', 'Chipotle',    'restaurant', 'US', 'debit',  '7711', NOW(),                       FALSE, FALSE, 'CLEAN', 0)
ON CONFLICT DO NOTHING;
