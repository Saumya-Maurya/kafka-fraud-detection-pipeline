"""
Fraud Detection Consumer
========================
Reads transactions from Kafka using Spark Structured Streaming,
applies stateless + stateful fraud rules, writes alerts to PostgreSQL.

Architecture:
  Kafka (transactions topic)
    → Spark readStream
    → JSON parse + schema enforcement
    → Stateless rules (UDF per-row)
    → Stateful velocity check (sliding window aggregation)
    → Union of fraud signals
    → Write alerts to PostgreSQL (fraud_alerts table)
    → Write all transactions to PostgreSQL (transactions table)

Run locally (without Docker):
  pip install pyspark kafka-python psycopg2-binary
  python consumer/fraud_detection_consumer.py
"""

import os
import sys
import json
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, TimestampType,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from consumer.fraud_rules import (
    evaluate_stateless_rules,
    VELOCITY_WINDOW_SECONDS,
    VELOCITY_MAX_TRANSACTIONS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC", "transactions")
PG_HOST          = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT          = os.getenv("POSTGRES_PORT", "5432")
PG_DB            = os.getenv("POSTGRES_DB", "fraud_db")
PG_USER          = os.getenv("POSTGRES_USER", "fraud_user")
PG_PASS          = os.getenv("POSTGRES_PASSWORD", "fraud_pass")
CHECKPOINT_DIR   = "/tmp/fraud_checkpoints"

PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_PROPS = {
    "user":   PG_USER,
    "password": PG_PASS,
    "driver": "org.postgresql.Driver",
}

# ── Spark session ──────────────────────────────────────────────────────────────
def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FraudDetectionPipeline")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
            "org.postgresql:postgresql:42.6.0"
        )
        .getOrCreate()
    )

# ── Schema ─────────────────────────────────────────────────────────────────────
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",    StringType(),  False),
    StructField("user_id",           StringType(),  False),
    StructField("amount",            DoubleType(),  False),
    StructField("currency",          StringType(),  False),
    StructField("merchant_id",       StringType(),  False),
    StructField("merchant_name",     StringType(),  False),
    StructField("merchant_category", StringType(),  False),
    StructField("merchant_country",  StringType(),  False),
    StructField("card_type",         StringType(),  False),
    StructField("card_last4",        StringType(),  False),
    StructField("timestamp",         StringType(),  False),
    StructField("device_id",         StringType(),  True),
    StructField("ip_address",        StringType(),  True),
    StructField("latitude",          DoubleType(),  True),
    StructField("longitude",         DoubleType(),  True),
    StructField("is_international",  BooleanType(), False),
    StructField("is_fraud",          BooleanType(), False),   # ground truth
])

# ── UDF: stateless rules ───────────────────────────────────────────────────────
@F.udf(returnType=StructType([
    StructField("is_fraud",   BooleanType(), False),
    StructField("rule_name",  StringType(),  False),
    StructField("confidence", DoubleType(),  False),
    StructField("reason",     StringType(),  False),
]))
def detect_fraud_udf(amount, merchant_category, is_international, timestamp):
    row = {
        "amount":            amount,
        "merchant_category": merchant_category,
        "is_international":  is_international,
        "timestamp":         timestamp,
    }
    from consumer.fraud_rules import evaluate_stateless_rules
    signal = evaluate_stateless_rules(row)
    return (signal.is_fraud, signal.rule_name, signal.confidence, signal.reason)


# ── Sink: write to PostgreSQL ──────────────────────────────────────────────────
def write_to_postgres(batch_df, batch_id: int, table: str):
    """Micro-batch writer for foreachBatch."""
    count = batch_df.count()
    if count == 0:
        return
    log.info("Batch %d → writing %d rows to %s", batch_id, count, table)
    (
        batch_df.write
        .jdbc(url=PG_URL, table=table, mode="append", properties=PG_PROPS)
    )


# ── Main pipeline ──────────────────────────────────────────────────────────────
def run():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session started.")

    # 1. Read raw stream from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Parse JSON payload
    parsed = (
        raw_stream
        .select(
            F.from_json(
                F.col("value").cast("string"),
                TRANSACTION_SCHEMA
            ).alias("data"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select("data.*", "kafka_timestamp")
        .withColumn(
            "event_time",
            F.to_timestamp(F.col("timestamp"))
        )
        .withWatermark("event_time", "30 seconds")
    )

    # 3. Apply stateless fraud rules via UDF
    with_stateless = parsed.withColumn(
        "fraud_signal",
        detect_fraud_udf(
            F.col("amount"),
            F.col("merchant_category"),
            F.col("is_international"),
            F.col("timestamp"),
        )
    ).withColumn("stateless_fraud",    F.col("fraud_signal.is_fraud")) \
     .withColumn("fraud_rule",         F.col("fraud_signal.rule_name")) \
     .withColumn("fraud_confidence",   F.col("fraud_signal.confidence")) \
     .withColumn("fraud_reason",       F.col("fraud_signal.reason")) \
     .drop("fraud_signal")

    # 4. Stateful velocity check: count txn per user in sliding 60-second window
    velocity = (
        parsed
        .groupBy(
            F.window(F.col("event_time"), f"{VELOCITY_WINDOW_SECONDS} seconds", "10 seconds"),
            F.col("user_id"),
        )
        .agg(F.count("*").alias("txn_count"))
        .filter(F.col("txn_count") > VELOCITY_MAX_TRANSACTIONS)
        .select(
            F.col("user_id").alias("velocity_user_id"),
            F.col("window.start").alias("velocity_window_start"),
            F.col("txn_count"),
        )
    )

    # 5. Join velocity flags back onto transactions
    enriched = (
        with_stateless
        .join(
            velocity,
            (with_stateless["user_id"] == velocity["velocity_user_id"])
            & (with_stateless["event_time"] >= velocity["velocity_window_start"])
            & (with_stateless["event_time"] < velocity["velocity_window_start"] + F.expr(f"INTERVAL {VELOCITY_WINDOW_SECONDS} SECONDS")),
            how="left",
        )
        .withColumn(
            "velocity_fraud",
            F.col("velocity_user_id").isNotNull()
        )
        .withColumn(
            "final_is_fraud",
            F.col("stateless_fraud") | F.col("velocity_fraud")
        )
        .withColumn(
            "final_rule",
            F.when(F.col("velocity_fraud"), F.lit("VELOCITY_BURST"))
             .when(F.col("stateless_fraud"), F.col("fraud_rule"))
             .otherwise(F.lit("CLEAN"))
        )
        .drop("velocity_user_id", "velocity_window_start")
    )

    # 6a. Write ALL transactions to transactions table
    all_txn_query = (
        enriched.select(
            "transaction_id", "user_id", "amount", "currency",
            "merchant_id", "merchant_name", "merchant_category", "merchant_country",
            "card_type", "card_last4", "event_time", "is_international",
            "final_is_fraud", "final_rule", "fraud_confidence",
        )
        .writeStream
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "transactions"))
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/transactions")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # 6b. Write ONLY fraud alerts to fraud_alerts table
    fraud_only = enriched.filter(F.col("final_is_fraud") == True)

    fraud_query = (
        fraud_only.select(
            "transaction_id", "user_id", "amount", "merchant_category",
            "merchant_country", "final_rule", "fraud_confidence", "fraud_reason",
            F.col("event_time").alias("alert_time"),
        )
        .writeStream
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "fraud_alerts"))
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/alerts")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # 6c. Console output for demo visibility
    console_query = (
        fraud_only.select(
            "transaction_id", "user_id", "amount",
            "merchant_category", "final_rule", "fraud_confidence",
        )
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="5 seconds")
        .start()
    )

    log.info("All streaming queries started. Awaiting termination …")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
