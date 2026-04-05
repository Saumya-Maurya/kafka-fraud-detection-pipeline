"""
Transaction schema definition.
Single source of truth for field names, types, and validation rules.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional
import json


@dataclass
class Transaction:
    transaction_id: str
    user_id: str
    amount: float
    currency: str
    merchant_id: str
    merchant_name: str
    merchant_category: str       # grocery, gas, online, atm, restaurant, travel
    merchant_country: str
    card_type: str               # credit, debit
    card_last4: str
    timestamp: str               # ISO 8601
    device_id: Optional[str]
    ip_address: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    is_international: bool
    is_fraud: bool = False       # ground truth label (for simulation only)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, json_str: str) -> "Transaction":
        data = json.loads(json_str)
        return cls(**data)

    def to_dict(self) -> dict:
        return asdict(self)


# JSON Schema (for Kafka Schema Registry or documentation)
TRANSACTION_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "transaction_id":    {"type": "string", "format": "uuid"},
        "user_id":           {"type": "string"},
        "amount":            {"type": "number", "minimum": 0},
        "currency":          {"type": "string", "minLength": 3, "maxLength": 3},
        "merchant_id":       {"type": "string"},
        "merchant_name":     {"type": "string"},
        "merchant_category": {"type": "string", "enum": ["grocery", "gas", "online", "atm", "restaurant", "travel", "retail", "pharmacy"]},
        "merchant_country":  {"type": "string"},
        "card_type":         {"type": "string", "enum": ["credit", "debit"]},
        "card_last4":        {"type": "string", "pattern": "^[0-9]{4}$"},
        "timestamp":         {"type": "string", "format": "date-time"},
        "device_id":         {"type": ["string", "null"]},
        "ip_address":        {"type": ["string", "null"]},
        "latitude":          {"type": ["number", "null"]},
        "longitude":         {"type": ["number", "null"]},
        "is_international":  {"type": "boolean"},
        "is_fraud":          {"type": "boolean"}
    },
    "required": [
        "transaction_id", "user_id", "amount", "currency",
        "merchant_id", "merchant_name", "merchant_category",
        "merchant_country", "card_type", "card_last4",
        "timestamp", "is_international"
    ]
}


# PySpark schema (used in consumer)
PYSPARK_SCHEMA = """
    transaction_id STRING,
    user_id STRING,
    amount DOUBLE,
    currency STRING,
    merchant_id STRING,
    merchant_name STRING,
    merchant_category STRING,
    merchant_country STRING,
    card_type STRING,
    card_last4 STRING,
    timestamp STRING,
    device_id STRING,
    ip_address STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    is_international BOOLEAN,
    is_fraud BOOLEAN
"""
