"""
PySpark StructType schema for incoming Kinesis transaction records.

The schema matches the JSON payload emitted by transaction_generator.py:
    {
        "transaction_id": "...",
        "user_id":        "...",
        "amount":         123.45,
        "merchant":       "...",
        "timestamp":      "2026-05-28T12:00:00.000000+00:00"
    }
"""

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        # Monetary amount in USD — stored as double; converted to Decimal for DynamoDB
        StructField("amount", DoubleType(), nullable=False),
        StructField("merchant", StringType(), nullable=True),
        # ISO-8601 string; cast to TimestampType inside the job for event-time windowing
        StructField("timestamp", StringType(), nullable=False),
    ]
)
