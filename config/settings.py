"""
Centralised configuration for the Real-Time Transaction Anomaly Detection Engine.

All settings are loaded from environment variables so the same code runs
unchanged in local dev (Docker/macOS ARM64) and cloud deployments.
"""

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


@dataclass
class AWSConfig:
    """AWS service endpoints and resource names."""

    region: str = field(
        default_factory=lambda: os.getenv("AWS_REGION", "us-east-1")
    )

    # Kinesis — single shard is sufficient for this project's 2 TPS throughput
    kinesis_stream_name: str = field(
        default_factory=lambda: os.getenv("KINESIS_STREAM_NAME", "live-transactions")
    )
    kinesis_shard_count: int = field(
        default_factory=lambda: int(os.getenv("KINESIS_SHARD_COUNT", "1"))
    )

    # DynamoDB — single table; transaction_id is the partition key
    dynamodb_anomalies_table: str = field(
        default_factory=lambda: os.getenv("DYNAMODB_ANOMALIES_TABLE", "transaction_anomalies")
    )

    # S3 — cold path for Parquet files queried via Athena
    s3_bucket: str = field(
        default_factory=lambda: os.getenv("S3_BUCKET", "live-transactions-cold")
    )


@dataclass
class SparkConfig:
    """PySpark Structured Streaming tuning parameters."""

    app_name: str = "RealTimeTransactionAnomalyDetection"

    checkpoint_location: str = field(
        default_factory=lambda: os.getenv(
            "CHECKPOINT_LOCATION", "/tmp/live-transactions-checkpoint"
        )
    )

    # Watermark: accept late events arriving up to this long after their event time
    watermark_delay: str = "2 minutes"

    # Tumbling window: fixed 1-minute non-overlapping buckets
    tumbling_window_duration: str = "1 minute"

    # Micro-batch trigger intervals
    hot_path_trigger_interval: str = "10 seconds"
    cold_path_trigger_interval: str = "30 seconds"


@dataclass
class AnomalyConfig:
    """Threshold rules for anomaly detection."""

    # Flag if a single transaction exceeds this amount ($)
    amount_threshold: float = field(
        default_factory=lambda: float(os.getenv("ANOMALY_AMOUNT_THRESHOLD", "2000.0"))
    )
    # Flag if a user makes more than this many transactions in one 1-min window
    high_frequency_threshold: int = field(
        default_factory=lambda: int(os.getenv("HIGH_FREQUENCY_THRESHOLD", "3"))
    )


@dataclass
class GeneratorConfig:
    """Controls the mock transaction generator."""

    # Seconds to sleep between transactions (0.5 = 2 TPS)
    interval_seconds: float = field(
        default_factory=lambda: float(os.getenv("GENERATOR_INTERVAL_SECONDS", "0.5"))
    )
    # Fraction of generated transactions that are intentionally anomalous
    anomaly_rate: float = field(
        default_factory=lambda: float(os.getenv("ANOMALY_RATE", "0.05"))
    )


# ── Module-level singletons ───────────────────────────────────────────────────
aws_config = AWSConfig()
spark_config = SparkConfig()
anomaly_config = AnomalyConfig()
generator_config = GeneratorConfig()
