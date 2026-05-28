"""
Mock Point-of-Sale Transaction Producer
========================================
Continuously generates realistic fake financial transactions and streams them
to AWS Kinesis at a configurable rate.

Key design choices
------------------
* user_id is used as the Kinesis partition key → all events for the same user
  land on the same shard, preserving per-user ordering.
* ~5 % of records are intentionally anomalous to exercise fraud detection:
    - HIGH_AMOUNT  : single transaction > $1,000
    - LATE_DATA    : backdated 3-7 min (exercises watermark logic in Spark)
    - BURST        : 6-10 rapid transactions from the same user
      (exercises high-frequency detection)
* Kinesis throttling (ProvisionedThroughputExceededException) is handled with
  exponential back-off via tenacity.
"""

import json
import logging
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError
from faker import Faker
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# Allow imports from the project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import aws_config, producer_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

fake = Faker()
Faker.seed(42)

# ── Static lookup tables ──────────────────────────────────────────────────────

MERCHANT_CATEGORIES: List[str] = [
    "grocery",
    "electronics",
    "restaurant",
    "gas_station",
    "pharmacy",
    "clothing",
    "entertainment",
    "travel",
    "healthcare",
    "online_retail",
]

# Fixed pools keep the data interesting for aggregation (repeated user IDs
# mean windows will have multiple transactions per user).
USER_POOL: List[str] = [f"user_{i:04d}" for i in range(1, 101)]
DEVICE_POOL: List[str] = [f"device_{uuid.uuid4().hex[:8]}" for _ in range(50)]


# ── Transaction builders ──────────────────────────────────────────────────────

def _base_transaction(user_id: str) -> Dict[str, Any]:
    """Return a transaction skeleton shared by all builder functions."""
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "merchant_name": fake.company(),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "currency": "USD",
        "location": {
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": "US",
            "zip_code": fake.zipcode(),
        },
        "card_last_four": str(random.randint(1000, 9999)),
        "is_international": random.random() < 0.05,
        "device_id": random.choice(DEVICE_POOL),
    }


def generate_normal_transaction(user_id: str) -> Dict[str, Any]:
    """Typical consumer transaction — log-normal amount, current timestamp."""
    txn = _base_transaction(user_id)
    # Log-normal distribution: median ≈ $33, most values in $10–$200
    txn["amount"] = round(random.lognormvariate(3.5, 1.2), 2)
    txn["timestamp"] = datetime.now(timezone.utc).isoformat()
    return txn


def generate_high_amount_transaction(user_id: str) -> Dict[str, Any]:
    """Anomaly: single charge > $1,000 — triggers HIGH_SINGLE_TRANSACTION flag."""
    txn = _base_transaction(user_id)
    txn["amount"] = round(random.uniform(1_001.0, 5_000.0), 2)
    txn["is_international"] = random.random() < 0.40  # elevated international rate
    txn["timestamp"] = datetime.now(timezone.utc).isoformat()
    return txn


def generate_late_transaction(user_id: str) -> Dict[str, Any]:
    """
    Anomaly: normal transaction with a backdated timestamp (3–7 minutes ago).

    This simulates real-world network lag and exercises PySpark's watermark
    logic: the event will be assigned to an earlier window rather than the
    current one, and will be accepted as long as it arrives within the 5-minute
    watermark tolerance.
    """
    txn = generate_normal_transaction(user_id)
    delay_minutes = random.randint(3, 7)
    txn["timestamp"] = (
        datetime.now(timezone.utc) - timedelta(minutes=delay_minutes)
    ).isoformat()
    return txn


# ── Kinesis helper ────────────────────────────────────────────────────────────

@retry(
    retry=retry_if_exception_type(ClientError),
    wait=wait_exponential(multiplier=1, min=0.5, max=30),
    stop=stop_after_attempt(5),
    reraise=True,
)
def _put_record(
    client: Any,
    stream_name: str,
    transaction: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Put a single record to Kinesis.

    Uses user_id as the partition key so all events for the same user land
    on the same shard — this preserves per-user ordering and improves the
    efficiency of per-user windowed aggregation in Spark.

    Automatically retries on ProvisionedThroughputExceededException with
    exponential back-off (tenacity).
    """
    return client.put_record(
        StreamName=stream_name,
        Data=json.dumps(transaction).encode("utf-8"),
        PartitionKey=transaction["user_id"],
    )


# ── Main producer loop ────────────────────────────────────────────────────────

def run_producer(
    stream_name: str,
    region: str,
    transactions_per_second: int = 10,
    anomaly_rate: float = 0.05,
) -> None:
    """
    Continuously push transactions to Kinesis until interrupted.

    Anomaly breakdown (given default anomaly_rate=0.05):
        40 % of anomalies → HIGH_AMOUNT   (rand < 0.02)
        20 % of anomalies → LATE_DATA     (rand < 0.03)
        40 % of anomalies → BURST         (rand < 0.05)
    """
    kinesis = boto3.client("kinesis", region_name=region)
    sleep_interval = 1.0 / max(transactions_per_second, 1)

    logger.info(
        "Producer starting — stream=%s  rate=%d TPS  anomaly_rate=%.1f%%",
        stream_name,
        transactions_per_second,
        anomaly_rate * 100,
    )

    total_sent = 0
    total_anomalies = 0

    try:
        while True:
            user_id = random.choice(USER_POOL)
            rand = random.random()

            # ── Decide transaction type ───────────────────────────────────────
            if rand < anomaly_rate * 0.40:
                # HIGH_AMOUNT: single large transaction
                txn = generate_high_amount_transaction(user_id)
                response = _put_record(kinesis, stream_name, txn)
                total_sent += 1
                total_anomalies += 1
                logger.warning(
                    "[ANOMALY] HIGH_AMOUNT  user=%s  amount=$%.2f  shard=%s",
                    user_id,
                    txn["amount"],
                    response["ShardId"],
                )

            elif rand < anomaly_rate * 0.60:
                # LATE_DATA: backdated timestamp (exercises watermarking)
                txn = generate_late_transaction(user_id)
                response = _put_record(kinesis, stream_name, txn)
                total_sent += 1
                total_anomalies += 1
                logger.warning(
                    "[ANOMALY] LATE_DATA    user=%s  timestamp=%s",
                    user_id,
                    txn["timestamp"],
                )

            elif rand < anomaly_rate:
                # BURST: many transactions in quick succession (exercises
                # high-frequency detection window)
                burst_count = random.randint(6, 10)
                for _ in range(burst_count):
                    burst_txn = generate_normal_transaction(user_id)
                    burst_txn["amount"] = round(random.uniform(50.0, 300.0), 2)
                    _put_record(kinesis, stream_name, burst_txn)
                    total_sent += 1
                total_anomalies += 1
                logger.warning(
                    "[ANOMALY] BURST        user=%s  count=%d",
                    user_id,
                    burst_count,
                )

            else:
                # Normal transaction
                txn = generate_normal_transaction(user_id)
                _put_record(kinesis, stream_name, txn)
                total_sent += 1

            if total_sent % 100 == 0:
                logger.info(
                    "Sent %d transactions  (%d anomalies, %.1f%%)",
                    total_sent,
                    total_anomalies,
                    (total_anomalies / total_sent * 100) if total_sent else 0,
                )

            time.sleep(sleep_interval)

    except KeyboardInterrupt:
        logger.info(
            "Producer stopped by user.  total_sent=%d  total_anomalies=%d",
            total_sent,
            total_anomalies,
        )
    except Exception:
        logger.exception("Producer encountered a fatal error.")
        raise


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_producer(
        stream_name=aws_config.kinesis_stream_name,
        region=aws_config.region,
        transactions_per_second=producer_config.transactions_per_second,
        anomaly_rate=producer_config.anomaly_rate,
    )
