"""
transaction_generator.py — Mock Point-of-Sale System
=====================================================
Phase 1 of the Real-Time Transaction Anomaly Detection Engine.

What this script does
---------------------
1. Generates realistic fake credit card transactions using the Faker library.
2. Pushes each transaction as a JSON record to the AWS Kinesis stream
   `live-transactions` via boto3.
3. Sleeps 0.5 seconds between transactions (≈ 2 TPS).
4. ~5 % of the time it intentionally generates an anomalous transaction
   (amount > $2,000) so the downstream PySpark job has something to flag.

Required pip packages
---------------------
Install everything from the project's requirements.txt:

    pip install -r requirements.txt

Or install the minimum set for this script alone:

    pip install boto3 faker python-dotenv

AWS configuration
-----------------
Option A — Environment variables (recommended for CI/CD):
    export AWS_REGION=us-east-1
    export AWS_ACCESS_KEY_ID=AKIA...
    export AWS_SECRET_ACCESS_KEY=...

Option B — AWS CLI profile (easiest for local dev):
    aws configure
    # Follow the prompts; boto3 will pick up ~/.aws/credentials automatically.

Option C — .env file (used by this project):
    cp .env.example .env          # fill in your values
    python transaction_generator.py

IAM permissions needed
----------------------
The IAM user / role running this script needs:
    kinesis:PutRecord
    kinesis:DescribeStream   (optional — only used by the waiter in setup_aws.py)
on the `live-transactions` stream ARN.

Running
-------
    python transaction_generator.py

Stop with Ctrl-C.  The script prints every transaction to stdout so you
can verify the Kinesis writes before hooking up the PySpark consumer.
"""

import json
import logging
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError
from faker import Faker
from dotenv import load_dotenv

# Load .env if present (no-op if the file does not exist)
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

fake = Faker()
Faker.seed(0)

# ── Configuration (read from environment / .env) ──────────────────────────────

STREAM_NAME: str = os.getenv("KINESIS_STREAM_NAME", "live-transactions")
AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
INTERVAL: float = float(os.getenv("GENERATOR_INTERVAL_SECONDS", "0.5"))
ANOMALY_RATE: float = float(os.getenv("ANOMALY_RATE", "0.05"))
ANOMALY_THRESHOLD: float = float(os.getenv("ANOMALY_AMOUNT_THRESHOLD", "2000.0"))

# Fixed user pool — repeated IDs create patterns that the windowing logic detects
USER_POOL: list = [f"user_{i:03d}" for i in range(1, 51)]

# Common merchant names
MERCHANTS: list = [
    "Amazon", "Walmart", "Target", "Starbucks", "McDonald's",
    "Best Buy", "Costco", "Home Depot", "Uber", "Netflix",
    "Apple Store", "Nike", "Shell Gas", "Whole Foods", "CVS Pharmacy",
]


# ── Transaction builders ──────────────────────────────────────────────────────

def build_transaction(user_id: str, amount: float) -> Dict[str, Any]:
    """Return a transaction dict with the canonical five fields."""
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "amount": round(amount, 2),
        "merchant": random.choice(MERCHANTS),
        # ISO-8601 UTC timestamp — used as event_timestamp in PySpark windowing
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def generate_normal_transaction() -> Dict[str, Any]:
    """
    Generate a typical consumer transaction.

    Amount drawn from a log-normal distribution:
        median ≈ $33, 95th percentile ≈ $370
    This keeps normal transactions well below the $2,000 anomaly threshold.
    """
    user_id = random.choice(USER_POOL)
    amount = random.lognormvariate(3.5, 1.0)  # median ~$33
    return build_transaction(user_id, amount)


def generate_anomalous_transaction() -> Dict[str, Any]:
    """
    Generate an anomalous transaction with amount > $2,000.

    This intentionally exceeds the ANOMALY_AMOUNT_THRESHOLD so the PySpark
    job flags it in the hot path and writes it to DynamoDB.
    """
    user_id = random.choice(USER_POOL)
    amount = random.uniform(ANOMALY_THRESHOLD + 1.0, ANOMALY_THRESHOLD * 5.0)
    return build_transaction(user_id, amount)


# ── Kinesis writer ────────────────────────────────────────────────────────────

def put_to_kinesis(
    client: Any,
    stream_name: str,
    transaction: Dict[str, Any],
    max_retries: int = 3,
) -> str:
    """
    Write a single transaction record to Kinesis.

    Partition key = user_id: all events for the same user land on the same
    shard, preserving per-user ordering — important for accurate window counts.

    Retries up to max_retries times on ProvisionedThroughputExceededException
    with simple linear back-off (0.5 s * attempt number).

    Returns the ShardId where the record was placed.
    """
    data = json.dumps(transaction).encode("utf-8")
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            response = client.put_record(
                StreamName=stream_name,
                Data=data,
                PartitionKey=transaction["user_id"],
            )
            return response["ShardId"]
        except ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            if error_code == "ProvisionedThroughputExceededException":
                wait = 0.5 * attempt
                logger.warning(
                    "Kinesis throughput exceeded (attempt %d/%d). Retrying in %.1fs...",
                    attempt, max_retries, wait,
                )
                time.sleep(wait)
                last_error = exc
            else:
                # Non-retriable error (e.g. stream not found, auth failure)
                raise

    raise last_error  # All retries exhausted


# ── Main loop ─────────────────────────────────────────────────────────────────

def run() -> None:
    """
    Continuously generate and push transactions to Kinesis until interrupted.
    """
    kinesis = boto3.client("kinesis", region_name=AWS_REGION)

    logger.info("=" * 60)
    logger.info("Transaction Generator starting")
    logger.info("  Stream   : %s", STREAM_NAME)
    logger.info("  Region   : %s", AWS_REGION)
    logger.info("  Interval : %.1f s  (≈ %.0f TPS)", INTERVAL, 1.0 / INTERVAL)
    logger.info("  Anomaly  : %.0f%% of transactions", ANOMALY_RATE * 100)
    logger.info("  Threshold: $%.0f", ANOMALY_THRESHOLD)
    logger.info("=" * 60)

    total_sent = 0
    total_anomalies = 0

    try:
        while True:
            # Decide: normal or anomalous?
            is_anomaly = random.random() < ANOMALY_RATE
            txn = generate_anomalous_transaction() if is_anomaly else generate_normal_transaction()

            shard_id = put_to_kinesis(kinesis, STREAM_NAME, txn)

            total_sent += 1
            if is_anomaly:
                total_anomalies += 1

            # Human-readable log line for every transaction
            tag = "[ANOMALY]" if is_anomaly else "[NORMAL] "
            logger.info(
                "%s  id=%-36s  user=%-8s  amount=$%8.2f  merchant=%-15s  shard=%s",
                tag,
                txn["transaction_id"],
                txn["user_id"],
                txn["amount"],
                txn["merchant"],
                shard_id,
            )

            # Progress summary every 50 transactions
            if total_sent % 50 == 0:
                logger.info(
                    "── Progress: %d sent  |  %d anomalies (%.1f%%) ──",
                    total_sent,
                    total_anomalies,
                    total_anomalies / total_sent * 100,
                )

            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        logger.info("Generator stopped by user.")
        logger.info("Total sent: %d  |  Anomalies: %d", total_sent, total_anomalies)
        sys.exit(0)
    except ClientError as exc:
        logger.error(
            "AWS error: %s — check your credentials and that the stream '%s' exists.",
            exc.response["Error"]["Message"],
            STREAM_NAME,
        )
        logger.error("Run `python infrastructure/setup_aws.py` to create AWS resources.")
        sys.exit(1)


if __name__ == "__main__":
    run()
