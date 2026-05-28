"""
AWS Infrastructure Setup — Real-Time Transaction Anomaly Detection Engine
=========================================================================
Phase 2: One-time script that provisions all required AWS resources.
Safe to re-run — every operation checks for existing resources first.

Resources created
-----------------
1. Kinesis Data Stream (live-transactions, 1 shard)
2. DynamoDB table: transaction_anomalies
      Partition key: transaction_id (String)
      Billing: On-Demand (PAY_PER_REQUEST) — no capacity planning needed
      TTL on attribute: ttl — auto-deletes records after 30 days
3. S3 bucket — cold-path raw transactions (Parquet) + Athena queries
      Versioning enabled, all public access blocked,
      Glacier lifecycle after 90 days
4. athena_setup.sql — generated DDL file; run manually in Athena Query Editor

Usage
-----
    python infrastructure/setup_aws.py
"""

import logging
import os
import sys

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import aws_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Kinesis
# ─────────────────────────────────────────────────────────────────────────────

def create_kinesis_stream(client, stream_name: str, shard_count: int) -> None:
    """Create Kinesis stream if it does not already exist, then wait for ACTIVE."""
    try:
        client.describe_stream_summary(StreamName=stream_name)
        logger.info("Kinesis stream '%s' already exists — skipping.", stream_name)
        return
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    logger.info(
        "Creating Kinesis stream '%s' with %d shard(s)...", stream_name, shard_count
    )
    client.create_stream(StreamName=stream_name, ShardCount=shard_count)
    waiter = client.get_waiter("stream_exists")
    waiter.wait(StreamName=stream_name)
    logger.info("Kinesis stream '%s' is ACTIVE.", stream_name)


# ─────────────────────────────────────────────────────────────────────────────
# DynamoDB
# ─────────────────────────────────────────────────────────────────────────────

def create_dynamodb_table(
    client,
    table_name: str,
    partition_key: str,
    sort_key: str = None,
    ttl_attribute: str = "ttl",
) -> None:
    """Create a DynamoDB table (on-demand billing) with optional sort key and TTL."""
    try:
        client.describe_table(TableName=table_name)
        logger.info("DynamoDB table '%s' already exists — skipping.", table_name)
        return
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    key_schema = [{"AttributeName": partition_key, "KeyType": "HASH"}]
    attr_defs = [{"AttributeName": partition_key, "AttributeType": "S"}]

    if sort_key:
        key_schema.append({"AttributeName": sort_key, "KeyType": "RANGE"})
        attr_defs.append({"AttributeName": sort_key, "AttributeType": "S"})

    logger.info("Creating DynamoDB table '%s'...", table_name)
    client.create_table(
        TableName=table_name,
        KeySchema=key_schema,
        AttributeDefinitions=attr_defs,
        # PAY_PER_REQUEST: no capacity planning required; scales automatically.
        BillingMode="PAY_PER_REQUEST",
    )

    waiter = client.get_waiter("table_exists")
    waiter.wait(TableName=table_name)

    # TTL lets DynamoDB automatically delete stale records, keeping storage
    # costs bounded without any application-level cleanup jobs.
    client.update_time_to_live(
        TableName=table_name,
        TimeToLiveSpecification={"Enabled": True, "AttributeName": ttl_attribute},
    )
    logger.info(
        "DynamoDB table '%s' created with TTL on '%s'.", table_name, ttl_attribute
    )


# ─────────────────────────────────────────────────────────────────────────────
# S3
# ─────────────────────────────────────────────────────────────────────────────

def create_s3_bucket(client, bucket_name: str, region: str) -> None:
    """Create an S3 bucket with versioning, public-access block, and lifecycle rules."""
    try:
        client.head_bucket(Bucket=bucket_name)
        logger.info("S3 bucket '%s' already exists — skipping.", bucket_name)
        return
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in ("404", "NoSuchBucket"):
            raise

    logger.info("Creating S3 bucket '%s' in region '%s'...", bucket_name, region)
    create_kwargs: dict = {"Bucket": bucket_name}
    # us-east-1 does not accept a LocationConstraint — it is the implicit default.
    if region != "us-east-1":
        create_kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
    client.create_bucket(**create_kwargs)

    # Versioning — protects against accidental overwrites / deletes
    client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"},
    )

    # Block all public access — this data is sensitive financial information
    client.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )

    # Lifecycle rule: transition raw transactions to Glacier after 90 days
    # to cut storage costs while keeping data available for audits.
    client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": "ArchiveRawTransactions",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "raw-transactions/"},
                    "Transitions": [{"Days": 90, "StorageClass": "GLACIER"}],
                }
            ]
        },
    )

    logger.info(
        "S3 bucket '%s' created (versioning ON, public access BLOCKED, "
        "Glacier lifecycle after 90 days).",
        bucket_name,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Athena DDL (generated file, not executed here)
# ─────────────────────────────────────────────────────────────────────────────

def write_athena_setup_sql(bucket_name: str) -> None:
    """
    Write athena_setup.sql to the project root.
    Run this SQL in the Athena Query Editor after cold-path data lands in S3.
    """
    sql = f"""\
-- =============================================================================
-- Athena Setup — Real-Time Transaction Anomaly Detection Engine
-- Run each statement individually in the AWS Athena Query Editor.
-- =============================================================================

-- 1. Create a dedicated database
CREATE DATABASE IF NOT EXISTS transaction_analytics;

-- 2. External table over the S3 cold-path Parquet data written by PySpark.
--    Partitioned by date for cheap, fast queries with partition pruning.
CREATE EXTERNAL TABLE IF NOT EXISTS transaction_analytics.transactions (
    transaction_id   STRING,
    user_id          STRING,
    amount           DOUBLE,
    merchant         STRING,
    event_timestamp  TIMESTAMP,
    arrival_time     TIMESTAMP
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION 's3://{bucket_name}/raw-transactions/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- 3. Sync partition metadata (re-run whenever new date partitions appear)
MSCK REPAIR TABLE transaction_analytics.transactions;

-- =============================================================================
-- Example analytical queries
-- =============================================================================

-- All transactions above the $2,000 anomaly threshold this month
SELECT
    transaction_id,
    user_id,
    amount,
    merchant,
    event_timestamp
FROM transaction_analytics.transactions
WHERE amount > 2000.0
  AND year  = YEAR(CURRENT_DATE)
  AND month = MONTH(CURRENT_DATE)
ORDER BY amount DESC
LIMIT 50;

-- Top 10 users by transaction count this month
SELECT
    user_id,
    COUNT(*)    AS txn_count,
    SUM(amount) AS total_spend,
    MAX(amount) AS max_single_txn
FROM transaction_analytics.transactions
WHERE year  = YEAR(CURRENT_DATE)
  AND month = MONTH(CURRENT_DATE)
GROUP BY user_id
ORDER BY txn_count DESC
LIMIT 10;

-- Hourly transaction volume — spot off-hours anomaly spikes
SELECT
    DATE_TRUNC('hour', event_timestamp) AS hour_bucket,
    COUNT(*)                            AS txn_count,
    SUM(amount)                         AS total_amount
FROM transaction_analytics.transactions
WHERE year  = YEAR(CURRENT_DATE)
  AND month = MONTH(CURRENT_DATE)
GROUP BY 1
ORDER BY 1;

-- Top merchants by transaction volume
SELECT
    merchant,
    COUNT(*)                                              AS txn_count,
    ROUND(AVG(amount), 2)                                 AS avg_amount,
    MAX(amount)                                           AS max_amount
FROM transaction_analytics.transactions
WHERE year = YEAR(CURRENT_DATE)
GROUP BY merchant
ORDER BY txn_count DESC
LIMIT 20;
"""

    output_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "athena_setup.sql",
    )
    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write(sql)
    logger.info("Athena DDL written to %s", output_path)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("Starting AWS infrastructure setup (region=%s)...", aws_config.region)

    kinesis = boto3.client("kinesis", region_name=aws_config.region)
    dynamodb = boto3.client("dynamodb", region_name=aws_config.region)
    s3 = boto3.client("s3", region_name=aws_config.region)

    # 1. Kinesis stream (1 shard — sufficient for this project's 2 TPS)
    create_kinesis_stream(
        kinesis,
        aws_config.kinesis_stream_name,
        aws_config.kinesis_shard_count,
    )

    # 2. DynamoDB: transaction_anomalies  (PK = transaction_id)
    create_dynamodb_table(
        dynamodb,
        table_name=aws_config.dynamodb_anomalies_table,
        partition_key="transaction_id",
    )

    # 3. S3 bucket (cold path)
    create_s3_bucket(s3, aws_config.s3_bucket, aws_config.region)

    # 4. Athena DDL file
    write_athena_setup_sql(aws_config.s3_bucket)

    logger.info("Infrastructure setup complete.")
    logger.info("  Kinesis stream  : %s", aws_config.kinesis_stream_name)
    logger.info("  DynamoDB table  : %s", aws_config.dynamodb_anomalies_table)
    logger.info("  S3 bucket       : %s", aws_config.s3_bucket)
    logger.info("  Athena SQL      : athena_setup.sql")
    logger.info("")
    logger.info("Next steps:")
    logger.info("  1. python transaction_generator.py    (Terminal 1)")
    logger.info("  2. bash run_spark_job.sh              (Terminal 2)")
    logger.info("  3. streamlit run app.py               (Terminal 3)")


if __name__ == "__main__":
    main()
