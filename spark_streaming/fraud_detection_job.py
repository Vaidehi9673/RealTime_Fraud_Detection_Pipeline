"""
PySpark Structured Streaming — Real-Time Transaction Anomaly Detection
======================================================================
Phase 3 & 4 of the Real-Time Transaction Anomaly Detection Engine.

Architecture
------------
  Kinesis (live-transactions)
    → parse JSON  →  watermark  →  1-min tumbling window aggregation
                                          │
                   ┌──────────────────────┴──────────────────────┐
                   ▼  Hot Path                                    ▼  Cold Path
             DynamoDB (transaction_anomalies)               S3 Parquet
             flagged rows only                              all raw rows
             sub-ms reads by transaction_id                Athena SQL analytics

Three streaming engineering concepts
-------------------------------------
1. CHECKPOINTING  — On restart, Spark reads saved Kinesis shard offsets and
   window state from disk, resuming exactly where it left off.

2. TUMBLING WINDOWS — Transactions are bucketed into fixed, non-overlapping
   1-minute intervals on their event_timestamp (not processing time).

3. WATERMARKING — Spark accepts events arriving up to 2 minutes late.
   Windows stay open for (1 min + 2 min); after that, state is GC'd.

Submission
----------
See run_spark_job.sh for the full spark-submit command with --packages.
"""

import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    dayofmonth,
    from_json,
    max as _max,
    month,
    to_timestamp,
    window,
    year,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import anomaly_config, aws_config, spark_config
from spark_streaming.schemas import TRANSACTION_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# DynamoDB helper — Hot Path
# ─────────────────────────────────────────────────────────────────────────────

def _to_decimal(value: float) -> Decimal:
    """DynamoDB requires decimal.Decimal, not Python float."""
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def write_anomalies_to_dynamodb(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch sink — writes flagged anomalies to DynamoDB.

    Only rows where is_anomaly=True are written, keeping the table small and
    fast for the Streamlit dashboard's polling queries.

    boto3 clients are NOT serialisable — a new client is created per executor
    partition (the standard pattern for Spark→AWS distributed writes).
    """
    # Filter to anomalies only before sending to executors
    anomalies_df = batch_df.filter(col("is_anomaly"))
    if anomalies_df.rdd.isEmpty():
        logger.info("[Hot Path] Batch %d — no anomalies.", batch_id)
        return

    region = aws_config.region
    table_name = aws_config.dynamodb_anomalies_table
    amount_threshold = anomaly_config.amount_threshold
    frequency_threshold = anomaly_config.high_frequency_threshold

    def _write_partition(rows) -> None:
        ddb = boto3.resource("dynamodb", region_name=region)
        table = ddb.Table(table_name)
        now_utc = datetime.now(timezone.utc)
        # DynamoDB TTL — auto-delete anomaly records after 30 days
        ttl_value = int((now_utc + timedelta(days=30)).timestamp())
        detected_at = now_utc.isoformat()

        with table.batch_writer() as writer:
            for row in rows:
                window_start: str = row.window.start.isoformat()
                window_end: str = row.window.end.isoformat()
                user_id: str = row.user_id
                txn_count: int = int(row.transaction_count)
                max_amt: float = float(row.max_amount)

                # Build human-readable list of triggered rules
                flags = []
                if max_amt > amount_threshold:
                    flags.append("HIGH_AMOUNT")
                if txn_count > frequency_threshold:
                    flags.append("HIGH_FREQUENCY")

                # Partition key = user_id + window_start (idempotent upsert)
                writer.put_item(
                    Item={
                        "transaction_id": f"{user_id}#{window_start}",  # PK
                        "user_id": user_id,
                        "window_start": window_start,
                        "window_end": window_end,
                        "transaction_count": txn_count,
                        "max_amount": _to_decimal(max_amt),
                        "anomaly_flags": flags,
                        "is_anomaly": True,
                        "detected_at": detected_at,
                        "ttl": ttl_value,
                    }
                )

    anomalies_df.foreachPartition(_write_partition)
    anomaly_count = anomalies_df.count()
    logger.info(
        "[Hot Path] Batch %d — wrote %d anomaly window(s) to DynamoDB.",
        batch_id,
        anomaly_count,
    )


# ─────────────────────────────────────────────────────────────────────────────
# SparkSession
# ─────────────────────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(spark_config.app_name)
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# Stream builders
# ─────────────────────────────────────────────────────────────────────────────

def build_raw_stream(spark: SparkSession) -> DataFrame:
    """
    Connect to Kinesis and return a parsed streaming DataFrame.

    The Kinesis connector exposes each record's payload in the `data` column
    as BINARY.  We decode it to a STRING then parse the JSON fields against
    TRANSACTION_SCHEMA.  The producer's ISO-8601 `timestamp` string is cast
    to a proper TimestampType so Spark can use it for event-time windowing.
    """
    raw_kinesis = (
        spark.readStream
        .format("kinesis")
        .option("streamName", aws_config.kinesis_stream_name)
        .option("startingPosition", "LATEST")
        .option("region", aws_config.region)
        .load()
    )

    return (
        raw_kinesis
        .selectExpr("CAST(data AS STRING) AS json_str", "approximateArrivalTimestamp AS arrival_time")
        .select(
            from_json(col("json_str"), TRANSACTION_SCHEMA).alias("txn"),
            col("arrival_time"),
        )
        .select("txn.*", "arrival_time")
        # Cast the producer's ISO-8601 string to TimestampType for windowing
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .drop("timestamp")
    )


def build_windowed_anomalies(raw_stream: DataFrame) -> DataFrame:
    """
    Apply watermarking + 1-minute tumbling window + anomaly rules.

    ── WATERMARK ─────────────────────────────────────────────────────────────
    .withWatermark("event_timestamp", "2 minutes") tells Spark:
      "Any event more than 2 minutes late may be silently dropped."
    Spark holds each window's state open for (1 min + 2 min) after the
    window start, then seals it and frees the memory.

    ── TUMBLING WINDOW ───────────────────────────────────────────────────────
    window(col("event_timestamp"), "1 minute") creates non-overlapping
    1-minute buckets:  [12:00, 12:01), [12:01, 12:02), …
    Events are assigned to buckets by their event_timestamp, so a transaction
    that happened at 12:00:50 but arrived at 12:01:05 still lands in the
    [12:00, 12:01) window (within the 2-minute watermark tolerance).

    ── ANOMALY RULES ─────────────────────────────────────────────────────────
    Two threshold rules are evaluated per (user_id, 1-min window):
      • is_high_amount     — max single transaction in the window > $2,000
      • is_high_frequency  — more than 3 transactions in the window
    is_anomaly = OR of the two flags.
    """
    amount_threshold = anomaly_config.amount_threshold
    frequency_threshold = anomaly_config.high_frequency_threshold

    return (
        raw_stream
        # ── Step 1: Declare watermark on the event-time column ────────────────
        .withWatermark("event_timestamp", spark_config.watermark_delay)

        # ── Step 2: Group by 1-minute tumbling window + user ─────────────────
        .groupBy(
            window(col("event_timestamp"), spark_config.tumbling_window_duration),
            col("user_id"),
        )

        # ── Step 3: Aggregate per window ──────────────────────────────────────
        .agg(
            count("*").alias("transaction_count"),
            _max("amount").alias("max_amount"),
        )

        # ── Step 4: Apply anomaly rules ───────────────────────────────────────
        .withColumn("is_high_amount",    col("max_amount") > amount_threshold)
        .withColumn("is_high_frequency", col("transaction_count") > frequency_threshold)
        .withColumn("is_anomaly",        col("is_high_amount") | col("is_high_frequency"))
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline() -> None:
    """
    Wire both streaming queries together and block the driver.

    ── CHECKPOINTING ─────────────────────────────────────────────────────────
    Each .writeStream specifies a unique checkpointLocation.  Spark writes
    the Kinesis shard sequence numbers and open window state to this path
    after every micro-batch commit.

    On restart the job:
      1. Reads the saved offsets → resumes consuming from where it stopped.
      2. Reads the saved state   → continues accumulating into open windows.

    Each query MUST have its own checkpoint path — sharing one corrupts both.
    """
    spark = create_spark_session()
    logger.info("SparkSession created. Building pipeline...")

    raw_stream = build_raw_stream(spark)
    windowed_anomalies = build_windowed_anomalies(raw_stream)

    # ── Cold Path: all raw transactions → S3 Parquet ─────────────────────────
    # outputMode("append") — each micro-batch appends; no rewrites.
    # Partitioned by date for cheap Athena queries with partition pruning.
    cold_path_query = (
        raw_stream
        .withColumn("year",  year("event_timestamp"))
        .withColumn("month", month("event_timestamp"))
        .withColumn("day",   dayofmonth("event_timestamp"))
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", f"s3a://{aws_config.s3_bucket}/raw-transactions/")
        .option(
            "checkpointLocation",
            f"{spark_config.checkpoint_location}/s3-cold-path",  # ← checkpoint #1
        )
        .partitionBy("year", "month", "day")
        .trigger(processingTime=spark_config.cold_path_trigger_interval)
        .start()
    )
    logger.info("Cold path query started → s3a://%s/raw-transactions/", aws_config.s3_bucket)

    # ── Hot Path: anomalous windows → DynamoDB ────────────────────────────────
    # outputMode("update") — emit a row as soon as its window is updated.
    # After the watermark seals the window, DynamoDB holds the final values.
    hot_path_query = (
        windowed_anomalies
        .writeStream
        .foreachBatch(write_anomalies_to_dynamodb)
        .outputMode("update")
        .option(
            "checkpointLocation",
            f"{spark_config.checkpoint_location}/dynamodb-hot-path",  # ← checkpoint #2
        )
        .trigger(processingTime=spark_config.hot_path_trigger_interval)
        .start()
    )
    logger.info(
        "Hot path query started → DynamoDB table '%s'",
        aws_config.dynamodb_anomalies_table,
    )

    logger.info("Pipeline running. Checkpoint root: %s", spark_config.checkpoint_location)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_pipeline()


import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

import boto3

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    approx_count_distinct,
    col,
    count,
    dayofmonth,
    from_json,
    max as _max,
    min as _min,
    month,
    sum as _sum,
    to_timestamp,
    window,
    year,
)

# Allow imports from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import aws_config, fraud_config, spark_config
from spark_streaming.schemas import TRANSACTION_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# DynamoDB helpers (Hot Path)
# ─────────────────────────────────────────────────────────────────────────────

def _to_decimal(value: float) -> Decimal:
    """
    DynamoDB does not accept Python float — it requires decimal.Decimal.
    Quantise to 2 decimal places to avoid scientific-notation edge cases.
    """
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def write_to_hot_path(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch sink — called by Spark for every micro-batch of the
    windowed-aggregates stream.

    Each Spark executor partition writes directly to DynamoDB using a
    locally-created boto3 client (boto3 clients are not serialisable and must
    not be shared across processes/threads).

    Two tables are written per batch:
      • transaction-aggregates — every window row (for a live dashboard)
      • fraud-alerts           — only rows where is_fraud == True
    """
    if batch_df.rdd.isEmpty():
        return

    # Capture config values as plain Python scalars so they can be
    # safely serialised and shipped to executor processes.
    region = aws_config.region
    agg_table_name = aws_config.dynamodb_aggregates_table
    alerts_table_name = aws_config.dynamodb_fraud_alerts_table
    amount_threshold = fraud_config.amount_threshold
    frequency_threshold = fraud_config.high_frequency_threshold
    spend_threshold = fraud_config.total_spend_threshold

    def _write_partition(rows) -> None:
        """
        Runs once per DataFrame partition, inside a Spark executor.
        A fresh boto3 resource is created per partition — this is the
        standard pattern for distributed writes from Spark to AWS services.
        """
        ddb = boto3.resource("dynamodb", region_name=region)
        agg_table = ddb.Table(agg_table_name)
        alerts_table = ddb.Table(alerts_table_name)

        now_utc = datetime.now(timezone.utc)
        # DynamoDB TTL values (Unix epoch seconds)
        agg_ttl = int((now_utc + timedelta(days=7)).timestamp())
        alert_ttl = int((now_utc + timedelta(days=30)).timestamp())
        processed_at = now_utc.isoformat()

        with agg_table.batch_writer() as agg_writer, \
             alerts_table.batch_writer() as alert_writer:

            for row in rows:
                window_start: str = row.window.start.isoformat()
                window_end: str = row.window.end.isoformat()
                user_id: str = row.user_id

                # Derive a human-readable list of fraud reasons
                fraud_flags = []
                if row.max_amount > amount_threshold:
                    fraud_flags.append("HIGH_SINGLE_TRANSACTION")
                if row.transaction_count > frequency_threshold:
                    fraud_flags.append("HIGH_FREQUENCY")
                if row.total_amount > spend_threshold:
                    fraud_flags.append("HIGH_TOTAL_SPEND")

                # ── Write to aggregates table (every window) ──────────────────
                agg_writer.put_item(
                    Item={
                        "pk": user_id,            # Partition key
                        "sk": window_start,        # Sort key
                        "window_start": window_start,
                        "window_end": window_end,
                        "transaction_count": int(row.transaction_count),
                        "total_amount": _to_decimal(row.total_amount),
                        "max_amount": _to_decimal(row.max_amount),
                        "min_amount": _to_decimal(row.min_amount),
                        "distinct_categories": int(row.distinct_merchant_categories),
                        "is_fraud": bool(row.is_fraud),
                        "fraud_flags": fraud_flags,
                        "processed_at": processed_at,
                        "ttl": agg_ttl,            # Auto-expire after 7 days
                    }
                )

                # ── Write to fraud-alerts table (flagged windows only) ─────────
                if row.is_fraud:
                    alert_writer.put_item(
                        Item={
                            "alert_id": f"{user_id}#{window_start}",
                            "user_id": user_id,
                            "window_start": window_start,
                            "window_end": window_end,
                            "transaction_count": int(row.transaction_count),
                            "total_amount": _to_decimal(row.total_amount),
                            "max_amount": _to_decimal(row.max_amount),
                            "fraud_flags": fraud_flags,
                            # HIGH when multiple rules fire simultaneously
                            "severity": "HIGH" if len(fraud_flags) > 1 else "MEDIUM",
                            "status": "NEW",
                            "created_at": processed_at,
                            "ttl": alert_ttl,      # Auto-expire after 30 days
                        }
                    )

    batch_df.foreachPartition(_write_partition)
    logger.info("[Hot Path] Batch %d written to DynamoDB.", batch_id)


# ─────────────────────────────────────────────────────────────────────────────
# SparkSession
# ─────────────────────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Build a SparkSession configured for Kinesis reading and S3A writing.

    The Kinesis connector JAR (com.qubole.spark:spark-sql-kinesis) and the
    Hadoop-AWS / AWS-SDK JARs must be provided via --packages when calling
    spark-submit (see run_spark_job.sh).
    """
    spark = (
        SparkSession.builder
        .appName(spark_config.app_name)
        # ── S3A filesystem (required for s3a:// paths) ────────────────────────
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        # ── Streaming / shuffle tuning ────────────────────────────────────────
        # Keep partition count low for local dev; increase for cluster deploys.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        # ── Kinesis consumer back-off ─────────────────────────────────────────
        .config("spark.kinesis.client.describeStreamBackoffTime", "1000")
        .config("spark.kinesis.client.describeStreamMaxRetries", "5")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# Stream builders
# ─────────────────────────────────────────────────────────────────────────────

def build_raw_stream(spark: SparkSession) -> DataFrame:
    """
    Connect to Kinesis and return a parsed streaming DataFrame.

    Kinesis record layout returned by the connector:
        data                      BINARY  ← our JSON payload
        partitionKey              STRING
        sequenceNumber            STRING
        approximateArrivalTimestamp TIMESTAMP
        streamName                STRING
        shardId                   STRING

    We decode `data` to a string, parse it against TRANSACTION_SCHEMA,
    then cast the ISO-8601 `timestamp` string to a proper TimestampType so
    PySpark can use it for event-time windowing.
    """
    raw_kinesis = (
        spark.readStream
        .format("kinesis")
        .option("streamName", aws_config.kinesis_stream_name)
        .option("startingPosition", "LATEST")
        .option("region", aws_config.region)
        .option("kinesis.client.numRetries", "5")
        .load()
    )

    parsed = (
        raw_kinesis
        # BINARY → STRING → parse JSON fields
        .selectExpr("CAST(data AS STRING) AS json_str", "approximateArrivalTimestamp AS arrival_time")
        .select(
            from_json(col("json_str"), TRANSACTION_SCHEMA).alias("txn"),
            col("arrival_time"),
        )
        .select("txn.*", "arrival_time")
        # Convert the ISO-8601 string from the producer to a proper timestamp.
        # to_timestamp() without a format uses Spark's built-in ISO-8601 parser.
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .drop("timestamp")   # Remove the now-redundant raw string column
    )

    return parsed


def build_windowed_aggregates(raw_stream: DataFrame) -> DataFrame:
    """
    Apply watermarking + tumbling-window aggregation + fraud-rule scoring.

    ── WATERMARK ────────────────────────────────────────────────────────────────
    .withWatermark("event_timestamp", "5 minutes") tells Spark:
      "The maximum out-of-order lateness of any event is 5 minutes."
    Spark will keep each window's state alive for (window_duration + 5 min)
    after the window closes.  Any event arriving later than the watermark
    threshold is silently dropped.  This bounds the in-memory state size
    to O(windows_in_flight) rather than O(all_time).

    ── TUMBLING WINDOW ──────────────────────────────────────────────────────────
    window(col("event_timestamp"), "1 minute") partitions the event-time axis
    into non-overlapping 1-minute buckets:
        [12:00, 12:01)  [12:01, 12:02)  [12:02, 12:03) …
    Every event falls into exactly one bucket based on its event_timestamp,
    NOT its processing time.  Two events with the same user_id that occurred
    in the same minute are grouped together regardless of when they arrived.

    ── FRAUD RULES ──────────────────────────────────────────────────────────────
    Three boolean flags are computed from the window aggregates:
        is_high_amount    — max single transaction in the window > $1,000
        is_high_frequency — more than 5 transactions in the window
        is_high_total_spend — total spend in the window > $3,000
    is_fraud = OR of all three flags.
    """
    return (
        raw_stream
        # ── Step 1: Declare event-time column and watermark ───────────────────
        .withWatermark("event_timestamp", spark_config.watermark_delay)

        # ── Step 2: Tumbling-window group-by ─────────────────────────────────
        .groupBy(
            window(col("event_timestamp"), spark_config.tumbling_window_duration),
            col("user_id"),
        )

        # ── Step 3: Per-window aggregations ──────────────────────────────────
        .agg(
            count("*").alias("transaction_count"),
            _sum("amount").alias("total_amount"),
            _max("amount").alias("max_amount"),
            _min("amount").alias("min_amount"),
            approx_count_distinct("merchant_category").alias("distinct_merchant_categories"),
        )

        # ── Step 4: Fraud scoring ─────────────────────────────────────────────
        .withColumn(
            "is_high_amount",
            col("max_amount") > fraud_config.amount_threshold,
        )
        .withColumn(
            "is_high_frequency",
            col("transaction_count") > fraud_config.high_frequency_threshold,
        )
        .withColumn(
            "is_high_total_spend",
            col("total_amount") > fraud_config.total_spend_threshold,
        )
        .withColumn(
            "is_fraud",
            col("is_high_amount") | col("is_high_frequency") | col("is_high_total_spend"),
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline() -> None:
    """
    Wire together both streaming queries and keep the driver alive.

    ── CHECKPOINTING ────────────────────────────────────────────────────────────
    Every .writeStream call specifies a unique checkpointLocation.
    Spark persists two things to this directory on every micro-batch commit:
      1. The Kinesis shard offsets (sequence numbers) — so on restart, Spark
         knows exactly which records have already been processed.
      2. The intermediate aggregation state (open windows, partial sums, etc.)
         — so in-flight windows are not lost across restarts.

    Each query MUST have its own checkpoint path — sharing a checkpoint between
    two queries corrupts the state of both.

    To observe checkpointing in action:
      1. Let the job run for ~30 seconds so a few micro-batches commit.
      2. Kill the job (Ctrl-C).
      3. Inspect $CHECKPOINT_LOCATION/dynamodb-hot-path/offsets/ — you will
         see JSON files containing the last committed Kinesis sequence numbers.
      4. Restart the job — it resumes from those offsets, not from LATEST.
    """
    spark = create_spark_session()
    logger.info("SparkSession created. Building pipeline...")

    raw_stream = build_raw_stream(spark)
    windowed_agg = build_windowed_aggregates(raw_stream)

    # ── Cold Path: raw transactions → S3 (Parquet, date-partitioned) ─────────
    # outputMode("append") — each micro-batch appends new rows; existing
    # Parquet files are never rewritten.  Athena can query this with partition
    # pruning (WHERE year=2026 AND month=5) for cheap, fast ad-hoc analytics.
    cold_path_query = (
        raw_stream
        .withColumn("year",  year("event_timestamp"))
        .withColumn("month", month("event_timestamp"))
        .withColumn("day",   dayofmonth("event_timestamp"))
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", f"s3a://{aws_config.s3_bucket}/raw-transactions/")
        .option(
            "checkpointLocation",
            f"{spark_config.checkpoint_location}/s3-raw-transactions",  # ← checkpoint #1
        )
        .partitionBy("year", "month", "day")
        .trigger(processingTime=spark_config.cold_path_trigger_interval)
        .start()
    )
    logger.info("Cold path query started → S3 Parquet sink.")

    # ── Hot Path: windowed aggregates + fraud alerts → DynamoDB ──────────────
    # outputMode("update") — emit a row as soon as its window is updated,
    # even before the window is fully closed.  Combined with watermarking,
    # final results are emitted once the watermark passes the window end,
    # then the state for that window is cleaned up.
    hot_path_query = (
        windowed_agg
        .writeStream
        .foreachBatch(write_to_hot_path)
        .outputMode("update")
        .option(
            "checkpointLocation",
            f"{spark_config.checkpoint_location}/dynamodb-hot-path",  # ← checkpoint #2
        )
        .trigger(processingTime=spark_config.hot_path_trigger_interval)
        .start()
    )
    logger.info("Hot path query started → DynamoDB foreachBatch sink.")

    logger.info(
        "Pipeline running. Checkpoint root: %s", spark_config.checkpoint_location
    )

    # Block the driver until one of the queries terminates (or an exception is
    # raised).  In production, wrap this in a supervisory process that restarts
    # on failure and relies on checkpointing to resume safely.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_pipeline()
