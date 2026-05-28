#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run_spark_job.sh — Submit the PySpark Structured Streaming anomaly detection job
#
# First run: Spark downloads the required JARs from Maven (~2 minutes).
# Subsequent runs use the local Ivy cache.
#
# Package versions — adjust to match your Spark installation:
#   SPARK_VERSION      3.5.x
#   SCALA_VERSION      2.12
#   HADOOP_VERSION     3.3.4  (verify: spark-shell --version | grep hadoop)
#
# Checkpoints are written to CHECKPOINT_LOCATION (from .env).
# To start fresh (discard all state), delete the checkpoint directory:
#     rm -rf /tmp/fraud-detection-checkpoint   # local
#     aws s3 rm s3://your-bucket/checkpoints --recursive  # S3
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# Load environment variables from .env if it exists
if [[ -f .env ]]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi

# ── Maven package coordinates ─────────────────────────────────────────────────
# Qubole's Kinesis connector for PySpark Structured Streaming
KINESIS_PKG="com.qubole.spark:spark-sql-kinesis_2.12:1.2.0_spark-3.0"

# Hadoop-AWS + AWS SDK — required for s3a:// filesystem support
HADOOP_AWS_PKG="org.apache.hadoop:hadoop-aws:3.3.4"
AWS_SDK_PKG="com.amazonaws:aws-java-sdk-bundle:1.12.261"

ALL_PACKAGES="${KINESIS_PKG},${HADOOP_AWS_PKG},${AWS_SDK_PKG}"

echo "Submitting PySpark fraud detection job..."
echo "  Packages: ${ALL_PACKAGES}"
echo "  Checkpoint: ${CHECKPOINT_LOCATION:-/tmp/fraud-detection-checkpoint}"
echo ""

spark-submit \
    --master "local[*]" \
    --packages "${ALL_PACKAGES}" \
    --conf "spark.sql.shuffle.partitions=4" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain" \
    --conf "spark.streaming.stopGracefullyOnShutdown=true" \
    spark_streaming/fraud_detection_job.py
