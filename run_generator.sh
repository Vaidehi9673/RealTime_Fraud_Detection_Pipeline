#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run_generator.sh — Start the mock POS transaction generator
#
# Generates one transaction every 0.5 s (≈2 TPS), pushes to Kinesis.
# ~5% of transactions are intentionally anomalous (amount > $2,000).
#
# Stop with Ctrl-C.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

if [[ -f .env ]]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi

echo "Starting transaction generator..."
echo "  Stream   : ${KINESIS_STREAM_NAME:-live-transactions}"
echo "  Interval : ${GENERATOR_INTERVAL_SECONDS:-0.5}s"
echo "  Anomaly  : ${ANOMALY_RATE:-0.05} (~5%)"
echo ""

python transaction_generator.py
