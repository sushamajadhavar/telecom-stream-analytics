#!/bin/bash
# setup_dirs.sh
# Creates input directories for Spark Structured Streaming and copies the CSV data files into them.
# Spark readStream monitors a directory for new files, so each stream needs its own directory.

set -e

MONITORING_DIR="monitoring_data"
PROVISIONING_DIR="provisioning_data"

mkdir -p "$MONITORING_DIR" "$PROVISIONING_DIR"

cp monitoring_stream.csv "$MONITORING_DIR/"
cp provisioning_stream.csv "$PROVISIONING_DIR/"

echo "Input directories created:"
echo "  $MONITORING_DIR/  <- monitoring_stream.csv"
echo "  $PROVISIONING_DIR/ <- provisioning_stream.csv"
echo ""
echo "You can now run:"
echo "  spark-submit stream_processor.py monitoring_data/ provisioning_data/ '2021-04-14T08:09:52Z'"
