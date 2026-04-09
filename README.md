# Telecom Stream Analytics

## Overview
This solution processes monitoring and provisioning data streams from a telecommunication network using **Spark Structured Streaming** (`spark.readStream`) to:
- **Task A**: Identify customers experiencing service downtimes at a given time
- **Task B**: Calculate mean duration of service disturbances for business customers up to a given time

## Prerequisites
- Python 3.7+
- Java 8 or Java 11 (required by Spark)
- PySpark 3.3.0 (installed via `pip`)

## Installation

```bash
pip install -r requirements.txt
```

## Setup: Create Input Directories

Spark Structured Streaming reads from **directories** (not individual files). Run the provided helper script to create the input directories and copy the CSV data files into them:

```bash
bash setup_dirs.sh
```

This creates:
```
monitoring_data/   <- contains monitoring_stream.csv
provisioning_data/ <- contains provisioning_stream.csv
```

You can also add new CSV files to these directories at any time; the streaming reader will pick them up automatically.

## Usage

```bash
spark-submit stream_processor.py <monitoring_dir> <provisioning_dir> <query_time>
```

### Example

```bash
spark-submit stream_processor.py monitoring_data/ provisioning_data/ '2021-04-14T08:09:52Z'
```

### Alternative: run with `python` (if `spark-submit` is not on PATH)

```bash
python stream_processor.py monitoring_data/ provisioning_data/ '2021-04-14T08:09:52Z'
```

## How It Works

### Streaming Architecture

The program uses Spark Structured Streaming (`spark.readStream`) to consume CSV files from input directories:

1. **Stream ingestion** — Both monitoring and provisioning directories are read as streams using `readStream` with explicit `StructType` schemas (required for streaming).
2. **`trigger(once=True)`** — Processes all currently available files in the directory as a single micro-batch and then stops the query (no ongoing monitoring). This makes it practical for CSV-based input while still using the streaming API.
3. **Analysis** — Once ingestion completes, the in-memory data is analysed using the standard DataFrame API to answer Task A and Task B.

### Data Format

**Monitoring stream** (`monitoring_data/` directory):
| Column | Type | Description |
|--------|------|-------------|
| `time` | Timestamp | ISO-8601 event time (e.g. `2021-04-14T08:09:43Z`) |
| `service_id` | String | Unique service identifier |
| `status` | String | `UP` or `DOWN` |

**Provisioning stream** (`provisioning_data/` directory):
| Column | Type | Description |
|--------|------|-------------|
| `time` | Timestamp | ISO-8601 event time |
| `service_id` | String | Unique service identifier |
| `customer_id` | String | Customer ID (empty = unprovisioning event) |
| `segment` | String | `BUSINESS` or `RESIDENTIAL` (empty for unprovisioning) |

## Tasks

### Task A: Customers Experiencing Downtime
Identifies all customers whose service is currently `DOWN` at the given `query_time`, based on the latest monitoring event and active provisioning assignment at that time. Services that have been unprovisioned (latest provisioning event has an empty `customer_id`) are excluded.

### Task B: Mean Business Downtime Duration
Calculates the mean and maximum duration of all downtime periods for `BUSINESS`-segment customers up to `query_time`. The segment classification is determined at the **start** of each downtime period (not the latest segment), ensuring historical accuracy. Ongoing downtimes (service still `DOWN` at `query_time`) are included with an end time of `query_time`.

