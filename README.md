# Telecom Stream Analytics

## Overview
This solution processes monitoring and provisioning data streams from a telecommunication network using **Spark Structured Streaming** (`spark.readStream`) to:
- **Task A**: Identify customers experiencing service downtimes at a given time
- **Task B**: Calculate mean duration of service disturbances for business customers up to a given time

## Approach

### Streaming Architecture
- Data is ingested using Spark Structured Streaming (`readStream`)
- Streams are processed using micro-batch execution (`trigger(once=True)`)
- This allows deterministic processing of bounded CSV data while maintaining a streaming architecture
- Both streams are written to in-memory tables via the `memory` sink, then analysed using the standard DataFrame API

### Task A: Customers Experiencing Downtime
For a given query timestamp:
1. Retrieve the latest service status (≤ query_time) for each service
2. Retrieve the latest provisioning state (≤ query_time) for each service
3. Join both to identify customers whose service is currently DOWN and actively provisioned

Services that have been unprovisioned (empty `customer_id`) are excluded.

### Task B: Mean Business Downtime Duration
1. Identify downtime periods using status transitions:
   - `UP → DOWN` = downtime start
   - `DOWN → UP` = downtime end
   - If still DOWN at query_time, the period is open-ended (end = query_time)
2. Map each downtime period to the customer and segment that were active at the **start** of the downtime
3. Filter for `BUSINESS` segment customers only
4. Compute mean and max duration across all qualifying downtime periods

## Assumptions
- Input data is time-ordered within each stream
- No late-arriving or out-of-order events
- Each service is associated with at most one customer at any given time
- Empty `customer_id` and `segment` fields indicate unprovisioning of a service
- The segment classification for a downtime period is determined at the start of that period

## Limitations
- Uses micro-batch simulation (`trigger(once=True)`), not continuous streaming with stateful processing
- No watermarking or late-event handling
- Results are collected to the driver node (not scalable for very large datasets)
- Window functions like `lag()` and `row_number()` operate on bounded data after ingestion

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

## How to Run

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

## Running Tests

```bash
pytest test_stream_processor.py -v
```

## Data Format

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

