# Telecom Stream Analytics

## Overview
This solution processes monitoring and provisioning data streams from a telecommunication network to:
- **Task A**: Identify customers experiencing service downtimes at a given time
- **Task B**: Calculate mean duration of service disturbances for business customers up to a given time

## Prerequisites
- Python 3.7+
- Java 8+ (required by Spark)

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
spark-submit stream_processor.py <monitoring_csv> <provisioning_csv> <query_time>
```

### Example

```bash
spark-submit stream_processor.py monitoring_stream.csv provisioning_stream.csv '2021-04-14T08:09:52Z'
```

## Tasks

### Task A: Customers Experiencing Downtime
Identifies all customers whose service is currently DOWN at the given `query_time`, based on the latest monitoring event and active provisioning assignment.

### Task B: Mean Business Downtime Duration
Calculates the mean and maximum duration of all downtime periods for BUSINESS-segment customers up to `query_time`. The segment classification is determined at the start of each downtime period, ensuring historical accuracy.
