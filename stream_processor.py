from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, to_timestamp, lag, max as spark_max,
    lit, unix_timestamp, row_number, avg, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType
import sys
import tempfile
import shutil
import os


# Explicit schemas are required for structured streaming (readStream cannot infer schema)
MONITORING_SCHEMA = StructType([
    StructField("time", StringType(), True),
    StructField("service_id", StringType(), True),
    StructField("status", StringType(), True),
])

PROVISIONING_SCHEMA = StructType([
    StructField("time", StringType(), True),
    StructField("service_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_segment", StringType(), True),
])


class TelecomStreamProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("TelecomStreamAnalytics") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.driver.memory", "4g") \
            .master("local[*]") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

    def ingest_streams(self, monitoring_dir, provisioning_dir):
        """
        Consume monitoring and provisioning CSV files from directories using
        Spark Structured Streaming (readStream).

        Both streams are written to temporary Parquet directories via the 'parquet' sink.
        trigger(once=True) processes all files currently in the directories and stops,
        making this suitable for batch-style CSV input while using the streaming API.
        The temp directory path is returned so the caller can clean it up after use.
        """
        tmp_dir = tempfile.mkdtemp(prefix="telecom_stream_")
        os.chmod(tmp_dir, 0o700)
        monitoring_out = os.path.join(tmp_dir, "monitoring_out")
        provisioning_out = os.path.join(tmp_dir, "provisioning_out")
        monitoring_ckpt = os.path.join(tmp_dir, "monitoring_ckpt")
        provisioning_ckpt = os.path.join(tmp_dir, "provisioning_ckpt")

        monitoring_stream = self.spark.readStream \
            .schema(MONITORING_SCHEMA) \
            .option("header", "true") \
            .csv(monitoring_dir)

        provisioning_stream = self.spark.readStream \
            .schema(PROVISIONING_SCHEMA) \
            .option("header", "true") \
            .csv(provisioning_dir)

        monitoring_query = monitoring_stream \
            .writeStream \
            .format("parquet") \
            .option("path", monitoring_out) \
            .option("checkpointLocation", monitoring_ckpt) \
            .trigger(once=True) \
            .start()

        provisioning_query = provisioning_stream \
            .writeStream \
            .format("parquet") \
            .option("path", provisioning_out) \
            .option("checkpointLocation", provisioning_ckpt) \
            .trigger(once=True) \
            .start()

        monitoring_query.awaitTermination()
        provisioning_query.awaitTermination()

        monitoring_df = self.spark.read.parquet(monitoring_out) \
            .withColumn("time", to_timestamp(col("time"))) \
            .select("time", "service_id", "status")

        # Normalize column name: production CSV uses "customer_segment", internal code uses "segment"
        provisioning_df = self.spark.read.parquet(provisioning_out) \
            .withColumn("time", to_timestamp(col("time"))) \
            .withColumnRenamed("customer_segment", "segment") \
            .select("time", "service_id", "customer_id", "segment")

        return monitoring_df, provisioning_df, tmp_dir

    def get_active_provisioning_at(self, provisioning_df, query_time):
        """
        For each service, get the most recent provisioning event at or before query_time.
        If that event is an unprovisioning (empty customer_id), the service has no customer.
        """
        window_spec = Window.partitionBy("service_id").orderBy(col("time").desc())

        latest_prov = provisioning_df \
            .filter(col("time") <= lit(query_time)) \
            .withColumn("rn", row_number().over(window_spec)) \
            .filter(col("rn") == 1) \
            .drop("rn")

        # Only keep services that are actively provisioned (non-empty customer_id)
        active_prov = latest_prov \
            .filter(col("customer_id").isNotNull() & (col("customer_id") != "")) \
            .select("service_id", "customer_id", "segment")

        return active_prov

    def get_latest_service_status(self, monitoring_df, query_time):
        """
        For each service, get the most recent status at or before query_time.
        """
        window_spec = Window.partitionBy("service_id").orderBy(col("time").desc())

        latest_status = monitoring_df \
            .filter(col("time") <= lit(query_time)) \
            .withColumn("rn", row_number().over(window_spec)) \
            .filter(col("rn") == 1) \
            .select("service_id", "status")

        return latest_status

    def get_customers_in_downtime(self, monitoring_df, provisioning_df, query_time):
        """
        Task A: Get list of customers experiencing downtime at a given time.

        A customer is in downtime if:
        1. Their service's latest monitoring status (at or before query_time) is DOWN
        2. The service is actively provisioned to a customer at query_time
        """
        # Get services currently DOWN
        latest_status = self.get_latest_service_status(monitoring_df, query_time)
        services_down = latest_status.filter(col("status") == "DOWN").select("service_id")

        # Get active customer assignments
        active_prov = self.get_active_provisioning_at(provisioning_df, query_time)

        # Join: customers whose service is DOWN
        customers_in_downtime = services_down \
            .join(active_prov, "service_id", "inner") \
            .select("customer_id", "service_id", "segment")

        return customers_in_downtime

    def get_downtime_periods(self, monitoring_df, query_time):
        """
        Identify downtime periods for each service up to query_time.

        A downtime period starts when status transitions to DOWN (or the first event is DOWN).
        A downtime period ends when status transitions to UP.
        If still DOWN at query_time, the period is open-ended (end = query_time).
        """
        # Filter events up to query_time.
        # Re-alias columns via toDF() to avoid Spark column lineage conflicts
        # when joining two DataFrames that are both derived from the same source plan.
        filtered = monitoring_df \
            .filter(col("time") <= lit(query_time)) \
            .toDF("time", "service_id", "status")

        window_spec = Window.partitionBy("service_id").orderBy("time")

        df = filtered \
            .withColumn("prev_status", lag("status").over(window_spec))

        # Downtime starts: first event is DOWN (prev_status is null) OR transition from UP to DOWN
        downtime_starts = df \
            .filter(
                (col("status") == "DOWN") &
                ((col("prev_status").isNull()) | (col("prev_status") == "UP"))
            ) \
            .select(col("service_id"), col("time").alias("downtime_start"))

        # Downtime ends: transition from DOWN to UP
        downtime_ends = df \
            .filter(
                (col("status") == "UP") &
                (col("prev_status") == "DOWN")
            ) \
            .select(col("service_id"), col("time").alias("downtime_end"))

        # Add row numbers to pair starts with ends properly
        start_window = Window.partitionBy("service_id").orderBy("downtime_start")
        end_window = Window.partitionBy("service_id").orderBy("downtime_end")

        starts_numbered = downtime_starts.withColumn(
            "period_id", row_number().over(start_window)
        )
        ends_numbered = downtime_ends.withColumn(
            "period_id", row_number().over(end_window)
        )

        # Left join so that open-ended downtimes (no matching end) are preserved
        downtime_periods = starts_numbered \
            .join(ends_numbered, ["service_id", "period_id"], "left") \
            .withColumn(
                "downtime_end",
                coalesce(col("downtime_end"), lit(query_time).cast("timestamp"))
            ) \
            .withColumn(
                "duration_seconds",
                unix_timestamp("downtime_end") - unix_timestamp("downtime_start")
            ) \
            .select("service_id", "downtime_start", "downtime_end", "duration_seconds")

        return downtime_periods

    def get_business_downtime_stats(self, monitoring_df, provisioning_df, query_time):
        """
        Task B: Calculate mean duration of disturbance for business customers up to query_time.

        Important: The segment is determined by the provisioning state at the time of each
        downtime period, not the latest segment.
        """
        downtime_periods = self.get_downtime_periods(monitoring_df, query_time)

        # For each downtime period, find the provisioning state at the downtime_start.
        prov_filtered = provisioning_df.filter(col("time") <= lit(query_time))

        # Join downtimes with provisioning where prov.time <= downtime_start
        dt_alias = downtime_periods.alias("dt")
        prov_alias = prov_filtered.alias("prov")

        joined = dt_alias.join(
            prov_alias,
            (col("dt.service_id") == col("prov.service_id")) &
            (col("prov.time") <= col("dt.downtime_start")),
            "inner"
        )

        window_spec = Window.partitionBy(
            col("dt.service_id"), col("dt.downtime_start")
        ).orderBy(col("prov.time").desc())

        prov_at_downtime = joined \
            .withColumn("rn", row_number().over(window_spec)) \
            .filter(col("rn") == 1) \
            .select(
                col("dt.service_id").alias("service_id"),
                col("dt.downtime_start").alias("downtime_start"),
                col("dt.downtime_end").alias("downtime_end"),
                col("dt.duration_seconds").alias("duration_seconds"),
                col("prov.customer_id").alias("customer_id"),
                col("prov.segment").alias("segment"
            )
        )

        # Filter for BUSINESS customers with active provisioning
        business_downtimes = prov_at_downtime \
            .filter(col("segment") == "BUSINESS") \
            .filter(col("customer_id").isNotNull() & (col("customer_id") != ""))

        # Calculate statistics
        stats = business_downtimes.agg(
            avg("duration_seconds").alias("mean_duration_seconds"),
            spark_max("duration_seconds").alias("max_duration_seconds")
        )

        return business_downtimes, stats

    def run(self, monitoring_dir, provisioning_dir, query_time_str):
        """Main execution"""

        print("\n" + "=" * 60)
        print("TELECOM STREAM ANALYTICS")
        print("=" * 60)

        # Consume data via Structured Streaming
        print(f"\nConsuming monitoring stream from: {monitoring_dir}")
        print(f"Consuming provisioning stream from: {provisioning_dir}")
        monitoring_df, provisioning_df, tmp_dir = self.ingest_streams(monitoring_dir, provisioning_dir)
        try:
            monitoring_df.cache()
            provisioning_df.cache()
            print(f"Loaded {monitoring_df.count()} monitoring events")
            print(f"Loaded {provisioning_df.count()} provisioning events")

            query_time = query_time_str
            print(f"\nQuery Time: {query_time}")
            print("=" * 60)

            # Task A: Customers in downtime
            print("\n### TASK A: CUSTOMERS EXPERIENCING DOWNTIME ###")
            customers_down = self.get_customers_in_downtime(
                monitoring_df, provisioning_df, query_time
            )

            customers_down_collected = customers_down.collect()
            if not customers_down_collected:
                print("\nNo customers experiencing downtime at this time.")
            else:
                print(f"\nCustomers in downtime: {len(customers_down_collected)}")
                customers_down.show(truncate=False)

            # Task B: Business downtime stats
            print("\n### TASK B: BUSINESS CUSTOMER DOWNTIME STATISTICS ###")
            business_downtimes, stats = self.get_business_downtime_stats(
                monitoring_df, provisioning_df, query_time
            )

            stats_collected = stats.collect()
            if stats_collected:
                mean_duration = stats_collected[0]["mean_duration_seconds"]
                max_duration = stats_collected[0]["max_duration_seconds"]

                if mean_duration is not None:
                    print(f"\nMean downtime duration for BUSINESS customers: {mean_duration:.2f} seconds")
                    print(f"Max downtime duration for BUSINESS customers: {max_duration:.2f} seconds")
                    print(f"\nDowntime periods (BUSINESS customers):")
                    business_downtimes.select(
                        "service_id", "customer_id", "downtime_start",
                        "downtime_end", "duration_seconds"
                    ).show(truncate=False)
                else:
                    print("\nNo business customer downtime data available.")
            else:
                print("No downtime data available.")

            print("\n" + "=" * 60)
        finally:
            try:
                shutil.rmtree(tmp_dir)
            except OSError as e:
                print(f"Warning: failed to remove temp directory {tmp_dir}: {e}")


def main():
    if len(sys.argv) < 4:
        print("\nUsage: spark-submit stream_processor.py <monitoring_dir> <provisioning_dir> <query_time>")
        print("\nExample:")
        print("  spark-submit stream_processor.py monitoring_data/ provisioning_data/ '2021-04-14T08:09:52Z'")
        print("\nNote: Run setup_dirs.sh first to create the input directories from the CSV files.")
        sys.exit(1)

    monitoring_dir = sys.argv[1]
    provisioning_dir = sys.argv[2]
    query_time = sys.argv[3]

    processor = TelecomStreamProcessor()
    processor.run(monitoring_dir, provisioning_dir, query_time)


if __name__ == "__main__":
    main()