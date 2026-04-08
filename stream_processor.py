from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, to_timestamp, when, lag, max as spark_max,
    collect_list, struct, lit, unix_timestamp,
    coalesce, last, row_number, avg
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime
import sys

class TelecomStreamProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("TelecomStreamAnalytics") \
            .config("spark.sql.shuffle.partitions", "1") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
    
    def load_monitoring_stream(self, file_path):
        """Load monitoring data stream"""
        df = self.spark.read \
            .option("header", "true") \
            .csv(file_path)
        
        df = df.withColumn("time", to_timestamp(col("time")))
        return df.select("time", "service_id", "status")
    
    def load_provisioning_stream(self, file_path):
        """Load provisioning data stream"""
        df = self.spark.read \
            .option("header", "true") \
            .csv(file_path)
        
        df = df.withColumn("time", to_timestamp(col("time")))
        
        # Mark unprovisioning events (empty customer_id and segment)
        df = df.withColumn(
            "is_unprovisioning",
            when((col("customer_id").isNull()) | (col("customer_id") == ""), True).otherwise(False)
        )
        
        return df.select("time", "service_id", "customer_id", "segment", "is_unprovisioning")
    
    def get_customers_in_downtime(self, monitoring_df, provisioning_df, query_time):
        """
        Task A: Get list of customers experiencing downtime at given time
        """
        
        # Get latest provisioning state for each service before query_time
        window_spec = Window.partitionBy("service_id").orderBy(col("time").desc())
        
        # Get last known customer for each service (handling unprovisioning)
        last_customer = provisioning_df \
            .filter(col("time") <= query_time) \
            .withColumn(
                "last_customer",
                last(
                    when(~col("is_unprovisioning"), struct(col("customer_id"), col("segment")))
                ).over(window_spec)
            ) \
            .select("service_id", col("last_customer.customer_id").alias("customer_id"), 
                   col("last_customer.segment").alias("segment")) \
            .distinct()
        
        # Get services that are DOWN at query_time
        services_down = monitoring_df \
            .filter(col("time") <= query_time) \
            .withColumn("rank", row_number().over(Window.partitionBy("service_id").orderBy(col("time").desc()))) \
            .filter(col("rank") == 1) \
            .filter(col("status") == "DOWN") \
            .select("service_id")
        
        # Join to get customers in downtime
        customers_in_downtime = services_down \
            .join(last_customer, "service_id", "inner") \
            .select("service_id", "customer_id", "segment") \
            .filter(col("customer_id").isNotNull())
        
        return customers_in_downtime
    
    def get_downtime_periods(self, monitoring_df):
        """
        Identify downtime periods (from DOWN to UP transition)
        """
        
        # Add row number to track state changes
        window_spec = Window.partitionBy("service_id").orderBy("time")
        
        df_with_lag = monitoring_df \
            .withColumn("prev_status", lag("status").over(window_spec)) \
            .withColumn("status_changed", 
                       when(col("prev_status") != col("status"), True).otherwise(False))
        
        # Identify downtime start (UP to DOWN transition)
        downtime_starts = df_with_lag \
            .filter((col("prev_status") == "UP") & (col("status") == "DOWN")) \
            .select(
                col("service_id"),
                col("time").alias("downtime_start"),
                row_number().over(Window.partitionBy("service_id").orderBy("time")).alias("downtime_id")
            )
        
        # Identify downtime end (DOWN to UP transition)
        downtime_ends = df_with_lag \
            .filter((col("prev_status") == "DOWN") & (col("status") == "UP")) \
            .select(
                col("service_id"),
                col("time").alias("downtime_end"),
                row_number().over(Window.partitionBy("service_id").orderBy("time")).alias("downtime_id")
            )
        
        # Join starts and ends
        downtime_periods = downtime_starts \
            .join(downtime_ends, ["service_id", "downtime_id"], "inner") \
            .withColumn(
                "duration_seconds",
                (unix_timestamp("downtime_end") - unix_timestamp("downtime_start"))
            )
        
        return downtime_periods
    
    def get_business_downtime_stats(self, monitoring_df, provisioning_df, query_time):
        """
        Task B: Calculate mean duration of disturbance for business customers
        """
        
        # Get downtime periods
        downtime_periods = self.get_downtime_periods(monitoring_df)
        
        # Get provisioning info for each service
        window_spec_service = Window.partitionBy("service_id").orderBy(col("time").desc())
        
        service_segments = provisioning_df \
            .filter(col("time") <= query_time) \
            .withColumn(
                "last_assignment",
                last(
                    when(~col("is_unprovisioning"), struct(col("customer_id"), col("segment")))
                ).over(window_spec_service)
            ) \
            .select(
                col("service_id"),
                col("last_assignment.segment").alias("segment")
            ) \
            .distinct()
        
        # Filter for business customers only
        business_downtimes = downtime_periods \
            .join(service_segments, "service_id", "inner") \
            .filter(col("segment") == "BUSINESS") \
            .filter(col("downtime_end") <= query_time)
        
        # Calculate mean duration
        stats = business_downtimes \
            .agg(
                avg("duration_seconds").alias("mean_duration_seconds"),
                spark_max("duration_seconds").alias("max_duration_seconds")
            )
        
        return business_downtimes, stats
    
    def run(self, monitoring_path, provisioning_path, query_time_str):
        """Main execution"""
        
        print("\n" + "="*60)
        print("TELECOM STREAM ANALYTICS")
        print("="*60)
        
        # Load data
        print("\nLoading monitoring stream...")
        monitoring_df = self.load_monitoring_stream(monitoring_path)
        print(f"Loaded {monitoring_df.count()} monitoring events")
        
        print("Loading provisioning stream...")
        provisioning_df = self.load_provisioning_stream(provisioning_path)
        print(f"Loaded {provisioning_df.count()} provisioning events")
        
        # Parse query time
        query_time = to_timestamp(lit(query_time_str)).cast(TimestampType())
        
        print(f"\nQuery Time: {query_time_str}")
        print("="*60)
        
        # Task A: Customers in downtime
        print("\n### TASK A: CUSTOMERS EXPERIENCING DOWNTIME ###")
        customers_down = self.get_customers_in_downtime(monitoring_df, provisioning_df, query_time)
        
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
                    "service_id", "downtime_start", "downtime_end", "duration_seconds"
                ).show(truncate=False)
            else:
                print("\nNo business customer downtime data available.")
        else:
            print("No downtime data available.")
        
        print("\n" + "="*60)

def main():
    if len(sys.argv) < 4:
        print("\nUsage: python stream_processor.py <monitoring_csv> <provisioning_csv> <query_time>")
        print("\nExample:")
        print("  python stream_processor.py monitoring_stream.csv provisioning_stream.csv '2021-04-14T08:09:52Z'")
        sys.exit(1)
    
    monitoring_path = sys.argv[1]
    provisioning_path = sys.argv[2]
    query_time = sys.argv[3]
    
    processor = TelecomStreamProcessor()
    processor.run(monitoring_path, provisioning_path, query_time)

if __name__ == "__main__":
    main()
