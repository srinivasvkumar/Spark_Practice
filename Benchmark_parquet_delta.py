from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand
import time
from delta import *
# Initialize Spark with Delta Support

from pyspark.sql import SparkSession

builder = SparkSession.builder \
    .appName("Benchmark_parquet_delta") \
    .config('spark.master', 'local[*]') \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/tmp/spark-events")  # default location where spark-history-server.sh looks for logs


spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Define storage paths
s3_path = "/home/srinivas/Spark_Practice/"
parquet_path = s3_path + "fact_orders_parquet"
delta_path = s3_path + "fact_orders_delta"

# Generate Sample Data (10M rows for benchmarking)
fact_orders = spark.range(10000000) \
    .select(
        (col("id") + 1).alias("order_id"),
        (col("id") % 2000000 + 1).alias("customer_id"),
        (col("id") % 100000 + 1).alias("product_id"),
        (col("id") % 10 + 1).alias("region_id"),
        (rand() * 1000).alias("order_amount"),
        (col("id") % 30 + 1).alias("order_date")
)

# Save as Parquet
fact_orders.write.format("parquet").mode(
    "overwrite").partitionBy("order_date").save(parquet_path)

# Save as Delta Lake
fact_orders.write.format("delta").mode(
    "overwrite").partitionBy("order_date").save(delta_path)

# Optimize Delta
spark.sql(f"OPTIMIZE delta.`{delta_path}` ZORDER BY (customer_id, product_id)")

# Benchmark Read Performance


def benchmark_read(path, format_type):
    start_time = time.time()
    df = spark.read.format(format_type).load(path)
    df.count()  # Force execution
    end_time = time.time()
    print(f"{format_type.upper()} Read Time: {end_time - start_time:.2f} seconds")


benchmark_read(parquet_path, "parquet")
benchmark_read(delta_path, "delta")

# Benchmark Filter Performance


def benchmark_filter(path, format_type):
    start_time = time.time()
    df = spark.read.format(format_type).load(path)
    df.filter("customer_id = 123456").count()  # Force execution
    end_time = time.time()
    print(f"{format_type.upper()} Filter Time: {end_time - start_time:.2f} seconds")


benchmark_filter(parquet_path, "parquet")
benchmark_filter(delta_path, "delta")

print("Benchmarking completed!")
