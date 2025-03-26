from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, monotonically_increasing_id
from delta.tables import DeltaTable
from delta import *

# Initialize Spark with Delta Support
builder = SparkSession.builder \
    .appName("DeltaMultiJoinDataGen") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/tmp/spark-events")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Define S3 bucket location for Delta tables
s3_path = "/home/srinivas/Spark_Practice/"

# Create dim_regions (50 rows)
dim_regions = spark.createDataFrame([
    (1, "North America"), (2, "South America"), (3, "Europe"),
    (4, "Asia"), (5, "Africa"), (6, "Oceania"), (7, "Middle East"),
    (8, "Central America"), (9, "Caribbean"), (10, "Antarctica")
], ["region_id", "region_name"])
dim_regions.write.format("delta").mode("overwrite").save(s3_path + "dim_regions")

# Create dim_customers (2M rows)
dim_customers = spark.range(2000000) \
    .select(
        (col("id") + 1).alias("customer_id"),
        (rand() * 10 + 20).alias("age"),
        (rand() * 5000).alias("credit_score"),
        (col("id") % 10 + 1).alias("region_id")
    )
dim_customers.write.format("delta").mode("overwrite").save(s3_path + "dim_customers")
spark.sql(f"OPTIMIZE delta.`{s3_path}dim_customers` ZORDER BY (customer_id)")

# Create dim_products (100K rows)
dim_products = spark.range(100000) \
    .select(
        (col("id") + 1).alias("product_id"),
        (rand() * 100).alias("price"),
        (rand() * 500).alias("cost"),
        (rand() * 5).alias("rating")
    )
dim_products.write.format("delta").mode("overwrite").save(s3_path + "dim_products")
spark.sql(f"OPTIMIZE delta.`{s3_path}dim_products` ZORDER BY (product_id)")

# Create fact_orders (500M rows)
fact_orders = spark.range(500000000) \
    .select(
        (col("id") + 1).alias("order_id"),
        (col("id") % 2000000 + 1).alias("customer_id"),
        (col("id") % 100000 + 1).alias("product_id"),
        (col("id") % 10 + 1).alias("region_id"),
        (rand() * 1000).alias("order_amount"),
        (col("id") % 30 + 1).alias("order_date")
    )
fact_orders.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("order_date") \
    .save(s3_path + "fact_orders")
spark.sql(f"OPTIMIZE delta.`{s3_path}fact_orders` ZORDER BY (customer_id, product_id)")

print("Data generation completed and stored in Delta format.")
