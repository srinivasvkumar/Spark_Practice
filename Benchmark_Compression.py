from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time
import random
from pyspark.sql import Row
import os


# **Exercise: Evaluating Compression Performance in Spark**
# **Step 1: Initialize Spark Session**
# Initialize Spark

spark = SparkSession.builder \
    .appName("CompressionComparison") \
    .getOrCreate()


# **Step 2: Create or Load a Large Dataset**


# Generate dummy data (1 million rows)
data = [Row(id=i, name=f"User_{i}", value=random.randint(
    1, 1000)) for i in range(1_000_000)]

df = spark.createDataFrame(data)
df.show(5)


# **Step 3: Write the Data with Different Compression Types**


compression_types = ["none", "snappy", "gzip", "lz4", "zstd"]

for compression in compression_types:
    path = f"/tmp/compressed_{compression}"
    df.write.mode("overwrite").option("compression", compression).parquet(path)
    print(f"Data written using {compression} compression.")


# **Step 4: Compare File Sizes**


for compression in compression_types:
    path = f"/tmp/compressed_{compression}"
    size = sum(os.path.getsize(os.path.join(root, file))
               for root, _, files in os.walk(path) for file in files)
    print(
        f"Compression: {compression}, File Size: {size / (1024 * 1024):.2f} MB")


# **Step 5: Measure Read Performance**


for compression in compression_types:
    path = f"/tmp/compressed_{compression}"
    start_time = time.time()
    df_read = spark.read.parquet(path)
    df_read.count()  # Trigger an action to measure performance
    end_time = time.time()
    print(f"Read time for {compression}: {end_time - start_time:.2f} seconds")
