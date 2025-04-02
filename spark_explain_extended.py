from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col
import time


def run_exercise(query, description):
    print(f"\n{description}")
    start_time = time.time()
    query.count()  # Trigger computation
    end_time = time.time()
    print(f"Execution Time: {end_time - start_time:.2f} seconds")
    query.explain("extended")


# Initialize Spark session
spark = SparkSession.builder.appName("JoinBenchmark").getOrCreate()

# Create large and small datasets
df_large = spark.range(0, 50000000).selectExpr("id as id", "rand() as value")
df_small = spark.range(0, 1000).selectExpr("id as id", "rand() as factor")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Category 1️⃣: Join Optimizations
# Exercise 1 - Default Join (Check if SortMergeJoin is used)
run_exercise(df_large.join(df_small, "id", "inner"), "Sort Merge Join")

# Exercise 2 - Broadcast Join Optimization
run_exercise(df_large.join(broadcast(df_small),
             "id", "inner"), "Broadcast Join")

# Exercise 3 - Skewed Data Handling
skewed_large = df_large.withColumn("id", col("id") % 5)
run_exercise(skewed_large.join(df_small, "id", "inner"), "Skewed Data Join")

# Exercise 4 - Reducing Shuffle Partitions
spark.conf.set("spark.sql.shuffle.partitions", "500")
run_exercise(df_large.repartition(500).join(
    df_small, "id", "inner"), "Excessive Shuffle Partitions")

# Exercise 5 - Cartesian Join (Very Expensive)
run_exercise(df_large.crossJoin(df_small), "Cartesian Join - Avoid This")

# Category 2️⃣: Caching & Reuse
# Exercise 6 - Without Cache
run_exercise(df_large.select("id", "value"), "Without Cache")

# Exercise 7 - With Cache
cached_df = df_large.select("id", "value").cache()
cached_df.count()  # Trigger cache
run_exercise(cached_df, "With Cache - Optimized")

# Category 3️⃣: Coalesce vs. Repartition
# Exercise 8 - Repartition vs. Coalesce
run_exercise(df_large.repartition(500), "Repartition High (Expensive)")
run_exercise(df_large.coalesce(
    50), "Coalesce (Efficient for Merging Partitions)")

# Category 4️⃣: Adaptive Query Execution (AQE)
spark.conf.set("spark.sql.adaptive.enabled", "false")
run_exercise(df_large.groupBy("id").count(), "Without AQE")

spark.conf.set("spark.sql.adaptive.enabled", "true")
run_exercise(df_large.groupBy("id").count(), "With AQE")

print("\nExecution Plans and Timings Generated Successfully!")
