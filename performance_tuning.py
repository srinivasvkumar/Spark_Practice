from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, broadcast
import time
from delta import *

# Initialize Spark Session
builder = SparkSession.builder \
    .config("spark.executor.memory", "20g") \
    .config("spark.driver.memory", "20g") \
    .config("spark.memory.fraction", "0.6") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=40") \
    .appName("SparkPerformanceTuning") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.kryo.registrationRequired", "true") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "8g") \
    .config("spark.eventLog.dir", "/tmp/spark-events")   # default location where spark-history-server.sh looks for logs

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Generate Large Data for Performance Testing
df_large = spark.range(50000000).toDF("id") \
    .withColumn("value", rand())

df_small = spark.range(1000).toDF("id") \
    .withColumn("factor", rand())

# 1️⃣ Default Join (Without Optimization)


def default_join():
    start = time.time()
    df_large.join(df_small, "id", "inner").count()
    print(f"Default Join Time: {time.time() - start:.2f} sec")
    

default_join()
spark.catalog.clearCache()

def optimized_join():
    start = time.time()
    df_large.join(broadcast(df_small), "id", "inner").count()
    print(f"Optimized Broadcast Join Time: {time.time() - start:.2f} sec")


optimized_join()
spark.catalog.clearCache()

def repartition_optimization():
    start = time.time()
    df_large.repartition(100).join(df_small, "id", "inner").count()
    print(f"Repartitioned Join Time: {time.time() - start:.2f} sec")


repartition_optimization()
spark.catalog.clearCache()

# 2️⃣ Caching Example
print("Caching Example")
spark.catalog.clearCache()
df_large.cache().count()
start = time.time()
df_large.count()
print(f"Cached Count Time: {time.time() - start:.2f} sec")

# 3️⃣ Serialization Optimization
# spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") # included in the spark config only

# 4️⃣ Shuffle Optimization


def shuffle_optimization():
    start = time.time()
    df_large.repartition(200).groupBy("id").count().count()
    print(f"Shuffle Optimized GroupBy Time: {time.time() - start:.2f} sec")


shuffle_optimization()
spark.catalog.clearCache()

# 5️⃣ Coalesce vs Repartition


def coalesce_vs_repartition():
    start = time.time()
    df_large.repartition(50).count()
    print(f"Repartition (50) Time: {time.time() - start:.2f} sec")

    start = time.time()
    df_large.coalesce(50).count()
    print(f"Coalesce (50) Time: {time.time() - start:.2f} sec")


coalesce_vs_repartition()
spark.catalog.clearCache()

# 6️⃣ Adaptive Query Execution (AQE)
# ark.conf.set("spark.sql.adaptive.enabled", "true")
print("first")


def aqe_optimization():
    print("second")
    start = time.time()
    df_large.groupBy("id").count().count()
    print(f"AQE Enabled GroupBy Time: {time.time() - start:.2f} sec")


print("start")
# aqe_optimization() #commented as thsi s leading to OOM failure

print("Performance tuning series completed!")

