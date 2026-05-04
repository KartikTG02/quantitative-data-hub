import os
import pyspark

# Force PySpark to use Java 17, bypassing the Codespace default
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as spark_sum, max as spark_max, min as spark_min, first, last
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Dynamically fetch the exact PySpark version pip installed
spark_version = pyspark.__version__
kafka_jar = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}"
postgres_jar = "org.postgresql:postgresql:42.6.0"

# 1. Initialize Spark 
print(f"Initializing PySpark Engine (Version {spark_version})...")
spark = SparkSession.builder \
    .appName("QuantitativeDataHub") \
    .config("spark.jars.packages", f"{kafka_jar},{postgres_jar}") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", "4")
kafka_schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)
])

# metadata_df = spark.read.csv("company_metadata.csv", header=True)

metadata_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/market_data") \
    .option("dbtable", "dim_stock_metadata") \
    .option("user", "admin") \
    .option("password", "password123") \
    .option("driver", "org.postgresql.Driver") \
    .load()

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "nse_live_ticks") \
    .option("StartingOffsets", "latest") \
    .load()

parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), kafka_schema).alias("data")
).select("data.*")


aggregated_stream = parsed_stream \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "1 minute"), col("ticker")) \
    .agg(
        first("price").alias("open_price"),
        spark_max("price").alias("high_price"),
        spark_min("price").alias("low_price"),
        last("price").alias("close_price"),
        spark_sum("volume").alias("total_volume"),
        (spark_sum(col("price") * col("volume")) / spark_sum("volume")).alias("vwap")
    )

enriched_stream = aggregated_stream.join(metadata_df, "ticker", "left")

final_df = enriched_stream.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("ticker"),
    col("company_name"),
    col("sector"),
    col("open_price"),
    col("high_price"),
    col("low_price"),
    col("close_price"),
    col("total_volume"),
    col("vwap")
)

def write_to_postgres(df, epoch_id):
    """
    Spark Structured Streaming cannot write to JDBC natively in stream mode.
    foreachBatch allows us to take the finalized 1-minute DataFrame chunk 
    and write it to Postgres like a standard batch job.
    """

    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/market_data") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "gold_market_candles") \
        .option("user", "admin") \
        .option("password", "password123") \
        .mode("append") \
        .save()
    
print("Igniting Stream-static join pipeline. Awaiting 60-second window closures...")

query = final_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .start()

query.awaitTermination()