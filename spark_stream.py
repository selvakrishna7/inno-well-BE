from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, MapType

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaToSparkStreaming") \
    .master("local[*]") \
    .getOrCreate()

# Define schema for parsing JSON
schema = StructType() \
    .add("ts", StringType()) \
    .add("params", MapType(StringType(), StringType()))

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "iot_raw_data") \
    .option("startingOffsets", "latest") \
    .load()

# Convert binary Kafka value to string
df = df.selectExpr("CAST(value AS STRING) as json")

# Parse JSON string into structured format
parsed_df = df.select(from_json(col("json"), schema).alias("data")) \
    .select(
        col("data.ts").alias("timestamp"),
        col("data.params")["voltage"].alias("voltage"),
        col("data.params")["current"].alias("current"),
        col("data.params")["pf"].alias("power_factor")
    )

# Write to console
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
