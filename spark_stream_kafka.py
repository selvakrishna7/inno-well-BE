from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, split
from pyspark.sql.types import StructType, StringType, MapType, DoubleType
from pyspark.sql.streaming import DataStreamWriter

# Initialize Spark Session with MongoDB support
spark = SparkSession.builder \
    .appName("IoT Kafka Spark Streaming - Only EM") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/iot_data.em_data") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/iot_data.em_data") \
    .getOrCreate()

# Kafka Source
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "iot_raw_data") \
    .option("startingOffsets", "latest") \
    .load()

# Define the schema for incoming JSON
json_schema = StructType() \
    .add("ts", StringType()) \
    .add("params", MapType(StringType(), DoubleType()))

# Parse Kafka JSON and explode the map
parsed_df = df.selectExpr("CAST(value AS STRING) AS json_str") \
    .select(from_json(col("json_str"), json_schema).alias("data")) \
    .select("data.ts", explode("data.params").alias("full_key", "value"))

# Split the full_key to extract structured fields
structured_df = parsed_df.withColumn("parts", split(col("full_key"), "/")) \
    .filter(col("parts").getItem(7) == "EM") \
    .select(
        col("ts").alias("time"),
        col("parts").getItem(0).alias("company_name"),
        col("parts").getItem(1).alias("building_name"),
        col("parts").getItem(2).alias("floor_name"),
        col("parts").getItem(3).alias("S0"),
        col("parts").getItem(4).alias("L0"),
        col("parts").getItem(5).alias("owner_name"),
        col("parts").getItem(6).alias("value_name"),
        col("value")
    )

# Function to write each micro-batch to MongoDB
def write_to_mongo(batch_df, batch_id):
    batch_df.write \
        .format("mongo") \
        .option("checkpointLocation", "/tmp/spark_checkpoint") \
        .mode("append") \
        .save()

# Write the streaming DataFrame to MongoDB using foreachBatch
query = structured_df.writeStream \
    .foreachBatch(write_to_mongo) \
    .start()

query.awaitTermination()
