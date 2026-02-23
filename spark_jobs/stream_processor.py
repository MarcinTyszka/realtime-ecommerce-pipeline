import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, year, month, dayofmonth, hour
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

# Configuration
KAFKA_TOPIC = "clicks"
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
S3_BUCKET_NAME = "ecommerce-raw"
CHECKPOINT_LOCATION = f"s3a://{S3_BUCKET_NAME}/checkpoints/clicks"
OUTPUT_PATH = f"s3a://{S3_BUCKET_NAME}/data/clicks"

def main():
    # 1. Initialize Spark Session with MinIO dependencies
    spark = SparkSession.builder \
        .appName("EcommerceRealTimeEngine") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    # 2. Define Schema
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_session", StringType(), True),
        StructField("membership_level", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("age_group", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("category", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("traffic_source", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("os", StringType(), True),
        StructField("payment_method", StringType(), True)
    ])

    # 3. Read Stream from Kafka
    print("Reading stream from Kafka...")
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # 4. Transform Data
    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    transformed_stream = parsed_stream \
        .withColumn("event_timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("year", year(col("event_timestamp"))) \
        .withColumn("month", month(col("event_timestamp"))) \
        .withColumn("day", dayofmonth(col("event_timestamp"))) \
        .withColumn("hour", hour(col("event_timestamp")))

    # 5. Write Stream to MinIO in Parquet format
    query = transformed_stream.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", OUTPUT_PATH) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .partitionBy("year", "month", "day", "hour") \
        .trigger(processingTime="10 seconds") \
        .start()

    print(f"Streaming started. Writing to {OUTPUT_PATH}...")
    query.awaitTermination()

if __name__ == "__main__":
    main()