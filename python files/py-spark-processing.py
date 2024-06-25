import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize logging for PySpark script
log_file = "/home/omarattia/Projects/logs/pyspark_kafka_to_parquet.log"
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # Initialize Spark session with some resource configuration
    logging.info("Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("KafkaToParquetProcessor") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.instances", "2") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    # Define the schema for the data (explicitly define all fields)
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("order_timestamp", TimestampType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("shipping_address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zipcode", StringType(), True)
        ]), True)
    ])

    # Read streaming data from Kafka
    logging.info("Reading streaming data from Kafka...")
    streaming_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "e-commerce_data_streaming_topic") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()

    # Convert the value column to string and parse JSON
    logging.info("Parsing JSON data and applying schema...")
    json_df = streaming_df \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Output directory for Parquet files
    output_path = "/home/omarattia/Projects/PySpark_Loading_Parquets"

    # Write the transformed data to Parquet files using writeStream
    logging.info("Writing transformed data to Parquet files...")
    query = json_df.writeStream \
        .trigger(processingTime='60 seconds') \
        .format("parquet") \
        .outputMode("append") \
        .option("path", output_path) \
        .option("checkpointLocation", "/home/omarattia/Projects/PySpark_Checkpoint") \
        .start()

    # Wait for up to 60 seconds for the streaming to finish
    start_time = time.time()
    while time.time() - start_time < 60 and query.isActive:
        time.sleep(1)

    # Stop the streaming query and Spark session
    if query.isActive:
        logging.info("Stopping streaming query...")
        query.stop()

    # Stop Spark session
    logging.info("Stopping Spark session...")
    spark.stop()

except Exception as e:
    logging.error(f"An error occurred: {str(e)}")
    raise
