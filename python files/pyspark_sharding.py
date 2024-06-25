# All rights reserved to this software and its documentation are reserved under "OMAR H. ATTIA"
# Read the requring documentation before executing this script.
# If you have any questions please feel free to contact me via my LinkedIn account.
# This script mocks the streaming of a big data sources and its dependencies and helps with debugging and testing them properly.


import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize logging for PySpark script
log_file = "/path/to/logs/pyspark_sql_aggregations.log"  # Replace with your log file path
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # Initialize Spark session with some resource configuration
    logging.info("Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("SQLAggregationsProcessor") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.instances", "2") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    # Read Parquet data
    input_path = "/path/to/your/PySpark_Loading_Parquets"  # Replace with your input directory path
    parquet_df = spark.read.parquet(input_path)

    # Print schema to verify available columns
    logging.info("Schema of parquet_df:")
    parquet_df.printSchema()

    # Register DataFrame as a temporary view
    parquet_df.createOrReplaceTempView("ecommerce_orders")

    # Perform SQL aggregation
    logging.info("Performing SQL aggregations...")
    aggregated_df = spark.sql("""
        SELECT
            product_name,
            SUM(quantity) AS total_quantity,
            AVG(price) AS average_price,
            MAX(price) AS max_price,
            MIN(price) AS min_price
        FROM ecommerce_orders
        GROUP BY product_name
    """)

    # Output directory for aggregated results (CSV format)
    output_path = "/path/to/your/PySpark_Aggregated_Results"  # Replace with your output directory path

    # Write aggregated data to CSV files
    logging.info("Writing aggregated data to CSV files...")
    query = aggregated_df \
        .write \
        .mode("overwrite") \
        .csv(output_path)

    logging.info("Aggregation and CSV writing completed successfully.")

except Exception as e:
    logging.error(f"An error occurred: {str(e)}")
    raise

finally:
    # Stop Spark session
    if 'spark' in locals():
        logging.info("Stopping Spark session...")
        spark.stop()
