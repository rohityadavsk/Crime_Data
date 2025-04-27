"""
Read data from CSV, write as Delta, and optimize the table.

Usage:
    python read_data.py <input_path> [log_level]

Arguments:
    input_path: Path to the input CSV file or directory (must have columns DR_NO, AREA, Mocodes).
    log_level: (Optional) Logging level (e.g., INFO, DEBUG, WARNING). Default is INFO.

CSV options used: header=True, inferSchema=True. To change, edit the csv_options dict in this script.
"""
import sys
from pyspark.sql import SparkSession
from src.logger import setup_logger
import logging

def main():
    # Argument validation
    if len(sys.argv) < 2:
        print("Usage: python read_data.py <input_path> [log_level]")
        sys.exit(1)
    input_path = sys.argv[1]
    log_level = getattr(logging, sys.argv[2].upper(), logging.INFO) if len(sys.argv) > 2 else logging.INFO

    # Setup logger
    logger = setup_logger('databricks_read', level=log_level)
    logger.info(f"Reading data from {input_path}")

    # CSV options (parameterize here if needed)
    csv_options = {"header": True, "inferSchema": True}
    logger.info(f"CSV read options: {csv_options}")

    # Initialize Spark session with Delta support
    spark = SparkSession.builder \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    try:
        # Read the data
        df = spark.read.options(**csv_options).csv(input_path)
        logger.info(f"Successfully read data: {df.count()} records")

        # Save the DataFrame to a temporary location for the next job
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .save("dbfs:/FileStore/temp/input_data")
        logger.info("Data written to Delta format at dbfs:/FileStore/temp/input_data")

        # Optimize the Delta table
        spark.sql("OPTIMIZE delta.`dbfs:/FileStore/temp/input_data`")
        logger.info("Delta table optimized.")

        return df
    except Exception as e:
        logger.error(f"Error reading data: {str(e)}")
        raise

if __name__ == "__main__":
    df = main() 