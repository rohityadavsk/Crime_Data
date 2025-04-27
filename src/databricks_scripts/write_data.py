"""
Write processed data from Delta to final location, optimize, and generate manifest.

Usage:
    python write_data.py <output_path> [log_level]

Arguments:
    output_path: Path to write the Delta table (e.g., dbfs:/FileStore/temp/final_output).
    log_level: (Optional) Logging level (e.g., INFO, DEBUG, WARNING). Default is INFO.

Input Delta table must have columns: DR_NO, AREA, Mocodes.
"""
import sys
from pyspark.sql import SparkSession
from src.logger import setup_logger
import logging

def main():
    # Argument validation
    if len(sys.argv) < 2:
        print("Usage: python write_data.py <output_path> [log_level]")
        sys.exit(1)
    output_path = sys.argv[1]
    log_level = getattr(logging, sys.argv[2].upper(), logging.INFO) if len(sys.argv) > 2 else logging.INFO

    # Setup logger
    logger = setup_logger('databricks_write', level=log_level)
    logger.info(f"Writing data to {output_path}")

    # Initialize Spark session with Delta support
    spark = SparkSession.builder \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    try:
        # Read the processed data from temporary location
        df = spark.read.format("delta").load("dbfs:/FileStore/temp/processed_data")
        logger.info(f"Read {df.count()} records from processed Delta table.")

        # Write the data to the final location
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .option("mergeSchema", "true") \
            .save(output_path)
        logger.info(f"Data written to Delta format at {output_path}")

        # Optimize the Delta table
        spark.sql(f"OPTIMIZE delta.`{output_path}`")
        logger.info("Delta table optimized.")

        # Generate manifest file for downstream processing
        spark.sql(f"GENERATE symlink_format_manifest FOR TABLE delta.`{output_path}`")
        logger.info("Manifest generated for Delta table.")

    except Exception as e:
        logger.error(f"Error writing data: {str(e)}")
        raise

if __name__ == "__main__":
    main() 