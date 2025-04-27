"""
Process data from Delta, select columns, write as Delta, and optimize the table.

Usage:
    python process_data.py [log_level]

Arguments:
    log_level: (Optional) Logging level (e.g., INFO, DEBUG, WARNING). Default is INFO.

Input Delta table must have columns: DR_NO, AREA, Mocodes.
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.logger import setup_logger
import logging

def main():
    # Argument validation
    log_level = getattr(logging, sys.argv[1].upper(), logging.INFO) if len(sys.argv) > 1 else logging.INFO

    # Setup logger
    logger = setup_logger('databricks_process', level=log_level)
    logger.info("Starting data processing")
    
    # Initialize Spark session with Delta support
    spark = SparkSession.builder \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    try:
        # Read the input data from temporary location
        df = spark.read.format("delta").load("dbfs:/FileStore/temp/input_data")
        logger.info(f"Read {df.count()} records from Delta input.")
        
        # Process the data (select columns DR_NO, AREA, Mocodes)
        processed_df = df.select(
            col("DR_NO"),
            col("AREA"),
            col("Mocodes")
        )
        logger.info(f"Processed {processed_df.count()} records (selected columns: DR_NO, AREA, Mocodes)")
        
        # Save the processed DataFrame to a temporary location
        processed_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save("dbfs:/FileStore/temp/processed_data")
        logger.info("Processed data written to Delta format at dbfs:/FileStore/temp/processed_data")
        
        # Optimize the Delta table
        spark.sql("OPTIMIZE delta.`dbfs:/FileStore/temp/processed_data`")
        logger.info("Delta table optimized.")
        
        return processed_df
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise

if __name__ == "__main__":
    main() 