import os
from pathlib import Path
from typing import Optional, Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from .config import Config
from .logger import setup_logger

class CrimeDataProcessor:
    """
    Processes crime data from CSV source and writes it in Delta format.
    """
    def __init__(self, config: Config):
        """
        Initialize the processor with configuration and logger.
        """
        self.config = config
        self.logger = setup_logger('crime_processor')
        self.spark = SparkSession.builder \
            .appName("CrimeDataProcessing") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1") \
            .getOrCreate()
        self.df: Optional[DataFrame] = None
        self.processed_df: Optional[DataFrame] = None
        self.logger.info("Initialized CrimeDataProcessor")

    def read_data(self, csv_options: Optional[Dict[str, Any]] = None) -> DataFrame:
        """
        Read input data from CSV file with configurable options.
        Args:
            csv_options: Optional dictionary of CSV read options.
        Returns:
            DataFrame: The loaded DataFrame.
        Raises:
            FileNotFoundError: If the input file does not exist.
            Exception: If reading fails for other reasons.
        """
        options = {"header": True, "inferSchema": True}
        if csv_options:
            options.update(csv_options)
        input_path = self.config.input_path
        if not Path(input_path).exists():
            self.logger.error(f"Input file or directory does not exist: {input_path}")
            raise FileNotFoundError(f"Input file or directory does not exist: {input_path}")
        self.logger.info(f"Reading data from {input_path} with options: {options}")
        try:
            self.df = self.spark.read.options(**options).csv(input_path)
            row_count = self.df.count()
            self.logger.info(f"Read {row_count} records from CSV.")
            return self.df
        except Exception as e:
            self.logger.error(f"Failed to read CSV: {str(e)}")
            raise

    def process_data(self, df: Optional[DataFrame] = None) -> DataFrame:
        """
        Process the data by selecting relevant columns.
        Args:
            df: Optional DataFrame to process. If None, uses self.df.
        Returns:
            DataFrame: The processed DataFrame.
        Raises:
            ValueError: If no DataFrame is available to process.
        """
        self.logger.info("Processing data")
        input_df = df if df is not None else self.df
        if input_df is None:
            self.logger.error("No DataFrame available to process.")
            raise ValueError("No DataFrame available to process.")
        processed_df = input_df.select(
            col("DR_NO"),
            col("AREA"),
            col("Mocodes")
        )
        row_count = processed_df.count()
        self.logger.info(f"Processed {row_count} records (selected columns: DR_NO, AREA, Mocodes)")
        if df is None:
            self.processed_df = processed_df
        return processed_df

    def write_data(
        self,
        df: Optional[DataFrame] = None,
        partition_by: Optional[List[str]] = None,
        mode: str = "overwrite",
        write_options: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Write processed data to Delta format with optional partitioning and advanced options.
        Args:
            df: Optional DataFrame to write. If None, uses self.processed_df.
            partition_by: Optional list of columns to partition by.
            mode: Write mode (default: 'overwrite').
            write_options: Additional write options as a dictionary.
        Raises:
            ValueError: If no DataFrame is available to write.
        """
        self.logger.info(f"Writing data to {self.config.output_path}")
        output_df = df if df is not None else self.processed_df
        if output_df is None:
            self.logger.error("No DataFrame available to write.")
            raise ValueError("No DataFrame available to write.")
        writer = output_df.write.format("delta").mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if write_options:
            for k, v in write_options.items():
                writer = writer.option(k, v)
        writer.save(self.config.output_path)
        row_count = output_df.count()
        self.logger.info(f"Data written successfully to {self.config.output_path} with {row_count} records.")

    def run_pipeline(self) -> None:
        """
        Run the complete data processing pipeline: read, process, and write.
        Raises:
            Exception: If any step in the pipeline fails.
        """
        try:
            self.read_data()
            self.process_data()
            self.write_data()
            self.logger.info("Pipeline execution completed successfully")
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            raise 