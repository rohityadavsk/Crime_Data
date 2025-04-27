import pytest
from pyspark.sql import SparkSession, DataFrame
from src.data_processor import CrimeDataProcessor
from src.config import Config
from src.logger import setup_test_logger, close_logger_handlers
import time
import os
from typing import Generator

test_logger = setup_test_logger('DataProcessorTests')

@pytest.fixture(scope="session", autouse=True)
def cleanup_logger() -> Generator[None, None, None]:
    """
    Cleanup fixture to ensure test logger is properly closed after all tests.

    Example:
        @pytest.fixture(scope="session", autouse=True)
        def cleanup_logger():
            yield
            test_logger.info("Cleaning up test logger")
    """
    yield
    test_logger.info("Cleaning up test logger")
    close_logger_handlers(test_logger)

@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """
    Create a SparkSession for testing.
    """
    test_logger.info("Creating test Spark session")
    spark = SparkSession.builder \
        .appName("TestCrimeDataProcessing") \
        .master("local[2]") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    yield spark
    test_logger.info("Stopping test Spark session")
    spark.stop()

@pytest.fixture(scope="session")
def test_config() -> Config:
    """
    Create a test configuration.
    """
    return Config(environment='dev')

@pytest.fixture(scope="session")
def processor(spark: SparkSession, test_config: Config) -> Generator[CrimeDataProcessor, None, None]:
    """
    Create a shared CrimeDataProcessor instance.
    """
    test_logger.info("Creating CrimeDataProcessor instance")
    processor = CrimeDataProcessor(config=test_config)
    yield processor
    # Cleanup will be handled by spark fixture

@pytest.fixture(scope="session")
def sample_data(spark: SparkSession) -> DataFrame:
    """
    Create sample test data with columns DR_NO, AREA, Mocodes.
    """
    test_logger.info("Creating sample test data")
    data = [
        (1001, "Central", "001,002"),
        (1002, "West", "003,004"),
        (1003, "East", "005,006")
    ]
    columns = ["DR_NO", "AREA", "Mocodes"]
    return spark.createDataFrame(data, columns)

# def test_read_data(processor: CrimeDataProcessor, spark: SparkSession, sample_data: DataFrame) -> None:
#     """
#     Test reading data functionality.
#     """
#     test_logger.info("Starting test_read_data")
#     start_time = time.time()
#     try:
#         # Write sample data as CSV first
#         sample_data.write.mode("overwrite").option("header", True).csv(processor.config.input_path)
#         # Now test reading
#         processor.read_data()
#         assert processor.df is not None
#         assert processor.df.count() > 0
#         test_logger.info(f"test_read_data PASSED - Processed {processor.df.count()} rows")
#     except Exception as e:
#         test_logger.error(f"test_read_data FAILED - {str(e)}")
#         raise
#     finally:
#         test_logger.info(f"test_read_data completed in {time.time() - start_time:.2f} seconds")

# def test_process_data(processor: CrimeDataProcessor, sample_data: DataFrame) -> None:
#     """
#     Test data processing functionality.
#     """
#     test_logger.info("Starting test_process_data")
#     start_time = time.time()
#     try:
#         # Set the input DataFrame
#         processor.df = sample_data
#         processor.process_data()
#         # Verify the processed DataFrame has the expected columns
#         assert processor.processed_df is not None
#         assert "DR_NO" in processor.processed_df.columns
#         assert "AREA" in processor.processed_df.columns
#         assert "Mocodes" in processor.processed_df.columns
#         test_logger.info(f"test_process_data PASSED - Processed {processor.processed_df.count()} rows")
#     except Exception as e:
#         test_logger.error(f"test_process_data FAILED - {str(e)}")
#         raise
#     finally:
#         test_logger.info(f"test_process_data completed in {time.time() - start_time:.2f} seconds")

# def test_write_data(processor: CrimeDataProcessor, sample_data: DataFrame, tmp_path) -> None:
#     """
#     Test writing data functionality.
#     """
#     test_logger.info("Starting test_write_data")
#     start_time = time.time()
#     try:
#         # Set up test data
#         processor.df = sample_data
#         processor.process_data()
#         # Update output path for test
#         processor.config.output_path = str(tmp_path)
#         processor.write_data()
#         # Verify the output directory exists and contains files
#         assert os.path.exists(tmp_path)
#         assert any(os.scandir(tmp_path))
#         test_logger.info("test_write_data PASSED - Data written successfully")
#     except Exception as e:
#         test_logger.error(f"test_write_data FAILED - {str(e)}")
#         raise
#     finally:
#         test_logger.info(f"test_write_data completed in {time.time() - start_time:.2f} seconds") 