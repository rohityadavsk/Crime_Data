import logging
import os
from datetime import datetime
from contextlib import contextmanager
import atexit
import importlib
from typing import Optional, Generator

# Global variables to track logger instances
test_loggers = {}
main_loggers = {}

# Global logger instances
_loggers = {}

def get_azure_clients() -> tuple:
    """
    Lazily import and return Azure clients. (Not implemented)
    Returns:
        Tuple of (DataLakeServiceClient, DefaultAzureCredential) or (None, None) if not available.
    """
    try:
        DataLakeFileClient = importlib.import_module('azure.storage.filedatalake').DataLakeFileClient
        DataLakeServiceClient = importlib.import_module('azure.storage.filedatalake').DataLakeServiceClient
        DefaultAzureCredential = importlib.import_module('azure.identity').DefaultAzureCredential
        return DataLakeServiceClient, DefaultAzureCredential
    except ImportError:
        return None, None

def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Setup a logger with file and console handlers.

    Args:
        name (str): Logger name.
        level (int): Logging level (default: logging.INFO).
    Returns:
        logging.Logger: Configured logger instance.

    Example:
        logger = setup_logger('main_app', level=logging.DEBUG)
        logger.info('This is an info message')
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        os.makedirs("logs", exist_ok=True)
        log_file = f"logs/{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    return logger

def setup_test_logger(test_name: str, level = logging.INFO) -> logging.Logger:
    """
    Set up a logger specifically for tests.

    Args:
        test_name (str): Name for the test logger.
        level (int): Logging level (default: logging.INFO).
    Returns:
        logging.Logger: Configured test logger instance.

    Example:
        test_logger = setup_test_logger('MyTest', level=logging.DEBUG)
        test_logger.info('Test started')
    """
    logger = logging.getLogger(f'test_{test_name}')
    logger.setLevel(level)
    if not logger.handlers:
        os.makedirs('logs', exist_ok=True)
        log_file = f'logs/test_{test_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    return logger

@contextmanager
def logger_context(name: str, level: int = logging.INFO) -> Generator[logging.Logger, None, None]:
    """
    Context manager for main logger cleanup.

    Args:
        name (str): Logger name.
        level (int): Logging level.
    Yields:
        logging.Logger: Configured logger instance.

    Example:
        with logger_context('main_app') as logger:
            logger.info('Inside context')
    """
    logger = setup_logger(name, level)
    try:
        yield logger
    finally:
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

def close_logger_handlers(logger: logging.Logger) -> None:
    """
    Close all handlers for a logger.
    Args:
        logger (logging.Logger): Logger instance.
    """
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)

def close_all_loggers() -> None:
    """
    Close all logger handlers for all loggers in the logging module.
    """
    for logger_name in list(logging.Logger.manager.loggerDict.keys()):
        logger = logging.getLogger(logger_name)
        close_logger_handlers(logger)

# Register cleanup function
atexit.register(close_all_loggers) 