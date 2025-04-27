import time
from functools import wraps
from src.logger import setup_test_logger, close_logger_handlers
import pytest
from typing import Callable, Any, Optional

# Create a global test logger instance
# Example: test_logger = setup_test_logger('TestResults', level=logging.DEBUG)
test_logger = setup_test_logger('TestResults')

@pytest.fixture(scope="session", autouse=True)
def cleanup_test_logger() -> None:
    """
    Cleanup fixture to ensure test logger is properly closed after all tests.
    """
    yield
    close_logger_handlers(test_logger)

def log_test_result(test_name: str, status: str, duration: Optional[float] = None, error: Optional[str] = None) -> None:
    """
    Log the result of a test case
    
    Args:
        test_name (str): Name of the test case
        status (str): Test status ('PASSED' or 'FAILED')
        duration (float, optional): Test execution time in seconds
        error (str, optional): Error message if test failed
    """
    message = f"Test '{test_name}' {status}"
    if duration is not None:
        message += f" in {duration:.2f} seconds"
    if error:
        message += f" - Error: {error}"
    
    if status == 'PASSED':
        test_logger.info(message)
    else:
        test_logger.error(message)

def log_test_execution(test_func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator to log test case execution
    
    Usage:
        @log_test_execution
        def test_function():
            # Test code here
    """
    @wraps(test_func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        try:
            test_logger.info(f"Starting test '{test_func.__name__}'")
            result = test_func(*args, **kwargs)
            duration = time.time() - start_time
            log_test_result(test_func.__name__, 'PASSED', duration)
            return result
        except Exception as e:
            duration = time.time() - start_time
            log_test_result(test_func.__name__, 'FAILED', duration, str(e))
            raise
    return wrapper

def test_log_test_execution() -> None:
    """Test the log_test_execution decorator"""
    @log_test_execution
    def sample_test() -> bool:
        return True
    
    # Run the decorated test
    assert sample_test() is True

@log_test_execution
def test_test_logger() -> None:
    """Test the test logger functionality"""
    assert test_logger is not None
    test_logger.info("Test message") 