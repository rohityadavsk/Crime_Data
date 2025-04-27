from src.data_processor import CrimeDataProcessor
from src.config import Config
from src.logger import setup_logger
import sys
import os

def main():
    """
    Main entry point for the Crime Data Processing Application.

    Example:
        python src/main.py dev
    """
    
    try:
        # Prefer CLI arg, then ENVIRONMENT env var, then default to 'dev'
        if len(sys.argv) > 1:
            env = sys.argv[1]
        else:
            env = os.environ.get("ENVIRONMENT", "dev")
        
        # Initialize the processor with config
        config = Config(environment=env)
        processor = CrimeDataProcessor(config=config)
        
        # Run the complete pipeline
        processor.run_pipeline()
        
    except Exception as e:
        raise e

if __name__ == "__main__":
    main() 