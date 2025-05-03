import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

@dataclass
class Config:
    """
    Configuration settings for the application.

    Example:
        # For local development
        config = Config(environment='dev')
        # For Databricks pre-prod
        config = Config(environment='pre-prod')
        # With custom data directory
        config = Config(environment='dev', base_data_dir='/tmp/mydata')
    """
    environment: str = 'dev'
    input_path: Optional[str] = None
    output_path: Optional[str] = None
    use_databricks: bool = False
    base_data_dir: Optional[str] = None
    data_path: str = field(init=False)
    azure_storage_account_name: Optional[str] = None
    azure_storage_account_key: Optional[str] = None
    azure_storage_container_name: Optional[str] = None

    def __post_init__(self) -> None:
        """
        Initialize configuration based on environment and custom data directory.

        Example:
            config = Config(environment='dev', base_data_dir='/tmp/mydata')
        """
        # Set base directory
        if self.base_data_dir:
            self.base_dir = Path(self.base_data_dir)
        else:
            self.base_dir = Path(os.getcwd())

        # Set up paths and configs based on environment
        if self.environment == 'dev':
            self.input_path = self.input_path or str(self.base_dir / 'data' / 'input')
            self.output_path = self.output_path or str(self.base_dir / 'data' / 'output')
            self.use_databricks = False
            # Create directories if they don't exist
            os.makedirs(self.input_path, exist_ok=True)
            os.makedirs(self.output_path, exist_ok=True)
        else:
            # For pre-prod and prod, use Azure Data Lake paths
            self.input_path = self.input_path or f"abfss://{self.azure_storage_container_name}@{self.azure_storage_account_name}.dfs.core.windows.net/raw/"
            self.output_path = self.output_path or f"abfss://{self.azure_storage_container_name}@{self.azure_storage_account_name}.dfs.core.windows.net/output"
            self.use_databricks = True
            self._set_databricks_config()
            self._set_azure_storage_config()

        self.data_path = str(self.base_dir / 'data')

    def _set_databricks_config(self) -> None:
        """
        Set Databricks-specific configuration from environment variables.

        Example:
            config = Config(environment='pre-prod')
        """

        self.databricks_host = os.getenv('DATABRICKS_HOST')
        self.databricks_token = os.getenv('DATABRICKS_TOKEN') 
        self.databricks_cluster_id = os.getenv('DATABRICKS_CLUSTER_ID')
            
        if not all([self.databricks_host, self.databricks_token, self.databricks_cluster_id]):
            raise ValueError(f"Missing required Databricks configuration for {self.environment} environment") 

    def _set_azure_storage_config(self) -> None:
        """
        Set Azure Storage configuration from environment variables.
        """
        self.azure_storage_account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
        self.azure_storage_account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
        self.azure_storage_container_name = os.getenv('AZURE_STORAGE_CONTAINER_NAME')
        if not all([self.azure_storage_account_name, self.azure_storage_account_key, self.azure_storage_container_name]):
            raise ValueError("Missing required Azure Storage configuration") 