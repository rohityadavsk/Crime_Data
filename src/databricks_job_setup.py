import os
import json
import requests
from src.config import Config

def create_databricks_job(config: Config) -> str:
    """
    Create a Databricks job using the REST API directly.
    
    Args:
        config: Configuration object with Databricks and Azure Storage settings
    
    Returns:
        str: The created job ID
    """
    # Define the API endpoint for job creation
    api_url = f"{config.databricks_host}/api/2.1/jobs/create"
    
    # Define headers for authentication
    headers = {
        "Authorization": f"Bearer {config.databricks_token}",
        "Content-Type": "application/json"
    }
    
    # Define the job payload
    job_payload = {
        "name": f"Crime Data Pipeline - {config.environment}",
        "format": "MULTI_TASK",
        "tasks": [
            {
                "task_key": "read_data",
                "depends_on": [],
                "existing_cluster_id": config.databricks_cluster_id,
                "python_wheel_task": {
                    "package_name": "crime_data_processor",
                    "entry_point": "read_data",
                    "parameters": [
                        f"--input_path={config.input_path}",
                        f"--azure_storage_account_name={config.azure_storage_account_name}",
                        f"--azure_storage_account_key={config.azure_storage_account_key}",
                        f"--azure_storage_container_name={config.azure_storage_container_name}"
                    ]
                }
            },
            {
                "task_key": "process_data",
                "depends_on": [
                    {
                        "task_key": "read_data"
                    }
                ],
                "existing_cluster_id": config.databricks_cluster_id,
                "python_wheel_task": {
                    "package_name": "crime_data_processor",
                    "entry_point": "process_data"
                }
            },
            {
                "task_key": "write_data",
                "depends_on": [
                    {
                        "task_key": "process_data"
                    }
                ],
                "existing_cluster_id": config.databricks_cluster_id,
                "python_wheel_task": {
                    "package_name": "crime_data_processor",
                    "entry_point": "write_data",
                    "parameters": [
                        f"--output_path={config.output_path}",
                        f"--azure_storage_account_name={config.azure_storage_account_name}",
                        f"--azure_storage_account_key={config.azure_storage_account_key}",
                        f"--azure_storage_container_name={config.azure_storage_container_name}"
                    ]
                }
            }
        ]
    }
    
    # Make the API request
    response = requests.post(api_url, headers=headers, json=job_payload)
    
    # Check if the request was successful
    if response.status_code == 200:
        response_data = response.json()
        job_id = response_data.get("job_id")
        print(f"Successfully created job with ID: {job_id}")
        return str(job_id)
    else:
        error_message = f"Failed to create job. Status code: {response.status_code}, Response: {response.text}"
        print(error_message)
        raise Exception(error_message)

if __name__ == "__main__":
    # Create config for environment specified in env vars
    env = os.environ.get("ENVIRONMENT", "pre-prod")
    print(f"Creating Databricks job for environment: {env}")
    
    # Load configuration
    config = Config(environment=env)
    
    # Print configuration details for debugging
    print(f"Databricks Host: {config.databricks_host}")
    print(f"Cluster ID: {config.databricks_cluster_id}")
    print(f"Azure Storage Container: {config.azure_storage_container_name}")
    
    try:
        # Create the job
        job_id = create_databricks_job(config)
        print(f"Created Databricks job with ID: {job_id}")
        
        # Save the job ID to a file for GitHub Actions
        with open("databricks_job_id.txt", "w") as f:
            f.write(job_id)
            
        print(f"Saved job ID to databricks_job_id.txt")
    except Exception as e:
        print(f"Error creating Databricks job: {str(e)}")
        exit(1) 