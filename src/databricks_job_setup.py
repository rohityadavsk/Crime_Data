from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
import os
from src.config import Config

def create_databricks_job(config: Config) -> str:
    """
    Create a Databricks job that runs the data processing pipeline.
    
    Args:
        config: Configuration object with Databricks and Azure Storage settings
    
    Returns:
        str: The created job ID
    """
    # Initialize Databricks client
    w = WorkspaceClient(
        host=config.databricks_host,
        token=config.databricks_token
    )

    # Define the job tasks
    tasks = [
        jobs.Task(
            task_key="read_data",
            python_wheel_task=jobs.PythonWheelTask(
                package_name="crime_data_processor",
                entry_point="read_data",
                named_parameters={
                    "input_path": config.input_path,
                    "AZURE_STORAGE_ACCOUNT_NAME": config.azure_storage_account_name,
                    "AZURE_STORAGE_ACCOUNT_KEY": config.azure_storage_account_key,
                    "AZURE_STORAGE_CONTAINER_NAME": config.azure_storage_container_name
                }
            ),
            depends_on=[],
            existing_cluster_id=config.databricks_cluster_id
        ),
        jobs.Task(
            task_key="process_data",
            python_wheel_task=jobs.PythonWheelTask(
                package_name="crime_data_processor",
                entry_point="process_data"
            ),
            depends_on=[jobs.TaskDependency(task_key="read_data")],
            existing_cluster_id=config.databricks_cluster_id
        ),
        jobs.Task(
            task_key="write_data",
            python_wheel_task=jobs.PythonWheelTask(
                package_name="crime_data_processor",
                entry_point="write_data",
                named_parameters={
                    "output_path": config.output_path,
                    "AZURE_STORAGE_ACCOUNT_NAME": config.azure_storage_account_name,
                    "AZURE_STORAGE_ACCOUNT_KEY": config.azure_storage_account_key,
                    "AZURE_STORAGE_CONTAINER_NAME": config.azure_storage_container_name
                }
            ),
            depends_on=[jobs.TaskDependency(task_key="process_data")],
            existing_cluster_id=config.databricks_cluster_id
        )
    ]

    # Create the job using the proper enum for format
    job = w.jobs.create(
        name=f"Crime Data Pipeline - {config.environment}",
        tasks=tasks,
        format=jobs.JobFormat.MULTI_TASK
    )

    return job.job_id

if __name__ == "__main__":
    # Create config for pre-prod environment
    config = Config(environment="pre-prod")
    
    # Create the job
    job_id = create_databricks_job(config)
    print(f"Created Databricks job with ID: {job_id}")
    
    # Save the job ID to a file for GitHub Actions
    with open("databricks_job_id.txt", "w") as f:
        f.write(job_id) 