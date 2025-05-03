from setuptools import setup, find_packages

setup(
    name="crime_data_processor",
    version="0.1.0",
    author="Rohit",
    description="A package for processing crime data using PySpark and Delta Lake",
    packages=find_packages(),
    package_data={
        "crime_data_processor": ["*.py"],
        "crime_data_processor.databricks_scripts": ["*.py"],
    },
    install_requires=[
        "pyspark>=3.3.0",
        "delta-spark>=3.3.1",
        "pytest==7.4.3",
        "pytest-cov==4.1.0",
        "pandas==2.1.4",
        "azure-storage-file-datalake==12.14.0",
        "azure-identity==1.15.0",
        "python-dotenv==1.0.0",
        "pyspark-test==0.1.0",
        "databricks-sdk==0.12.0"
    ],
    entry_points={
        "console_scripts": [
            "read_data=src.databricks_scripts.read_data:main",
            "process_data=src.databricks_scripts.process_data:main",
            "write_data=src.databricks_scripts.write_data:main",
        ],
    },
    python_requires=">=3.8",
    author_email="rohityadavsk@gmail.com",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/rohityadavsk/crime_data_processor",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    # Prevent egg-info creation
    zip_safe=False,
    include_package_data=True,
) 