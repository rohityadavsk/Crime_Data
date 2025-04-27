from setuptools import setup, find_packages

setup(
    name="crime_data_processor",
    version="0.1",
    author="Rohit",
    description="A package for processing crime data using PySpark and Delta Lake",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.5.3",
        "delta-spark>=3.3.1",
        "pytest>=7.0.0",
        "azure-storage-file-datalake>=12.0.0",
        "azure-identity>=1.0.0",
        "python-dotenv>=1.0.0",
    ],
    python_requires=">=3.6",
    author_email="your.email@example.com",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/crime_data_processor",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    # Prevent egg-info creation
    zip_safe=False,
    include_package_data=True,
) 