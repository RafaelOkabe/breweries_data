# Azure Data Engineering ETL Pipeline

This repository contains an Extract, Transform, Load (ETL) pipeline built on the Azure platform, demonstrating an end-to-end data flow using services such as Azure Data Factory, Azure Databricks, and Azure Storage.

## Project Overview

The goal of this project is to ingest raw data from a public API, transform and aggregate it to provide insights into breweries. The pipeline is designed to follow a data lake architecture with three layers: bronze, silver, and gold.

## Architecture

![Project Architecture](link_to_your_architecture_image_here.png)

The architecture consists of the following components:

* **Azure Data Factory (ADF):** Orchestrates the ETL pipeline, coordinating data movement and transformation.
* **Azure Databricks:** Executes PySpark notebooks to transform and aggregate data in the silver and gold layers.
* **Azure Storage:** Stores data in different layers of the data lake:
    * **Bronze:** Stores raw data in JSON format from the `https://api.openbrewerydb.org/breweries` API.
    * **Silver:** Stores transformed data in Parquet format, with improved data quality.
    * **Gold:** Stores aggregated data in Parquet format, ready for analysis.

## Data Flow

1.  **Extraction (Bronze):**
    * ADF extracts raw data from the Open Brewery DB API in JSON format and stores it in the bronze layer of Azure Storage.
2.  **Transformation (Silver):**
    * A Databricks notebook in PySpark reads the data from the bronze layer.
    * The data is transformed and cleaned, including handling missing values and ensuring the integrity of the phone, latitude, and longitude fields.
    * The transformed data is stored in the silver layer in Parquet format.
3.  **Aggregation (Gold):**
    * Another Databricks notebook in PySpark reads the data from the silver layer.
    * The data is aggregated to create a view that shows the count of breweries by type and location.
    * The aggregated data is stored in the gold layer in Parquet format.

## Technical Details

* **Azure Data Factory:**
    * Pipelines and activities for data extraction and orchestration.
    * Linked services to connect to Azure Storage and Azure Databricks.
* **Azure Databricks:**
    * PySpark notebooks for data transformation and aggregation.
    * Reading and writing data in Parquet format.
    * Handling missing data and ensuring data quality.
    * Aggregations to create summarized data views.
* **Azure Storage:**
    * Data lake organized in bronze, silver, and gold layers.
    * Storage of data in JSON and Parquet formats.

## How to Run

1.  **Prerequisites:**
    * An Azure subscription.
    * Configured Azure Data Factory, Azure Databricks, and Azure Storage instances.
    * Databricks notebooks and ADF pipelines imported into their respective instances.
    * Configure Data Factory Linked services to Databricks and the Storage Account.
2.  **Execution:**
    * Run the ADF pipeline to start the ETL data flow.
    * Monitor pipeline execution in ADF and Databricks jobs in the Databricks workspace.

## Repository Contents

* `adf/`: Azure Data Factory pipelines and linked services.
* `databricks/`: PySpark notebooks for data transformation and aggregation.
* `images/`: Architecture diagrams and other relevant images.
* `README.md`: This file.

## Contribution

Contributions are welcome! Feel free to submit pull requests to improve the pipeline or add new features.

## License

This project is licensed under the [MIT License](link_to_your_license_here).
