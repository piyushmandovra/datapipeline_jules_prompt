# Python Pipeline Framework with Databricks Integration

This project provides a modular framework for building data processing pipelines. Initial data ingestion and decryption are handled by Python components orchestrated by Airflow. Core data processing tasks, such as data loading (via Autoloader) and deduplication, are performed using **Spark on Databricks**, with Airflow triggering these Databricks jobs. The framework includes utilities for logging, configuration management, and custom exceptions.

## Modules

-   **`data_ingestion`**: Handles incoming data (e.g., via a Flask webhook listener).
-   **`decryption`**: Decrypts data using Fernet symmetric encryption.
-   **`transformations`**: (Primarily for pre-Databricks processing) Applies a series of configurable transformations to pandas DataFrames. Can be used for initial data cleaning or preparation before staging for Databricks.
-   **`deduplication`**: **Generates PySpark/Spark SQL code for data deduplication.** This includes strategies like `dropDuplicates` (for simple duplicate removal, potentially with watermarking for streaming) and Spark SQL `MERGE INTO` statements for more complex upsert/SCD2 logic in Delta Lake.
-   **`data_loading`**: **Generates PySpark scripts for Databricks Autoloader.** This allows for efficient, incremental data ingestion from cloud storage (e.g., S3) into Delta Lake tables on Databricks.
-   **`utils`**: Contains shared utilities:
    -   `logger.py`: For setting up application-wide logging.
    -   `custom_exceptions.py`: Defines custom exception classes for the framework.
    -   `config_manager.py`: For loading configurations from JSON files.
-   **`airflow_dags`**: Contains the sample Airflow DAG (`sample_pipeline_dag.py`) and its primary configuration file (`pipeline_config.json`).

## Airflow Integration with Databricks

The pipeline is orchestrated by Apache Airflow, with a significant shift towards leveraging Databricks for heavy-duty data processing.

-   **Databricks Operators**: The sample DAG (`sample_pipeline_dag.py`) uses the `DatabricksSubmitRunOperator` from the `apache-airflow-providers-databricks` package to trigger jobs on Databricks.
-   **Dynamic Script Generation**: Python scripts for Autoloader and Deduplication (Spark SQL MERGE INTO) are generated dynamically by Airflow tasks using the `data_loading` and `deduplication` modules, respectively.
-   **Generic Databricks Notebook Runner**:
    -   These dynamically generated PySpark scripts are passed as parameters to a generic Databricks notebook (e.g., `generic_pyspark_runner.ipynb` - **user must create this notebook in their Databricks workspace**).
    -   The notebook is responsible for executing the received Python script content.
    -   **Example `generic_pyspark_runner.ipynb` content**:
        ```python
        # generic_pyspark_runner.ipynb

        # Define widgets to accept parameters from Airflow
        dbutils.widgets.text("python_script_content", "", "Python Script Content")
        dbutils.widgets.text("script_target_description", "", "Script Target Description")

        # Retrieve parameters
        script_to_execute = dbutils.widgets.get("python_script_content")
        description = dbutils.widgets.get("script_target_description")

        print(f"Executing script for: {description}")
        print("--- Script Content ---")
        print(script_to_execute)
        print("--- End Script Content ---")

        # Execute the script
        if script_to_execute:
            exec(script_to_execute)
            print(f"Successfully executed script for: {description}")
        else:
            print("No script content provided.")
            dbutils.notebook.exit("No script content provided to execute.")
        ```
    -   The path to this generic runner notebook is specified in `pipeline_config.json` under `airflow_databricks_config.databricks_notebook_path`.

## Configuration (`pipeline_config.json`)

The primary configuration for the Airflow DAG and its Databricks jobs is managed through `pipeline_framework/airflow_dags/pipeline_config.json`.

Key sections in `pipeline_config.json`:

-   **`general`**:
    -   `pipeline_name`: Name of the pipeline (used as DAG ID).
    -   `fernet_key_airflow_variable_name`: Name of the Airflow Variable storing the Fernet encryption key.
-   **`airflow_databricks_config`**:
    -   `databricks_conn_id`: **Crucial**. The Airflow connection ID for your Databricks workspace. **Users must create this connection in Airflow.**
    -   `databricks_job_cluster_key`: ID of an existing Databricks job cluster to use. If not provided, a `new_cluster` configuration (example provided in config) should be defined for the operator to create a new job cluster.
    -   `databricks_notebook_path`: Path to the generic PySpark runner notebook in your Databricks workspace.
-   **`data_staging_config`**:
    -   `staging_area_path_template`: Template for the cloud storage path (e.g., S3) where decrypted data is staged before Autoloader processing. Supports `{pipeline_name}` and `{{ dag_run.id }}` placeholders.
-   **`autoloader_job_params`**:
    -   Parameters for the Autoloader PySpark script generation (e.g., `spark_app_name`, `input_path_suffix` relative to staging, `input_format`, `output_delta_table_name`, paths for `schema_location_root` and `checkpoint_location_root`, `trigger_type`).
-   **`deduplication_job_params`**:
    -   Parameters for the deduplication PySpark script generation.
    -   `strategy`: Currently supports `merge_into`.
    -   `merge_into_config`: Contains details for the `MERGE INTO` operation (e.g., `source_table_name` (often the Autoloader's output), `target_table_name`, `deduplication_keys`, and SQL clauses for `WHEN MATCHED`, `WHEN NOT MATCHED`).

## Setup and Usage

1.  **Prerequisites**:
    -   Python 3.8+
    -   Apache Airflow environment with `apache-airflow-providers-databricks>=5.0.0` installed.
    -   A Databricks workspace.
    -   Cloud storage (e.g., AWS S3, Azure ADLS Gen2, GCS) accessible by Databricks.
2.  **Airflow Configuration**:
    -   Set up a Databricks connection in Airflow. The ID of this connection must match `databricks_conn_id` in `pipeline_config.json`.
    -   Create an Airflow Variable to store the Fernet encryption key. The name of this variable must match `fernet_key_airflow_variable_name` in `pipeline_config.json`.
3.  **Databricks Workspace Setup**:
    -   Create the generic PySpark runner notebook (e.g., `generic_pyspark_runner.ipynb` as shown above) in your Databricks workspace. Update `databricks_notebook_path` in `pipeline_config.json` accordingly.
    -   Ensure your Databricks cluster (either existing job cluster or new cluster definition) has necessary permissions and libraries (if any beyond standard Spark).
4.  **Pipeline Configuration**:
    -   Customize `pipeline_framework/airflow_dags/pipeline_config.json` with your specific paths, table names, cluster settings, etc. Replace placeholders like `YOUR_BUCKET_NAME`.
5.  **Deploy to Airflow**: Place the `pipeline_framework` directory in Airflow's `PYTHONPATH` (or install it as a package) and ensure the `airflow_dags` directory is discoverable by Airflow.
6.  **Trigger the DAG**: Manually trigger the `DataProcessingPipeline_v2_Configured` (or your configured `pipeline_name`) DAG in the Airflow UI.

### Workflow Overview

1.  **Data Ingestion (Simulated)**: The DAG starts, and an initial task simulates receiving data.
2.  **Decryption (Airflow Worker)**: The Fernet key is fetched from Airflow Variables, and the data is decrypted by a PythonOperator running on an Airflow worker.
3.  **Staging (Airflow Worker/Cloud Storage)**: The decrypted data (as a JSON string) is "staged" to a cloud storage path specified in the configuration. The current DAG simulates this by writing to a local temp file but logs the intended cloud path.
4.  **Autoloader Job (Databricks)**:
    -   A `PythonOperator` generates a PySpark script for Databricks Autoloader using parameters from `pipeline_config.json` and the staging path from XComs.
    -   `DatabricksSubmitRunOperator` triggers a job on Databricks, passing the generated script to the generic runner notebook.
    -   Autoloader picks up data from the staged cloud path and ingests it into a Delta table (e.g., `bronze_data_raw`).
5.  **Deduplication Job (Databricks)**:
    -   A `PythonOperator` generates a PySpark script (typically a `MERGE INTO` SQL statement) for deduplication, using the output of the Autoloader job as its source and parameters from `pipeline_config.json`.
    -   `DatabricksSubmitRunOperator` triggers another job on Databricks to execute this script.
    -   Data is deduplicated into a target Delta table (e.g., `silver_data_deduplicated`).

## Parameterization and Secure Management

-   **Pipeline Logic**: Key parameters for Spark jobs (Autoloader paths, table names, deduplication keys, merge conditions) are externalized to `pipeline_config.json`.
-   **Sensitive Values**: Decryption keys and other sensitive information should be managed using Airflow Variables (backed by a secure secrets backend in production) or environment variables, not hardcoded. The configuration file stores *references* (like Airflow Variable names) to these secrets.
-   **Databricks Connection**: Connection details for Databricks are managed securely within Airflow's connection settings.

This architecture allows for flexible and secure orchestration of PySpark jobs on Databricks, leveraging Airflow's scheduling and dependency management capabilities.
