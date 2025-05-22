import json
import os
import logging

# Get a logger for this module
logger = logging.getLogger(__name__)
# Basic configuration for the logger if no other logging is set up yet.
# This is important if this module is used standalone or before global logging setup.
if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s')


def load_config(config_path: str = "config.json") -> dict:
    """
    Loads configuration from a JSON file.

    Args:
        config_path (str, optional): Path to the JSON configuration file.
                                     Defaults to "config.json".

    Returns:
        dict: A dictionary containing the configuration. Returns an empty
              dict if the file is not found or if there's an error parsing JSON.

    Note:
        For more advanced scenarios, consider:
        - Default values for missing keys.
        - Environment variable overrides for specific settings.
        - Support for other formats like YAML (would require PyYAML dependency).
        - Schema validation for the configuration.
    """
    config = {}
    try:
        if not os.path.exists(config_path):
            logger.warning(f"Configuration file not found: {config_path}. Returning empty configuration.")
            return {}

        with open(config_path, 'r') as f:
            config = json.load(f)
        logger.info(f"Configuration loaded successfully from {config_path}.")
    except FileNotFoundError: # Should be caught by os.path.exists, but as a safeguard
        logger.warning(f"Configuration file not found (FileNotFoundError): {config_path}. Returning empty configuration.")
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from {config_path}: {e}. Returning empty configuration.")
        return {}
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading config from {config_path}: {e}. Returning empty configuration.")
        return {}
    
    return config

if __name__ == '__main__':
    print("--- Demonstrating Configuration Manager ---")

    # Define a sample configuration with the new structure
    new_structure_config_data = {
        "_comment_main": "Main configuration for the data processing pipeline orchestrated by Airflow.",
        "general": {
            "pipeline_name": "DataProcessingPipeline_v2_Configured",
            "environment": "development",
            "_comment_fernet_key": "The Fernet key for data decryption should be stored as an Airflow Variable for security." ,
            "fernet_key_airflow_variable_name": "pipeline_framework_fernet_key"
        },
        "airflow_databricks_config": {
            "_comment": "Configuration for Airflow's Databricks connection and job submission.",
            "databricks_conn_id": "databricks_default",
            "databricks_job_cluster_key": "YOUR_DATABRICKS_JOB_CLUSTER_KEY_OR_LEAVE_FOR_NEW_CLUSTER"
        },
        "data_staging_config": {
            "_comment": "Configuration for the initial staging area for decrypted data, before Autoloader processing.",
            "staging_area_path_template": "s3://YOUR_BUCKET_NAME/pipeline_staging/{pipeline_name}/run_{{ dag_run.id }}/"
        },
        "autoloader_job_params": {
            "_comment": "Parameters for generating the Databricks Autoloader PySpark script.",
            "spark_app_name": "Autoloader_Job_for_{{ dag_run.id }}",
            "input_path_suffix": "decrypted_data_for_autoloader/",
            "input_format": "json",
            "output_delta_table_name": "YOUR_DATABASE.bronze_data_raw",
            "schema_location_root": "s3://YOUR_BUCKET_NAME/databricks_pipeline_metadata/schemas/",
            "checkpoint_location_root": "s3://YOUR_BUCKET_NAME/databricks_pipeline_metadata/checkpoints/"
        },
        "deduplication_job_params": {
            "_comment": "Parameters for the deduplication job.",
            "strategy": "merge_into",
            "spark_app_name": "Deduplication_Job_for_{{ dag_run.id }}",
            "merge_into_config": {
                "source_table_name": "YOUR_DATABASE.bronze_data_raw",
                "target_table_name": "YOUR_DATABASE.silver_data_deduplicated",
                "deduplication_keys": ["primary_key_column_1"]
            }
        }
    }
    temp_config_filename = "temp_new_config.json"

    # 1. Programmatically create a temporary config file with the new structure
    print(f"\n1. Creating temporary '{temp_config_filename}' with new structure...")
    try:
        with open(temp_config_filename, 'w') as f:
            json.dump(new_structure_config_data, f, indent=4)
        print(f"'{temp_config_filename}' created successfully.")
    except Exception as e:
        print(f"Error creating '{temp_config_filename}': {e}")
        # If file creation fails, the next step will also fail, which is acceptable for a demo.

    # 2. Load the created configuration file
    print(f"\n2. Loading configuration from '{temp_config_filename}'...")
    loaded_config = load_config(temp_config_filename)
    if loaded_config:
        print("Loaded configuration (new structure):")
        # Print a few items to verify structure
        print(f"  Pipeline Name: {loaded_config.get('general', {}).get('pipeline_name')}")
        print(f"  Databricks Conn ID: {loaded_config.get('airflow_databricks_config', {}).get('databricks_conn_id')}")
        print(f"  Autoloader Input Format: {loaded_config.get('autoloader_job_params', {}).get('input_format')}")
        print(f"  Deduplication Strategy: {loaded_config.get('deduplication_job_params', {}).get('strategy')}")
        print(f"  Full loaded config keys: {list(loaded_config.keys())}")
        print(f"  Autoloader params keys: {list(loaded_config.get('autoloader_job_params', {}).keys())}")
    else:
        print("Failed to load configuration or configuration was empty.")

    # 3. Test loading a non-existent config file (remains useful)
    print("\n3. Testing loading a non-existent config file ('non_existent.json')...")
    non_existent_config = load_config("non_existent.json")
    if not non_existent_config: # An empty dict is falsy
        print("Successfully handled non-existent config file (returned empty dict as expected).")
    else:
        print(f"Unexpectedly loaded something from non_existent.json: {non_existent_config}")

    # Clean up the temporary config file
    if os.path.exists(temp_config_filename):
        try:
            os.remove(temp_config_filename)
            print(f"\nCleaned up '{temp_config_filename}'.")
        except Exception as e:
            print(f"Error cleaning up '{temp_config_filename}': {e}")

    print("\nConfiguration Manager demonstration finished.")
