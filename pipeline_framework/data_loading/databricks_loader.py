import json
import logging
# import pandas as pd # No longer needed for this script's primary function
import os

# Configure logging for this module
logger = logging.getLogger(__name__)
if not logger.handlers: # Avoid adding multiple handlers if already configured
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s')

# -----------------------------------------------------------------------------
# Original prepare_for_autoloader and DatabricksAPIClient are commented out
# as the module's focus shifts to generating a PySpark script.
# -----------------------------------------------------------------------------

# def prepare_for_autoloader(data, target_path: str, file_format: str = "json") -> str:
#     """
#     Prepares data for Databricks Autoloader by formatting it and simulating
#     writing it to a target cloud storage path.
#     ... (original docstring and code) ...
#     """
#     # ... (original code commented out) ...
#     pass

# class DatabricksAPIClient:
#     """
#     Placeholder for a client to interact with Databricks APIs.
#     ... (original docstring and code) ...
#     """
#     # ... (original code commented out) ...
#     pass


# -----------------------------------------------------------------------------
# New Function: generate_autoloader_script
# -----------------------------------------------------------------------------

def generate_autoloader_script(params: dict) -> str:
    """
    Generates a PySpark script for Databricks Autoloader based on input parameters.

    This script is intended to be executed on a Databricks cluster, typically
    orchestrated by an Airflow DAG using a Databricks operator (e.g., DatabricksSubmitRunOperator).

    Args:
        params (dict): A dictionary containing parameters for the Autoloader script.
            Required keys:
                - 'input_path' (str): Path to the input data (e.g., "s3://bucket/landing/").
                - 'input_format' (str): Format of the input data (e.g., "json", "csv", "parquet").
                - 'output_delta_table_name' (str): Full name of the target Delta table (e.g., "bronze.my_data").
                - 'schema_location' (str): Path for schema inference and evolution.
                - 'checkpoint_location' (str): Path for checkpointing the stream.
            Optional keys:
                - 'file_format_options' (dict): Additional options for the input format (e.g., {"header": "true"} for CSV).
                - 'table_properties' (dict): Properties to set on the target Delta table.
                - 'trigger_type' (str): Streaming trigger type. Can be "availableNow" (default) or "processingTime".
                - 'trigger_interval' (str): Interval for processingTime trigger (e.g., "1 minute").
                - 'output_mode' (str): Output mode for the stream (e.g., "append" (default), "complete").
                - 'spark_app_name' (str): Name for the Spark application. Defaults to "Autoloader_Pipeline".
                - 'extra_spark_configs' (dict): Extra Spark configurations to set at the start of the script.

    Returns:
        str: A string containing the generated PySpark script.

    Raises:
        ValueError: If any of the required parameters are missing.
    """
    logger.info(f"Generating Autoloader PySpark script with parameters: {params}")

    required_keys = [
        'input_path', 'input_format', 'output_delta_table_name',
        'schema_location', 'checkpoint_location'
    ]
    for key in required_keys:
        if key not in params:
            err_msg = f"Missing required parameter '{key}' in params for generate_autoloader_script."
            logger.error(err_msg)
            raise ValueError(err_msg)

    # Build file_format_options string
    file_format_options_str = ""
    if 'file_format_options' in params and isinstance(params['file_format_options'], dict):
        for k, v in params['file_format_options'].items():
            file_format_options_str += f".option(\"{k}\", \"{str(v)}\")" # Ensure value is stringified

    # Build table_properties string
    table_properties_str = ""
    if 'table_properties' in params and isinstance(params['table_properties'], dict):
        tbl_props_list = [f"'{k}' = '{v}'" for k, v in params['table_properties'].items()]
        table_properties_str = f".tableProperty({', '.join(tbl_props_list)})" # This is not standard for .toTable, usually part of CREATE TABLE
                                                                                # For .toTable, properties are usually set before or via DeltaTableBuilder
                                                                                # For simplicity, this example won't directly use .tableProperty in writeStream
                                                                                # but it's good to capture. It might be used in a separate CREATE TABLE IF NOT EXISTS step.
        # Correct approach for Delta table properties is often with DeltaTableBuilder or SQL,
        # for .toTable(), it creates if not exists but specific properties are harder to set this way.
        # For now, we'll log a warning if table_properties are provided, as direct .toTable() doesn't support them.
        if params['table_properties']:
            logger.warning("The 'table_properties' parameter is captured but not directly applied via '.toTable()' in this basic script. "
                           "For table properties, consider using DeltaTable.createIfNotExists() or SQL DDL commands separately.")


    # Build trigger string
    trigger_type = params.get('trigger_type', 'availableNow')
    if trigger_type == 'processingTime':
        trigger_interval = params.get('trigger_interval', '1 minute')
        trigger_str = f".trigger(processingTime='{trigger_interval}')"
    elif trigger_type == 'availableNow':
        trigger_str = ".trigger(availableNow=True)"
    elif trigger_type == 'once':
        trigger_str = ".trigger(once=True)" # Common alternative to availableNow for batch-like streaming
    else:
        logger.warning(f"Unsupported trigger_type: '{trigger_type}'. Defaulting to 'availableNow=True'.")
        trigger_str = ".trigger(availableNow=True)"
        
    output_mode = params.get('output_mode', 'append')
    spark_app_name = params.get('spark_app_name', 'Autoloader_Pipeline')

    # Build extra_spark_configs string
    extra_spark_configs_str = ""
    if 'extra_spark_configs' in params and isinstance(params['extra_spark_configs'], dict):
        for k, v in params['extra_spark_configs'].items():
            extra_spark_configs_str += f"    spark.conf.set(\"{k}\", \"{str(v)}\")\n"


    # Using an f-string for the PySpark script template
    pyspark_script = f"""\"\"\"
Generated PySpark script for Databricks Autoloader.
This script processes data from an input path using Autoloader and writes
it to a Delta table. It is designed to be run on a Databricks cluster.
\"\"\"
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import * # Common practice, but can specify functions if preferred

# Configure Python logging within the PySpark script
script_logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

script_logger.info("Starting Autoloader PySpark script.")
script_logger.info("Script Parameters:")
script_logger.info("  Input Path: {params['input_path']}")
script_logger.info("  Input Format: {params['input_format']}")
script_logger.info("  Output Delta Table: {params['output_delta_table_name']}")
script_logger.info("  Schema Location: {params['schema_location']}")
script_logger.info("  Checkpoint Location: {params['checkpoint_location']}")
script_logger.info("  File Format Options: {params.get('file_format_options', {{}})}")
script_logger.info("  Trigger: {trigger_type} (Interval: {params.get('trigger_interval', 'N/A' if trigger_type != 'processingTime' else '1 minute')})")
script_logger.info("  Output Mode: {output_mode}")
script_logger.info("  Spark App Name: {spark_app_name}")

try:
    # Initialize SparkSession
    spark = (SparkSession.builder
             .appName("{spark_app_name}")
             .getOrCreate())

    script_logger.info("SparkSession initialized.")

    # Set any extra Spark configurations
{extra_spark_configs_str}

    # Define the readStream for Autoloader
    df_read = (spark.readStream
               .format("cloudFiles")
               .option("cloudFiles.format", "{params['input_format']}")
               .option("cloudFiles.schemaLocation", "{params['schema_location']}")
               {file_format_options_str}  # Additional file format options
               .load("{params['input_path']}")
    )

    script_logger.info(f"Read stream configured for input path: {params['input_path']}")

    # Define the writeStream to the Delta table
    query = (df_read.writeStream
             .format("delta")
             .outputMode("{output_mode}")
             .option("checkpointLocation", "{params['checkpoint_location']}")
             {trigger_str}
             # .toTable("{params['output_delta_table_name']}") # Using .table() is more robust for creating if not exists
             .table("{params['output_delta_table_name']}")
    )
    
    # Note: If using .toTable(), it implicitly waits for the stream to complete if trigger is availableNow=True or once=True.
    # For continuous streams or if explicit waiting is needed for other reasons:
    # query.awaitTermination() # This line would make the script run indefinitely for continuous triggers.
    # For availableNow=True, .table() or .toTable() often handles the execution lifecycle.
    # If this script is submitted as a job, the job will complete when the availableNow/once trigger finishes.
    
    script_logger.info(f"Write stream configured for Delta table: {params['output_delta_table_name']}.")
    script_logger.info("Autoloader script setup complete. Stream processing will start based on the trigger.")
    # For availableNow=True, the processing happens and the query object returned by .table() effectively completes.
    # If there's a need to ensure the job doesn't exit until data is processed for availableNow,
    # sometimes a simple `query.status` check or a short `awaitTermination(timeout_ms)` might be added,
    # but often it's managed by the Databricks job runner itself for batch-like streams.
    
    # Example: if trigger is availableNow, the actual processing and writing happens when this statement is executed.
    # No explicit awaitTermination is strictly needed for the job to process data then complete.

except Exception as e:
    script_logger.error(f"An error occurred during the Autoloader script execution: {{str(e)}}", exc_info=True)
    raise # Re-raise the exception to mark the Databricks job as failed

script_logger.info("Autoloader PySpark script finished (or stream started for continuous triggers).")

"""
    return pyspark_script.strip()


if __name__ == '__main__':
    print("--- Demonstrating Autoloader PySpark Script Generation ---")

    # 1. Sample parameters for the script
    sample_autoloader_params = {
        'input_path': "s3://my-landing-bucket/raw_data/json_files/",
        'input_format': "json", # e.g., json, csv, parquet, text, binaryFile
        'output_delta_table_name': "bronze_layer.autoloader_target_table",
        'schema_location': "s3://my-databricks-configs/schemas/autoloader_target_table_schema",
        'checkpoint_location': "s3://my-databricks-configs/checkpoints/autoloader_target_table_checkpoint",
        'file_format_options': {
            "multiline": "true", # Example for JSON
            "inferSchema": "false" # Often recommended to set to false and rely on schema evolution
        },
        'table_properties': { # Note: These are not directly applied by .toTable() in the basic script.
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true"
        },
        'trigger_type': "availableNow", # "availableNow", "processingTime", "once"
        # 'trigger_interval': "5 minutes", # Only if trigger_type is "processingTime"
        'output_mode': "append",
        'spark_app_name': "MyCompany_Autoloader_JSON_to_Bronze",
        'extra_spark_configs': {
            "spark.databricks.delta.schema.autoMerge.enabled": "true"
        }
    }

    print("\n--- Generating script with sample parameters: ---")
    try:
        generated_script = generate_autoloader_script(sample_autoloader_params)
        print("\nGenerated PySpark Script:\n")
        print(generated_script)
    except ValueError as e:
        print(f"\nError generating script: {e}")

    # 2. Test with missing essential parameters
    print("\n\n--- Generating script with missing 'input_path': ---")
    incomplete_params = sample_autoloader_params.copy()
    del incomplete_params['input_path']
    try:
        generate_autoloader_script(incomplete_params)
    except ValueError as e:
        print(f"Caught expected error: {e}")

    print("\n\n--- Generating script with CSV parameters: ---")
    csv_params = {
        'input_path': "s3://my-landing-bucket/raw_data/csv_files/",
        'input_format': "csv",
        'output_delta_table_name': "bronze_layer.autoloader_csv_table",
        'schema_location': "s3://my-databricks-configs/schemas/autoloader_csv_table_schema",
        'checkpoint_location': "s3://my-databricks-configs/checkpoints/autoloader_csv_table_checkpoint",
        'file_format_options': {
            "header": "true",
            "delimiter": ",",
            "inferSchema": "true" # For CSV, inferring schema might be okay for initial setup
        },
        'trigger_type': "processingTime",
        'trigger_interval': "10 minutes",
        'extra_spark_configs': {
            "spark.databricks.io.cache.enabled": "true"
        }
    }
    try:
        generated_csv_script = generate_autoloader_script(csv_params)
        print("\nGenerated PySpark Script for CSV:\n")
        print(generated_csv_script)
    except ValueError as e:
        print(f"\nError generating CSV script: {e}")


    print("\n\nDatabricks Autoloader script generation demonstration finished.")
