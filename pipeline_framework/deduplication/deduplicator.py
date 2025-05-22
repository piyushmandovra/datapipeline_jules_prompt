# import pandas as pd # No longer needed for this module's primary function
import logging

# Configure logging for this module
logger = logging.getLogger(__name__)
if not logger.handlers: # Avoid adding multiple handlers if already configured
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s')

# -----------------------------------------------------------------------------
# Original pandas-based deduplicate_data is commented out
# as the module's focus shifts to generating Spark code.
# -----------------------------------------------------------------------------

# def deduplicate_data(df: pd.DataFrame, unique_columns: list[str], keep: str | bool = 'first') -> pd.DataFrame:
#     """
#     Removes duplicate rows from a pandas DataFrame based on specified columns.
#     ... (original docstring and code) ...
#     """
#     # ... (original code commented out) ...
#     pass


# -----------------------------------------------------------------------------
# New Functions for Generating Spark Deduplication Code
# -----------------------------------------------------------------------------

def _validate_params(params: dict, required_keys: list[str]):
    """Helper function to validate required keys in a dictionary."""
    missing_keys = [key for key in required_keys if key not in params or not params[key]]
    if missing_keys:
        err_msg = f"Missing or empty required parameters: {missing_keys}."
        logger.error(err_msg)
        raise ValueError(err_msg)

def generate_spark_deduplication_code(params: dict) -> str:
    """
    Generates a PySpark code snippet or a Spark SQL MERGE INTO statement for deduplication.

    This function is designed to produce code that can be executed within a
    Databricks environment or any Spark session.

    Args:
        params (dict): A dictionary containing parameters for deduplication.
            Common keys:
                - 'strategy' (str): The deduplication strategy.
                                    Supported: 'drop_duplicates', 'merge_into'.
            For 'drop_duplicates' strategy:
                - 'deduplication_keys' (list[str]): Columns to identify duplicates. (Required)
                - 'watermark_column' (str, optional): Column for watermarking in streaming.
                - 'watermark_delay_threshold' (str, optional): Delay for watermarking (e.g., "10 minutes").
                - 'input_dataframe_name' (str, optional): Name of the input DataFrame variable in PySpark. Defaults to 'df'.
            For 'merge_into' strategy (generates Spark SQL):
                - 'target_table_name' (str): Full name of the target Delta table. (Required)
                - 'source_view_name' (str): Name of the source view/DataFrame. (Required)
                - 'deduplication_keys' (list[str]): Columns for the ON clause of MERGE. (Required)
                - 'merge_condition_extras' (str, optional): Additional SQL for the ON clause (e.g., "AND source.is_latest = true").
                - 'when_matched_update_clause' (str, optional): SQL for WHEN MATCHED THEN UPDATE (e.g., "SET target.colA = source.colA").
                                                               If "NONE", no update occurs. If not provided, and an update is desired,
                                                               it must be explicitly defined.
                - 'when_not_matched_insert_clause' (str, optional): SQL for WHEN NOT MATCHED THEN INSERT (e.g., "INSERT (col1, col2) VALUES (source.col1, source.col2)").
                                                                   If "NONE", no insert occurs. If not provided, and an insert is desired,
                                                                   it must be explicitly defined (e.g. "INSERT *").
                - 'when_not_matched_by_source_delete_clause' (str, optional): SQL for WHEN NOT MATCHED BY SOURCE THEN DELETE (e.g., "DELETE").
                - 'when_not_matched_by_source_update_clause' (str, optional): SQL for WHEN NOT MATCHED BY SOURCE THEN UPDATE (e.g., "SET target.is_active = false").

    Returns:
        str: A string containing the generated PySpark code snippet or Spark SQL statement.

    Raises:
        ValueError: If 'strategy' is missing/invalid or if required parameters for the chosen strategy are missing.
    """
    logger.info(f"Generating Spark deduplication code with params: {params}")
    strategy = params.get('strategy')
    if not strategy:
        raise ValueError("Missing 'strategy' parameter. Must be 'drop_duplicates' or 'merge_into'.")

    if strategy == 'drop_duplicates':
        _validate_params(params, ['deduplication_keys'])
        keys_str = ", ".join([f"\"{key}\"" for key in params['deduplication_keys']])
        df_name = params.get('input_dataframe_name', 'df')
        
        code_lines = []
        if params.get('watermark_column') and params.get('watermark_delay_threshold'):
            code_lines.append(
                f"{df_name} = {df_name}.withWatermark(\"{params['watermark_column']}\", \"{params['watermark_delay_threshold']}\")"
            )
        code_lines.append(f"{df_name} = {df_name}.dropDuplicates([{keys_str}])")
        
        pyspark_snippet = "\n".join(code_lines)
        logger.info(f"Generated 'drop_duplicates' PySpark snippet:\n{pyspark_snippet}")
        return pyspark_snippet

    elif strategy == 'merge_into':
        _validate_params(params, ['target_table_name', 'source_view_name', 'deduplication_keys'])
        
        target_table = params['target_table_name']
        source_view = params['source_view_name']
        
        if not params['deduplication_keys']: # Ensure it's not an empty list
             raise ValueError("'deduplication_keys' cannot be empty for 'merge_into' strategy.")
        merge_on_conditions = " AND ".join([f"target.{key} = source.{key}" for key in params['deduplication_keys']])
        if params.get('merge_condition_extras'):
            merge_on_conditions += f" {params['merge_condition_extras']}"

        sql_merge = f"MERGE INTO {target_table} AS target\n"
        sql_merge += f"USING {source_view} AS source\n"
        sql_merge += f"ON {merge_on_conditions}\n"

        # WHEN MATCHED clause
        matched_update = params.get('when_matched_update_clause')
        if matched_update and matched_update.upper() != "NONE":
            sql_merge += f"WHEN MATCHED THEN {matched_update}\n"
        elif matched_update and matched_update.upper() == "NONE":
             logger.info("WHEN MATCHED clause explicitly set to NONE. No update will occur on match.")
        # If not provided at all, no WHEN MATCHED clause is added by default.

        # WHEN NOT MATCHED clause
        not_matched_insert = params.get('when_not_matched_insert_clause')
        if not_matched_insert and not_matched_insert.upper() != "NONE":
            sql_merge += f"WHEN NOT MATCHED THEN {not_matched_insert}\n"
        elif not_matched_insert and not_matched_insert.upper() == "NONE":
            logger.info("WHEN NOT MATCHED clause explicitly set to NONE. No insert will occur on no match.")
        # If not provided at all, no WHEN NOT MATCHED clause is added by default. (e.g., for an update-only merge)

        # WHEN NOT MATCHED BY SOURCE clauses (for SCD Type 2 style operations)
        not_matched_by_source_delete = params.get('when_not_matched_by_source_delete_clause')
        if not_matched_by_source_delete and not_matched_by_source_delete.upper() != "NONE":
            sql_merge += f"WHEN NOT MATCHED BY SOURCE THEN {not_matched_by_source_delete}\n"
        
        not_matched_by_source_update = params.get('when_not_matched_by_source_update_clause')
        if not_matched_by_source_update and not_matched_by_source_update.upper() != "NONE":
            sql_merge += f"WHEN NOT MATCHED BY SOURCE THEN {not_matched_by_source_update}\n"
            
        logger.info(f"Generated 'merge_into' Spark SQL statement:\n{sql_merge.strip()}")
        return sql_merge.strip()

    else:
        raise ValueError(f"Invalid 'strategy': {strategy}. Must be 'drop_duplicates' or 'merge_into'.")


def generate_merge_into_script(params: dict) -> str:
    """
    Generates a full PySpark script to perform a MERGE INTO operation.

    This is useful for batch jobs or within a forEachBatch micro-batch in streaming.
    The source data for the merge is assumed to be readable as a table/view.

    Args:
        params (dict): Parameters for the MERGE operation.
            Required keys:
                - 'target_table_name' (str): Full name of the target Delta table.
                - 'source_table_name' (str): Name of the source table from which a temporary view will be created.
                                             This temporary view name will be used as 'source_view_name' for SQL generation.
                - 'deduplication_keys' (list[str]): Columns for the ON clause.
            Optional keys (passed to generate_spark_deduplication_code for 'merge_into'):
                - 'merge_condition_extras' (str)
                - 'when_matched_update_clause' (str)
                - 'when_not_matched_insert_clause' (str)
                - 'when_not_matched_by_source_delete_clause' (str)
                - 'when_not_matched_by_source_update_clause' (str)
                - 'spark_app_name' (str, optional): Name for the Spark application. Defaults to "MergeInto_Pipeline".
                - 'extra_spark_configs' (dict, optional): Extra Spark configurations.
    Returns:
        str: A string containing the generated PySpark script.
    Raises:
        ValueError: If required parameters for script generation are missing.
    """
    logger.info(f"Generating full PySpark MERGE INTO script with params: {params}")
    
    # Validate params specific to this script generator
    _validate_params(params, ['target_table_name', 'source_table_name', 'deduplication_keys'])

    # Prepare params for the SQL generation part
    sql_gen_params = params.copy()
    sql_gen_params['strategy'] = 'merge_into'
    # The source_view_name for the SQL will be derived from source_table_name,
    # typically by creating a temporary view with a predictable name.
    # Let's use a convention like f"{params['source_table_name']}_vw" or make it configurable.
    source_view_name_for_sql = f"{params['source_table_name'].replace('.', '_')}_source_vw"
    sql_gen_params['source_view_name'] = source_view_name_for_sql
    
    merge_sql = generate_spark_deduplication_code(sql_gen_params)

    spark_app_name = params.get('spark_app_name', 'MergeInto_Pipeline_Script')
    extra_spark_configs_str = ""
    if 'extra_spark_configs' in params and isinstance(params['extra_spark_configs'], dict):
        for k, v in params['extra_spark_configs'].items():
            extra_spark_configs_str += f"    spark.conf.set(\"{k}\", \"{str(v)}\")\n"

    pyspark_script = f"""\"\"\"
Generated PySpark script for MERGE INTO operation.
This script reads a source table, creates a temporary view, and then
executes a MERGE INTO SQL statement against a target Delta table.
\"\"\"
import logging
from pyspark.sql import SparkSession

script_logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

script_logger.info("Starting PySpark MERGE INTO script.")
script_logger.info("Script Parameters:")
script_logger.info("  Source Table Name: {params['source_table_name']}")
script_logger.info("  Target Table Name: {params['target_table_name']}")
script_logger.info("  Source View for SQL: {source_view_name_for_sql}")
script_logger.info("  Deduplication Keys: {params['deduplication_keys']}")
script_logger.info("  Spark App Name: {spark_app_name}")

try:
    spark = (SparkSession.builder
             .appName("{spark_app_name}")
             .enableHiveSupport() # Often needed for table operations, esp. if using Hive Metastore
             .getOrCreate())
    script_logger.info("SparkSession initialized.")

    # Set any extra Spark configurations
{extra_spark_configs_str}

    # Create a temporary view from the source table
    # This makes the source data addressable in the MERGE SQL.
    # Ensure the user running this script has SELECT access to params['source_table_name'].
    spark.read.table("{params['source_table_name']}").createOrReplaceTempView("{source_view_name_for_sql}")
    script_logger.info(f"Temporary view '{source_view_name_for_sql}' created from table '{params['source_table_name']}'.")

    script_logger.info("Executing MERGE INTO SQL statement:")
    # Triple-quotes for the SQL block to handle multi-line SQL nicely
    merge_sql_statement = \"\"\"
{merge_sql}
\"\"\"
    script_logger.info(merge_sql_statement)
    
    spark.sql(merge_sql_statement)
    script_logger.info("MERGE INTO SQL statement executed successfully.")

except Exception as e:
    script_logger.error(f"An error occurred during the MERGE INTO script execution: {{str(e)}}", exc_info=True)
    raise
finally:
    # Clean up the temporary view
    spark.catalog.dropTempView("{source_view_name_for_sql}")
    script_logger.info(f"Temporary view '{source_view_name_for_sql}' dropped.")
    # spark.stop() # Usually not needed if run as a Databricks job; cluster lifecycle manages this.

script_logger.info("PySpark MERGE INTO script finished.")
"""
    return pyspark_script.strip()


if __name__ == '__main__':
    print("--- Demonstrating Spark Deduplication Code Generation ---")

    # 1. 'drop_duplicates' strategy
    print("\n--- Strategy: drop_duplicates (PySpark snippet) ---")
    drop_dup_params_simple = {
        'strategy': 'drop_duplicates',
        'deduplication_keys': ['user_id', 'event_type'],
        'input_dataframe_name': 'streaming_df'
    }
    print(f"Params: {drop_dup_params_simple}")
    print("Generated Code Snippet:")
    print(generate_spark_deduplication_code(drop_dup_params_simple))

    drop_dup_params_watermark = {
        'strategy': 'drop_duplicates',
        'deduplication_keys': ['session_id'],
        'watermark_column': 'event_timestamp',
        'watermark_delay_threshold': '30 minutes',
        # 'input_dataframe_name' defaults to 'df'
    }
    print(f"\nParams (with watermark): {drop_dup_params_watermark}")
    print("Generated Code Snippet:")
    print(generate_spark_deduplication_code(drop_dup_params_watermark))

    # 2. 'merge_into' strategy (Spark SQL statement)
    print("\n\n--- Strategy: merge_into (Spark SQL MERGE statement) ---")
    merge_sql_params = {
        'strategy': 'merge_into',
        'target_table_name': 'silver.users_deduplicated',
        'source_view_name': 'new_users_microbatch_vw',
        'deduplication_keys': ['user_id'],
        'merge_condition_extras': "AND source.registration_date > '2023-01-01'",
        'when_matched_update_clause': "SET target.last_seen = source.event_time, target.email = source.email",
        'when_not_matched_insert_clause': "INSERT (user_id, email, registration_date, last_seen) VALUES (source.user_id, source.email, source.registration_date, source.event_time)"
    }
    print(f"Params: {merge_sql_params}")
    print("Generated SQL Statement:")
    print(generate_spark_deduplication_code(merge_sql_params))

    merge_sql_params_no_update = {
        'strategy': 'merge_into',
        'target_table_name': 'logs.events_archive',
        'source_view_name': 'daily_events_vw',
        'deduplication_keys': ['event_id'],
        'when_matched_update_clause': "NONE", # Explicitly no update
        'when_not_matched_insert_clause': "INSERT *"
    }
    print(f"\nParams (no update on match): {merge_sql_params_no_update}")
    print("Generated SQL Statement:")
    print(generate_spark_deduplication_code(merge_sql_params_no_update))
    
    merge_sql_scd2_params = {
        'strategy': 'merge_into',
        'target_table_name': 'dim.customers_scd2',
        'source_view_name': 'customer_updates_vw',
        'deduplication_keys': ['customer_nk'], # Natural Key
        'merge_condition_extras': "AND target.is_current = true", # Only match current records in target
        'when_matched_update_clause': "SET target.is_current = false, target.valid_to_ts = source.update_ts", # Expire old record
        # Insert for new records, or new versions of existing records (handled by separate insert logic usually, or more complex merge)
        # This example focuses on expiring matched records. A full SCD2 often involves an INSERT for the new version.
        # A more complete SCD2 might require multiple MERGE statements or more complex logic.
        # For simplicity, we'll assume the new/updated record is inserted separately or this merge is part of a larger process.
        # 'when_not_matched_insert_clause': "INSERT (customer_nk, ..., is_current, valid_from_ts) VALUES (source.customer_nk, ..., true, source.update_ts)",
        'when_not_matched_by_source_delete_clause': "NONE", # Or "DELETE" if applicable
        'when_not_matched_by_source_update_clause': "SET target.is_current = false, target.status = 'archived'" # Example if source is authoritative
    }
    print(f"\nParams (SCD2 style example): {merge_sql_scd2_params}")
    print("Generated SQL Statement:")
    print(generate_spark_deduplication_code(merge_sql_scd2_params))


    # 3. Full PySpark script for 'merge_into'
    print("\n\n--- Full PySpark Script for 'merge_into' ---")
    full_script_params = {
        # Params for generate_merge_into_script itself
        'source_table_name': 'bronze.user_updates_daily',
        'target_table_name': 'silver.users_deduplicated',
        'deduplication_keys': ['user_id'],
        'spark_app_name': 'DailyUserDeduplication',
        'extra_spark_configs': {"spark.databricks.delta.optimizeWrite.enabled": "true"},
        # Params passed through to generate_spark_deduplication_code for SQL
        'when_matched_update_clause': "SET target.last_login = source.last_login, target.country_code = source.country_code",
        'when_not_matched_insert_clause': "INSERT (user_id, first_login, last_login, country_code, status) VALUES (source.user_id, source.first_login, source.last_login, source.country_code, 'active')"
    }
    print(f"Params: {full_script_params}")
    print("Generated PySpark Script:")
    print(generate_merge_into_script(full_script_params))

    # 4. Test error handling for missing parameters
    print("\n\n--- Testing error handling for missing parameters ---")
    try:
        print("\nAttempting 'drop_duplicates' with missing 'deduplication_keys':")
        generate_spark_deduplication_code({'strategy': 'drop_duplicates'})
    except ValueError as e:
        print(f"Caught expected error: {e}")

    try:
        print("\nAttempting 'merge_into' with missing 'target_table_name':")
        generate_spark_deduplication_code({
            'strategy': 'merge_into', 
            'source_view_name': 's_view', 
            'deduplication_keys': ['id']
        })
    except ValueError as e:
        print(f"Caught expected error: {e}")

    try:
        print("\nAttempting full 'merge_into' script with missing 'source_table_name':")
        generate_merge_into_script({
            'target_table_name': 't_table', 
            'deduplication_keys': ['id']
        })
    except ValueError as e:
        print(f"Caught expected error: {e}")

    print("\n\nSpark deduplication code generation demonstration finished.")
