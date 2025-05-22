import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
import tempfile # For local staging simulation

# Airflow specific imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.models import Variable

# Pipeline framework imports
from pipeline_framework.utils.config_manager import load_config
from pipeline_framework.utils.custom_exceptions import ConfigurationError
from pipeline_framework.decryption.decryptor import Decryptor
# Corrected imports for script generators:
from pipeline_framework.data_loading.databricks_loader import generate_autoloader_script
from pipeline_framework.deduplication.deduplicator import generate_merge_into_script, generate_spark_deduplication_code

from cryptography.fernet import Fernet

# Configure basic logging for the DAG file itself
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --- 0. Configuration Loading & Initial Setup ---
DAG_FILE_DIR = Path(__file__).parent
DEFAULT_CONFIG_PATH = DAG_FILE_DIR / "pipeline_config.json"
pipeline_config = load_config(config_path=str(DEFAULT_CONFIG_PATH))

if not pipeline_config:
    logger.error(f"CRITICAL: Failed to load pipeline configuration from {DEFAULT_CONFIG_PATH}. DAG cannot operate.")
    pipeline_config = {} # Ensure it's a dict to prevent KeyErrors during parsing, but tasks will fail.

# Sample data (original, will be encrypted within a task)
# This data structure matches what ENCRYPTED_SAMPLE_DATA used to hold after encryption.
ORIGINAL_SAMPLE_DATA_FOR_ENCRYPTION = [
    {"id": 1, "name": "Alice", "value": 120, "secret_code": "AX123"},
    {"id": 1, "name": "Alice", "value": 120, "secret_code": "AX123"}, # Duplicate
    {"id": 2, "name": "Bob", "value": 150, "secret_code": "BY456"},
    {"id": 3, "name": "Charlie", "value": 90, "secret_code": "CZ789"},
    {"id": 4, "name": "Alicia", "value": 200, "secret_code": "AX123"},
]

# --- Helper Function for Databricks Notebook Parameters ---
def get_db_notebook_params(script_content_str: str, description_str: str) -> dict:
    """Prepares the base_parameters dictionary for a Databricks notebook task."""
    return {
        "python_script_content": script_content_str,
        "script_target_description": description_str
    }

# --- 1. Python Callable Functions for Pre-Databricks Tasks ---

def get_decryption_key_callable(**kwargs):
    ti = kwargs['ti']
    general_config = pipeline_config.get('general', {})
    fernet_key_var_name = general_config.get('fernet_key_airflow_variable_name')

    if not fernet_key_var_name:
        logger.error("Fernet key Airflow Variable name not specified in pipeline_config.json.")
        raise ConfigurationError("Fernet key Airflow Variable name missing in config.")
    
    try:
        retrieved_key_str = Variable.get(fernet_key_var_name)
        # Key should be bytes, but Airflow variables are strings. Ensure it's encoded.
        # Fernet keys are base64 encoded strings, so they are safe for Airflow variables.
        # The Decryptor class expects bytes.
        logger.info(f"Successfully retrieved Fernet key from Airflow Variable: '{fernet_key_var_name}'.")
        ti.xcom_push(key="decryption_key_str", value=retrieved_key_str)
    except KeyError: # Airflow's Variable.get raises KeyError if variable not found
        logger.error(f"Airflow Variable '{fernet_key_var_name}' not found for Fernet key.")
        # Fallback for DEMO ONLY - NOT FOR PRODUCTION
        logger.warning("FALLBACK (DEMO ONLY): Generating a temporary Fernet key as Airflow Variable was not found.")
        demo_key_bytes = Fernet.generate_key()
        ti.xcom_push(key="decryption_key_str", value=demo_key_bytes.decode('utf-8')) # Store as string
        # raise ConfigurationError(f"Airflow Variable '{fernet_key_var_name}' not found.")

def decrypt_data_callable(**kwargs):
    ti = kwargs['ti']
    decryption_key_str = ti.xcom_pull(task_ids="get_decryption_key_task", key="decryption_key_str")
    if not decryption_key_str:
        raise ValueError("Decryption key not found in XComs.")

    decryption_key_bytes = decryption_key_str.encode('utf-8')
    
    # Encrypt the sample data dynamically for this run
    try:
        f = Fernet(decryption_key_bytes)
        sample_data_json_str_original = json.dumps(ORIGINAL_SAMPLE_DATA_FOR_ENCRYPTION)
        encrypted_payload = f.encrypt(sample_data_json_str_original.encode('utf-8'))
        logger.info(f"Sample data encrypted dynamically for task: {encrypted_payload[:50]}...")
    except Exception as e:
        logger.error(f"Failed to encrypt sample data within task: {e}")
        raise

    decryptor = Decryptor(key=decryption_key_bytes)
    decrypted_data_bytes = decryptor.decrypt_data(encrypted_payload)
    
    if decrypted_data_bytes:
        decrypted_data_str = decrypted_data_bytes.decode('utf-8')
        logger.info(f"Data decrypted successfully. Decrypted data (sample): {decrypted_data_str[:100]}...")
        ti.xcom_push(key="decrypted_data_json_str", value=decrypted_data_str)
    else:
        logger.error("Decryption failed.")
        raise ValueError("Decryption returned None.")

def stage_data_for_autoloader_callable(**kwargs):
    ti = kwargs['ti']
    dag_run_id = kwargs['dag_run'].id
    decrypted_data_json_str = ti.xcom_pull(task_ids="decrypt_data_task", key="decrypted_data_json_str")
    if not decrypted_data_json_str:
        raise ValueError("Decrypted data not found in XComs.")

    data_staging_config = pipeline_config.get('data_staging_config', {})
    autoloader_params_config = pipeline_config.get('autoloader_job_params', {})
    
    staging_template = data_staging_config.get('staging_area_path_template')
    input_suffix = autoloader_params_config.get('input_path_suffix')

    if not staging_template or not input_suffix:
        raise ConfigurationError("Staging path template or input suffix missing in config.")

    # Format the path
    pipeline_name_from_config = pipeline_config.get('general', {}).get('pipeline_name', 'default_pipeline')
    cloud_staging_path_base = staging_template.replace("{pipeline_name}", pipeline_name_from_config).replace("{{ dag_run.id }}", dag_run_id)
    
    # This is the path Autoloader will scan (a directory)
    autoloader_input_cloud_path = os.path.join(cloud_staging_path_base, input_suffix)

    # Simulate upload by writing to a local temporary file structure
    # The actual Autoloader job will read from `autoloader_input_cloud_path` in the cloud.
    # This local write is just for demonstration within the Airflow worker.
    local_temp_dir = tempfile.mkdtemp(prefix="local_staging_")
    local_file_path = Path(local_temp_dir) / dag_run_id / input_suffix / "decrypted_data_001.json"
    local_file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(local_file_path, 'w') as f:
        f.write(decrypted_data_json_str)
    
    logger.info(f"Simulated upload: Data written to local temp file: {local_file_path}")
    logger.info(f"Autoloader will be configured to read from cloud path: {autoloader_input_cloud_path}")
    
    ti.xcom_push(key="autoloader_input_cloud_path", value=autoloader_input_cloud_path)


# --- 2. Python Callables for Generating Databricks Script Parameters ---

def generate_autoloader_script_for_databricks_callable(**kwargs):
    ti = kwargs['ti']
    autoloader_input_cloud_path = ti.xcom_pull(task_ids="stage_data_for_autoloader_task", key="autoloader_input_cloud_path")
    if not autoloader_input_cloud_path:
        raise ValueError("Autoloader input cloud path not found in XComs.")

    autoloader_job_cfg = pipeline_config.get('autoloader_job_params', {})
    
    # Construct schema and checkpoint locations
    output_table_name = autoloader_job_cfg.get('output_delta_table_name', 'default_db.default_bronze_table')
    schema_root = autoloader_job_cfg.get('schema_location_root', 's3://default-bucket/schemas/')
    checkpoint_root = autoloader_job_cfg.get('checkpoint_location_root', 's3://default-bucket/checkpoints/')
    
    # Sanitize table name for use in path
    sanitized_table_name = output_table_name.replace('.', '_')

    params_for_script_gen = {
        'input_path': autoloader_input_cloud_path,
        'input_format': autoloader_job_cfg.get('input_format', 'json'),
        'output_delta_table_name': output_table_name,
        'schema_location': os.path.join(schema_root, sanitized_table_name),
        'checkpoint_location': os.path.join(checkpoint_root, sanitized_table_name),
        'file_format_options': autoloader_job_cfg.get('file_format_options', {}),
        'trigger_type': autoloader_job_cfg.get('trigger_type', 'availableNow'),
        'output_mode': autoloader_job_cfg.get('output_mode', 'append'),
        'spark_app_name': autoloader_job_cfg.get('spark_app_name', 'GenericAutoloaderApp').replace("{{ dag_run.id }}", kwargs['dag_run'].id),
        'extra_spark_configs': autoloader_job_cfg.get('extra_spark_configs', {})
    }
    
    autoloader_script_content = generate_autoloader_script(params_for_script_gen)
    ti.xcom_push(key="autoloader_script_content", value=autoloader_script_content)

def generate_deduplication_script_for_databricks_callable(**kwargs):
    ti = kwargs['ti']
    dedup_job_cfg = pipeline_config.get('deduplication_job_params', {})
    autoloader_job_cfg = pipeline_config.get('autoloader_job_params', {}) # Needed for source table

    strategy = dedup_job_cfg.get('strategy')
    if not strategy:
        raise ConfigurationError("Deduplication strategy not specified in config.")

    if strategy == 'merge_into':
        merge_cfg = dedup_job_cfg.get('merge_into_config', {})
        params_for_script_gen = {
            'source_table_name': merge_cfg.get('source_table_name') or autoloader_job_cfg.get('output_delta_table_name'), # Default to autoloader output
            'target_table_name': merge_cfg.get('target_table_name'),
            'deduplication_keys': merge_cfg.get('deduplication_keys'),
            'merge_condition_extras': merge_cfg.get('merge_condition_extras'),
            'when_matched_update_clause': merge_cfg.get('when_matched_update_clause'),
            'when_not_matched_insert_clause': merge_cfg.get('when_not_matched_insert_clause'),
            'when_not_matched_by_source_delete_clause': merge_cfg.get('when_not_matched_by_source_delete_clause'),
            'when_not_matched_by_source_update_clause': merge_cfg.get('when_not_matched_by_source_update_clause'),
            'spark_app_name': dedup_job_cfg.get('spark_app_name', 'GenericMergeIntoApp').replace("{{ dag_run.id }}", kwargs['dag_run'].id),
            'extra_spark_configs': dedup_job_cfg.get('extra_spark_configs', {})
        }
        # Validate required fields for merge_into_script
        if not all([params_for_script_gen['source_table_name'], params_for_script_gen['target_table_name'], params_for_script_gen['deduplication_keys']]):
            raise ConfigurationError("Missing critical parameters for 'merge_into' script generation.")
        deduplication_script_content = generate_merge_into_script(params_for_script_gen)
    elif strategy == 'drop_duplicates':
        # This would generate a script that reads a table, applies dropDuplicates, and overwrites/appends.
        # For this example, we assume 'drop_duplicates' might be a simpler script if used.
        # The generate_spark_deduplication_code for drop_duplicates produces a snippet, not a full script.
        # For a full job, it would need a similar structure to generate_merge_into_script.
        # Let's assume a simplified path for now, or that it's integrated differently.
        # For now, this path will raise an error to indicate it needs more specific handling if chosen.
        # Alternatively, one could define a separate script generator for a batch drop_duplicates job.
        logger.warning("Strategy 'drop_duplicates' for a full Databricks job script is not fully fleshed out here. "
                       "It would typically involve reading a source table, applying dropDuplicates, and writing to a new table or overwriting.")
        # Example of how one might structure parameters for a batch drop_duplicates job:
        # drop_dup_cfg = dedup_job_cfg.get('drop_duplicates_config_example_comment_out', {}) # Using the example config
        # params_for_snippet = {
        #     'strategy': 'drop_duplicates',
        #     'deduplication_keys': drop_dup_cfg.get('deduplication_keys', []),
        #     # ... other params like input_dataframe_name ...
        # }
        # deduplication_script_content = f"""
        # spark.read.table("{drop_dup_cfg.get('source_table_name')}").{generate_spark_deduplication_code(params_for_snippet)}.write.mode("overwrite").saveAsTable("{drop_dup_cfg.get('target_table_name')}")
        # """
        # This is a conceptual example.
        raise NotImplementedError("Full script generation for 'drop_duplicates' strategy as a separate Databricks job needs specific implementation.")
    else:
        raise ConfigurationError(f"Unsupported deduplication strategy: {strategy}")
        
    ti.xcom_push(key="deduplication_script_content", value=deduplication_script_content)

# --- 3. DAG Definition ---
dag_id_from_config = pipeline_config.get("general", {}).get("pipeline_name", "DataProcessingPipeline_v2_Fallback")
db_config = pipeline_config.get('airflow_databricks_config', {})
default_db_conn_id = db_config.get('databricks_conn_id', 'databricks_default')
default_notebook_path = db_config.get('databricks_notebook_path', '/Shared/GenericPySparkRunner_Fallback') # Ensure this notebook exists
default_cluster_config = db_config.get('new_cluster_config') # This should match your JSON structure if using new_cluster
if not default_cluster_config: # Fallback or use job cluster key
    default_cluster_config = {"existing_cluster_id": db_config.get('databricks_job_cluster_key', 'YOUR_JOB_CLUSTER_ID_HERE')}


default_args = {
    'owner': 'airflow_admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0, # Set to 0 for easier debugging during development
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id=dag_id_from_config,
    default_args=default_args,
    description='Orchestrates data processing using Databricks: Decryption, Staging, Autoloader, Deduplication.',
    schedule_interval=None,
    catchup=False,
    tags=['pipeline_framework', 'databricks'],
) as dag:

    get_key_op = PythonOperator(
        task_id='get_decryption_key_task',
        python_callable=get_decryption_key_callable,
    )

    decrypt_data_op = PythonOperator(
        task_id='decrypt_data_task',
        python_callable=decrypt_data_callable,
    )

    stage_data_op = PythonOperator(
        task_id='stage_data_for_autoloader_task',
        python_callable=stage_data_for_autoloader_callable,
    )

    # --- Autoloader on Databricks ---
    generate_autoloader_script_op = PythonOperator(
        task_id='generate_autoloader_script_task',
        python_callable=generate_autoloader_script_for_databricks_callable,
    )

    autoloader_notebook_params = get_db_notebook_params(
        script_content_str="{{ ti.xcom_pull(task_ids='generate_autoloader_script_task', key='autoloader_script_content') }}",
        description_str="Autoloader Job from Airflow"
    )
    
    databricks_autoloader_op = DatabricksSubmitRunOperator(
        task_id='databricks_autoloader_job',
        databricks_conn_id=default_db_conn_id,
        json={
            'new_cluster': default_cluster_config if 'existing_cluster_id' not in default_cluster_config else None,
            'existing_cluster_id': default_cluster_config.get('existing_cluster_id'),
            'notebook_task': {
                'notebook_path': default_notebook_path,
                'base_parameters': autoloader_notebook_params,
            },
        },
    )

    # --- Deduplication on Databricks ---
    generate_deduplication_script_op = PythonOperator(
        task_id='generate_deduplication_script_task',
        python_callable=generate_deduplication_script_for_databricks_callable,
    )
    
    deduplication_notebook_params = get_db_notebook_params(
        script_content_str="{{ ti.xcom_pull(task_ids='generate_deduplication_script_task', key='deduplication_script_content') }}",
        description_str="Deduplication Job from Airflow"
    )

    databricks_deduplication_op = DatabricksSubmitRunOperator(
        task_id='databricks_deduplication_job',
        databricks_conn_id=default_db_conn_id,
        json={
            'new_cluster': default_cluster_config if 'existing_cluster_id' not in default_cluster_config else None,
            'existing_cluster_id': default_cluster_config.get('existing_cluster_id'),
            'notebook_task': {
                'notebook_path': default_notebook_path,
                'base_parameters': deduplication_notebook_params,
            },
        },
    )

    # --- Task Dependencies ---
    get_key_op >> decrypt_data_op >> stage_data_op
    stage_data_op >> generate_autoloader_script_op >> databricks_autoloader_op
    databricks_autoloader_op >> generate_deduplication_script_op >> databricks_deduplication_op
    
logger.info(f"DAG file '{Path(__file__).name}' (dag_id: {dag.dag_id}) processed by Airflow.")
