class PipelineFrameworkError(Exception):
    """Base class for exceptions in this framework."""
    def __init__(self, message="A pipeline framework error occurred"):
        self.message = message
        super().__init__(self.message)

class DataIngestionError(PipelineFrameworkError):
    """Exception raised for errors during data ingestion."""
    def __init__(self, message="Error during data ingestion"):
        super().__init__(message)

class DataDecryptionError(PipelineFrameworkError):
    """Exception raised for errors during data decryption."""
    def __init__(self, message="Error during data decryption"):
        super().__init__(message)

class DataTransformationError(PipelineFrameworkError):
    """Exception raised for errors during data transformation."""
    def __init__(self, message="Error during data transformation"):
        super().__init__(message)

class DeduplicationError(PipelineFrameworkError):
    """Exception raised for errors during data deduplication."""
    def __init__(self, message="Error during data deduplication"):
        super().__init__(message)

class DataLoaderError(PipelineFrameworkError):
    """Exception raised for errors during data loading."""
    def __init__(self, message="Error during data loading"):
        super().__init__(message)

class ConfigurationError(PipelineFrameworkError):
    """Exception raised for errors related to configuration."""
    def __init__(self, message="Configuration error"):
        super().__init__(message)

class AirflowDagError(PipelineFrameworkError):
    """Exception raised for errors specific to Airflow DAG construction or execution."""
    def __init__(self, message="Airflow DAG error"):
        super().__init__(message)


if __name__ == '__main__':
    print("--- Demonstrating Custom Exceptions ---")

    # Example 1: Catching a specific custom exception
    print("\nExample 1: Catching DataDecryptionError")
    try:
        # Simulate a situation where decryption might fail
        encrypted_data = "some_encrypted_data_that_is_invalid"
        key = "some_key"
        print(f"Attempting to decrypt data: '{encrypted_data}' with key: '{key}'")
        # Imagine a function call here that could raise DataDecryptionError
        # decrypt_function(encrypted_data, key) 
        raise DataDecryptionError("Failed to decrypt: Invalid token or key.")
    except DataDecryptionError as e:
        print(f"Caught expected exception: {type(e).__name__} - {e.message}")
    except Exception as e:
        print(f"Caught unexpected exception: {type(e).__name__} - {e}")

    # Example 2: Catching the base PipelineFrameworkError
    print("\nExample 2: Catching base PipelineFrameworkError for a ConfigurationError")
    try:
        # Simulate a configuration loading issue
        config_path = "non_existent_config.yml"
        print(f"Attempting to load configuration from: '{config_path}'")
        # Imagine a function call here that could raise ConfigurationError
        # load_app_config(config_path)
        raise ConfigurationError(f"Configuration file not found at {config_path}")
    except PipelineFrameworkError as e: # Catches ConfigurationError and any other custom error
        print(f"Caught expected pipeline framework exception: {type(e).__name__} - {e.message}")
    except Exception as e:
        print(f"Caught unexpected exception: {type(e).__name__} - {e}")

    # Example 3: Showing the hierarchy
    print("\nExample 3: Showing exception hierarchy")
    try:
        raise DataTransformationError("Invalid data type for transformation.")
    except PipelineFrameworkError as e:
        print(f"Caught: {type(e).__name__} (is instance of PipelineFrameworkError: {isinstance(e, PipelineFrameworkError)})")
        print(f"Message: {e.message}")

    print("\nCustom exceptions demonstration finished.")
