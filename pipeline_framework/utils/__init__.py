# pipeline_framework/utils/__init__.py

"""
Utilities for the Pipeline Framework.

This package includes modules for:
- Logging setup
- Custom exception classes
- Configuration management
"""

from .logger import setup_logging
from .custom_exceptions import (
    PipelineFrameworkError,
    DataIngestionError,
    DataDecryptionError,
    DataTransformationError,
    DeduplicationError,
    DataLoaderError,
    ConfigurationError,
    AirflowDagError
)
from .config_manager import load_config

__all__ = [
    "setup_logging",
    "PipelineFrameworkError",
    "DataIngestionError",
    "DataDecryptionError",
    "DataTransformationError",
    "DeduplicationError",
    "DataLoaderError",
    "ConfigurationError",
    "AirflowDagError",
    "load_config",
]

# Optional: Log that the utils package has been accessed/imported,
# but be careful as this can be noisy if not controlled.
# import logging
# logging.getLogger(__name__).info("Pipeline Framework utils package initialized.")
