"""
Data Ingestion Utilities Package
Contains utility functions for file operations and data validation.
"""

from .file_utils import get_file_metadata, ensure_directory
from .validation import validate_dataframe_schema

__all__ = ['get_file_metadata', 'ensure_directory', 'validate_dataframe_schema']
