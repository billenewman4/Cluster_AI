"""
Data Ingestion Package
Handles reading, cleaning, and processing of raw inventory files.
"""

from .core.processor import DataProcessor
from .core.reader import FileReader
from .core.cleaner import DataCleaner
from .core.product_transformer import ProductTransformer
from .utils import get_file_metadata, ensure_directory, validate_dataframe_schema, detect_anomalies

__all__ = [
    'DataProcessor', 
    'FileReader', 
    'DataCleaner',
    'ProductTransformer',
    'get_file_metadata',
    'ensure_directory',
    'validate_dataframe_schema',
    'detect_anomalies'
]