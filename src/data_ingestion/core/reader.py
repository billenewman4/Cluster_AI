"""
File Reader Module
Centralized service for reading all file formats with optimized performance.
Provides unified interface for CSV, Excel, TSV, and Parquet files with robust error handling.

This module incorporates all file utility functions from file_utils.py, eliminating redundancy
and providing a single source of truth for file operations throughout the application.
"""

import pandas as pd
import logging
import os
import hashlib
from pathlib import Path
from datetime import datetime
from typing import List, Union, Optional, Dict, Any, Set, Tuple, Callable
from ..utils.file_utils import (
    ensure_directory,
    find_newest_file,
    batch_file_operations
)

logger = logging.getLogger(__name__)

class FileReader:
    """
    Centralized file reading service with optimized performance.
    
    This class provides a unified interface for reading all file formats used
    in the data ingestion pipeline, including CSV, Excel, TSV, and Parquet.
    Features include:
    - Automatic encoding detection
    - Consistent error handling
    - Performance optimizations
    - Support for sheet-specific Excel reading
    - Robust CSV dialect handling
    - File discovery and batch operations
    """
    
    # Constants for improved maintainability
    SUPPORTED_EXTENSIONS: Set[str] = {'.xlsx', '.xls', '.csv', '.tsv', '.parquet'}
    DEFAULT_ENCODING: str = 'utf-8'
    FALLBACK_ENCODING: str = 'latin-1'
    BATCH_SIZE: int = 50
    
    def __init__(self, batch_size: int = BATCH_SIZE):
        """Initialize the file reader with configurable batch size.
        
        Args:
            batch_size: Size of batches for batch processing operations
        """
        self.batch_size = batch_size
    
    def get_supported_files(self, directory: Union[str, Path], include_patterns: Optional[List[str]] = None) -> List[Path]:
        """Get all supported files from directory with optimized path handling.
        
        Args:
            directory: Source directory path
            include_patterns: Optional list of glob patterns to filter files (e.g., ['*.csv', '*.xlsx'])
            
        Returns:
            List[Path]: List of files with supported extensions
        """
        # Ensure directory exists and get Path object
        directory = ensure_directory(directory)
        
        if not directory.exists():
            raise FileNotFoundError(f"Directory does not exist: {directory}")
            
        # If specific patterns provided, use them; otherwise use supported extensions
        if include_patterns:
            matching_files = []
            for pattern in include_patterns:
                matching_files.extend(directory.glob(pattern))
            return [f for f in matching_files if f.is_file()]
        else:
            # Use set for O(1) lookup of extensions and list comprehension for optimal performance
            return [
            file 
            for ext in self.SUPPORTED_EXTENSIONS
            for file in directory.glob(f"*{ext}")
        ]
    
    def read_file(self, file_path: Union[str, Path], **kwargs) -> pd.DataFrame:
        """Read a single file and return DataFrame using format-specific optimizations.
        
        Args:
            file_path: Path to the file
            **kwargs: Additional parameters to pass to underlying pandas read functions
            
        Returns:
            pd.DataFrame: DataFrame containing file contents
            
        Raises:
            ValueError: If file format is not supported
        """
        # Ensure file_path is Path object for consistency
        file_path = Path(file_path) if isinstance(file_path, str) else file_path
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        logger.info(f"Reading file: {file_path.name}")
        ext = file_path.suffix.lower()
        
        # Dispatch to appropriate reader based on file extension
        if ext in {'.xlsx', '.xls'}:
            return self.read_excel(file_path, **kwargs)
        elif ext == '.csv':
            return self.read_csv(file_path, **kwargs)
        elif ext == '.tsv':
            return self.read_tsv(file_path, **kwargs)
        elif ext == '.parquet':
            return self.read_parquet(file_path, **kwargs)
        else:
            raise ValueError(f"Unsupported file format: {ext}")
    
    def read_csv(self, file_path: Union[str, Path], **kwargs) -> pd.DataFrame:
        """Read CSV file with automatic encoding detection and error handling.
        
        Args:
            file_path: Path to the CSV file
            **kwargs: Additional parameters to pass to pd.read_csv
            
        Returns:
            pd.DataFrame: DataFrame containing CSV contents
        """
        file_path = Path(file_path) if isinstance(file_path, str) else file_path
        
        # Default parameters - can be overridden by kwargs
        params = {
            'encoding': self.DEFAULT_ENCODING,
            'on_bad_lines': 'warn',
            'low_memory': True
        }
        
        # Update with any provided kwargs
        params.update(kwargs)
        
        try:
            return pd.read_csv(file_path, **params)
        except UnicodeDecodeError:
            # Fall back to Latin-1 encoding if UTF-8 fails
            logger.warning(f"UTF-8 decoding failed for {file_path.name}, trying Latin-1")
            params['encoding'] = self.FALLBACK_ENCODING
            return pd.read_csv(file_path, **params)
        except Exception as e:
            logger.error(f"Error reading CSV {file_path.name}: {str(e)}")
            raise
    
    def read_tsv(self, file_path: Union[str, Path], **kwargs) -> pd.DataFrame:
        """Read TSV file with automatic encoding detection and error handling.
        
        Args:
            file_path: Path to the TSV file
            **kwargs: Additional parameters to pass to pd.read_csv
            
        Returns:
            pd.DataFrame: DataFrame containing TSV contents
        """
        # Ensure separator is tab
        kwargs['sep'] = '\t'
        # Use the CSV reader with tab separator
        return self.read_csv(file_path, **kwargs)
    
    def read_excel(self, file_path: Union[str, Path], **kwargs) -> pd.DataFrame:
        """Read Excel file with optimized performance.
        
        Args:
            file_path: Path to the Excel file
            **kwargs: Additional parameters to pass to pd.read_excel
            
        Returns:
            pd.DataFrame: DataFrame containing Excel contents
        """
        file_path = Path(file_path) if isinstance(file_path, str) else file_path
        
        # Default parameters - can be overridden by kwargs
        params = {
            'engine': 'openpyxl'  # More robust than default
        }
        
        # Update with any provided kwargs
        params.update(kwargs)
        
        try:
            return pd.read_excel(file_path, **params)
        except Exception as e:
            logger.error(f"Error reading Excel {file_path.name}: {str(e)}")
            raise
    
    def read_excel_sheets(self, file_path: Union[str, Path], 
                         sheet_filter: Optional[callable] = None) -> Dict[str, pd.DataFrame]:
        """Read multiple sheets from Excel file with optional filtering.
        
        Args:
            file_path: Path to the Excel file
            sheet_filter: Optional function to filter sheet names
            
        Returns:
            Dict[str, pd.DataFrame]: Dictionary of sheet name to DataFrame mappings
        """
        file_path = Path(file_path) if isinstance(file_path, str) else file_path
        
        try:
            # Load the Excel file
            excel_file = pd.ExcelFile(file_path, engine='openpyxl')
            
            # Get sheet names, applying filter if provided
            sheet_names = excel_file.sheet_names
            if sheet_filter:
                sheet_names = [name for name in sheet_names if sheet_filter(name)]
            
            # Read each sheet into a DataFrame
            result = {}
            for sheet_name in sheet_names:
                result[sheet_name] = pd.read_excel(excel_file, sheet_name=sheet_name)
                
            return result
        except Exception as e:
            logger.error(f"Error reading Excel sheets from {file_path.name}: {str(e)}")
            raise
    
    def read_parquet(self, file_path: Union[str, Path], **kwargs) -> pd.DataFrame:
        """Read Parquet file with optimized performance.
        
        Args:
            file_path: Path to the Parquet file
            **kwargs: Additional parameters to pass to pd.read_parquet
            
        Returns:
            pd.DataFrame: DataFrame containing Parquet contents
        """
        file_path = Path(file_path) if isinstance(file_path, str) else file_path
        
        try:
            return pd.read_parquet(file_path, **kwargs)
        except Exception as e:
            logger.error(f"Error reading Parquet {file_path.name}: {str(e)}")
            raise
            
    def read_with_chunking(self, file_path: Union[str, Path], 
                          chunk_size: int = 10000, 
                          processor: callable = None) -> pd.DataFrame:
        """Read large files in chunks to optimize memory usage.
        
        Args:
            file_path: Path to the file
            chunk_size: Number of rows to read per chunk
            processor: Function to process each chunk
            
        Returns:
            pd.DataFrame: Combined DataFrame or None if processor provided
        """
        file_path = Path(file_path) if isinstance(file_path, str) else file_path
        ext = file_path.suffix.lower()
        
        try:
            if ext == '.csv':
                # CSV chunking using pandas built-in chunking
                chunks = []
                for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                    if processor:
                        processor(chunk)
                    else:
                        chunks.append(chunk)
                
                # Only combine if we're collecting chunks
                if not processor:
                    return pd.concat(chunks, ignore_index=True)
                return None
                
            elif ext in {'.xlsx', '.xls'}:
                # Excel doesn't support native chunking, but we can manually chunk after loading
                df = self.read_excel(file_path)
                if processor:
                    # Process in chunks
                    for i in range(0, len(df), chunk_size):
                        chunk = df.iloc[i:i+chunk_size].copy()
                        processor(chunk)
                    return None
                else:
                    return df
                    
            elif ext == '.parquet':
                # Parquet has built-in chunking support
                return pd.read_parquet(file_path)
                
        except Exception as e:
            logger.error(f"Error reading {file_path.name} with chunking: {str(e)}")
            raise

