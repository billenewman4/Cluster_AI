"""
File Reader Module
Handles reading Excel, CSV, and TSV files with automatic format detection.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import List, Union, Optional

logger = logging.getLogger(__name__)

class FileReader:
    """Handles reading various file formats with optimized performance."""
    
    SUPPORTED_EXTENSIONS = ['.xlsx', '.xls', '.csv', '.tsv']
    
    def get_supported_files(self, directory: Union[str, Path]) -> List[Path]:
        """Get all supported files from directory.
        
        Args:
            directory: Source directory path
            
        Returns:
            List[Path]: List of files with supported extensions
        """
        # Convert to Path if string
        directory = Path(directory) if isinstance(directory, str) else directory
            
        # Use list comprehension for efficiency
        return [
            file 
            for ext in self.SUPPORTED_EXTENSIONS
            for file in directory.glob(f"*{ext}")
        ]
    
    def read_file(self, file_path: Path) -> pd.DataFrame:
        """Read a single file and return DataFrame.
        
        Uses format-specific optimizations based on file extension.
        
        Args:
            file_path: Path to the file
            
        Returns:
            pd.DataFrame: DataFrame containing file contents
            
        Raises:
            ValueError: If file format is not supported
        """
        logger.info(f"Reading file: {file_path.name}")
        
        ext = file_path.suffix.lower()
        
        try:
            if ext in ['.xlsx', '.xls']:
                # For Excel files, use optimized engine
                return pd.read_excel(file_path, engine='openpyxl')
                
            elif ext == '.csv':
                # Try to infer CSV dialect and encoding
                return pd.read_csv(
                    file_path, 
                    encoding='utf-8',  # Try UTF-8 first
                    on_bad_lines='warn',  # Don't fail on malformed lines
                    low_memory=True  # Memory optimization
                )
                
            elif ext == '.tsv':
                # Handle TSV with explicit delimiter
                return pd.read_csv(
                    file_path,
                    sep='\t',
                    encoding='utf-8',
                    on_bad_lines='warn',
                    low_memory=True
                )
                
            else:
                raise ValueError(f"Unsupported file format: {ext}")
                
        except UnicodeDecodeError:
            # Fall back to Latin-1 encoding if UTF-8 fails
            if ext in ['.csv', '.tsv']:
                separator = ',' if ext == '.csv' else '\t'
                logger.warning(f"UTF-8 decoding failed for {file_path.name}, trying Latin-1")
                return pd.read_csv(
                    file_path,
                    sep=separator,
                    encoding='latin-1',
                    on_bad_lines='warn',
                    low_memory=True
                )
            raise
            
        except Exception as e:
            logger.error(f"Error reading {file_path.name}: {str(e)}")
            raise
