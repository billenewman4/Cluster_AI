"""
Data Processor Module
Main orchestrator for efficient data ingestion and processing pipeline.

This module serves as the single source of truth for all file reading and processing
operations, eliminating redundant file reading implementations across the codebase.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import List, Dict, Optional, Union, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from .core.reader import FileReader
from .core.cleaner import DataCleaner
from .core.product_transformer_Product_Q import ProductTransformer
from .utils.file_utils import get_file_metadata

logger = logging.getLogger(__name__)

class DataProcessor:
    """Main data processing orchestrator with optimized operations.
    
    Serves as the centralized service for all file reading, processing, and saving
    operations across the entire application. This class should be the only point of
    access for reading files, eliminating redundant implementations elsewhere.
    """
    
    # Constants for improved maintainability and consistency
    DEFAULT_INCOMING_DIR = "data/incoming"
    DEFAULT_PROCESSED_DIR = "data/processed"
    PRODUCT_QUERY_FILENAME = "Product_Query_2025_06_06.csv"
    
    def __init__(self, incoming_dir: str = DEFAULT_INCOMING_DIR, processed_dir: str = DEFAULT_PROCESSED_DIR):
        """Initialize the data processor with configurable directories.
        
        Args:
            incoming_dir: Directory containing incoming data files
            processed_dir: Directory for processed output
        """
        self.incoming_dir = Path(incoming_dir)
        self.processed_dir = Path(processed_dir)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize dependencies using composition for loose coupling
        self.file_reader = FileReader()
        self.data_cleaner = DataCleaner()
        self.product_transformer = ProductTransformer()
    
    def process_file(self, file_path: Union[str, Path], category: List[str] = None, limit_per_category: int = None) -> pd.DataFrame:
        """Process a single file with appropriate cleaning steps.
        
        Args:
            file_path: Path to the file (string or Path object)
            category: Category of the products we want to process
        Returns:
            pd.DataFrame: Cleaned DataFrame from the file
        """
        # Convert string to Path object if needed
        file_path = Path(file_path) if isinstance(file_path, str) else file_path
        
        try:
            # Get file metadata for better error reporting
            metadata = get_file_metadata(file_path)
            logger.info(f"Processing file: {metadata['filename']} ({metadata['size_bytes']} bytes)")
            
            # Read the file data
            df = self.file_reader.read_file(file_path)
            
            if df.empty:
                logger.warning(f"Empty DataFrame from {file_path.name}")
                return pd.DataFrame()
            
            # Clean the data
            df = self.data_cleaner.clean_dataframe(df, file_path.name)
            df = self.product_transformer.process_product_data(df)

            if category:
                df_list = []
                for cat in category:
                    cat_df = df[df['category_description'] == cat]
                    if cat_df.empty:
                        logger.warning(f"No data found for category: '{cat}'. Available categories: {df['category_description'].unique()[:10]}")
                        continue
                    
                    if limit_per_category:
                        # Only sample if we have enough rows
                        if len(cat_df) >= limit_per_category:
                            df_list.append(cat_df.sample(n=limit_per_category))
                        else:
                            logger.info(f"Category '{cat}' has only {len(cat_df)} rows, using all available (requested {limit_per_category})")
                            df_list.append(cat_df)
                    else:
                        df_list.append(cat_df)
                
                if not df_list:
                    logger.error(f"No data found for any of the requested categories: {category}")
                    return pd.DataFrame()
                    
                df = pd.concat(df_list, ignore_index=True)
            
            logger.info(f"Successfully processed {file_path.name} with {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Error processing {file_path.name if hasattr(file_path, 'name') else str(file_path)}: {str(e)}")
            return pd.DataFrame()
    
    def process_all_files(self, max_workers: int = 4) -> pd.DataFrame:
        """Process all files and return combined DataFrame.
        
        Uses parallel processing for improved performance with large datasets.
        
        Args:
            max_workers: Maximum number of threads for parallel processing
            
        Returns:
            pd.DataFrame: Combined DataFrame from all files
        """
        files = self.file_reader.get_supported_files(self.incoming_dir)
        
        if not files:
            logger.warning(f"No files found in {self.incoming_dir}")
            return pd.DataFrame()
            
        logger.info(f"Found {len(files)} files to process")
        
        # Use parallel processing for efficiency with ThreadPoolExecutor
        dataframes = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks and create a map of future to filename
            future_to_file = {executor.submit(self.process_file, file): file for file in files}
            
            # Process as they complete
            for future in as_completed(future_to_file):
                file = future_to_file[future]
                try:
                    df = future.result()
                    if not df.empty:
                        dataframes.append(df)
                except Exception as e:
                    logger.error(f"Exception processing {file.name}: {str(e)}")
        
        if not dataframes:
            logger.warning("No valid data found in any files")
            return pd.DataFrame()
            
        # Combine all data efficiently
        combined_df = pd.concat(dataframes, ignore_index=True)
        logger.info(f"Combined data: {len(combined_df)} rows")
        
        return combined_df
    
    def save_processed_data(self, df: pd.DataFrame, filename: str = "inventory_base.parquet") -> Optional[Path]:
        """Save processed data to file using efficient parquet format.
        
        Args:
            df: DataFrame to save
            filename: Output filename
            
        Returns:
            Path: Path to the saved file or None if DataFrame was empty
        """
        if df.empty:
            logger.warning("No data to save")
            return None
            
        # Ensure processed directory exists
        self.processed_dir.mkdir(exist_ok=True, parents=True)
        
        # Save as parquet for efficiency
        output_path = self.processed_dir / filename
        df.to_parquet(output_path, index=False, compression='snappy')
        
        logger.info(f"Saved {len(df)} rows to {output_path}")
        return output_path
        
    def read_parquet(self, filename: str) -> pd.DataFrame:
        """Read a parquet file from the processed directory.
        
        Centralizes parquet file reading to avoid direct pandas calls elsewhere.
        
        Args:
            filename: Name of the parquet file to read
            
        Returns:
            pd.DataFrame: DataFrame read from the parquet file
        """
        file_path = self.processed_dir / filename
        
        if not file_path.exists():
            logger.warning(f"Parquet file not found: {file_path}")
            return pd.DataFrame()
            
        try:
            logger.info(f"Reading parquet file: {file_path.name}")
            return self.file_reader.read_parquet(file_path)
        except Exception as e:
            logger.error(f"Error reading parquet file {file_path.name}: {str(e)}")
            return pd.DataFrame()
    
    def run(self) -> pd.DataFrame:
        """Run the full data ingestion pipeline.
        
        Returns:
            pd.DataFrame: Processed data
        """
        logger.info("Starting data ingestion pipeline")
        
        # Process all files
        df = self.process_all_files()
        
        if df.empty:
            logger.warning("No data processed")
            return df
            
        # Save results
        self.save_processed_data(df, self.INVENTORY_BASE_FILENAME)
        
        logger.info("Data ingestion pipeline completed successfully")
        return df
