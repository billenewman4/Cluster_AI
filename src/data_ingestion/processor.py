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
from .product_transformer import ProductTransformer

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
    
    def process_file(self, file_path: Path) -> pd.DataFrame:
        """Process a single file with appropriate cleaning steps.
        
        Args:
            file_path: Path to the file
            
        Returns:
            pd.DataFrame: Cleaned DataFrame from the file
        """
        try:
            # Read the file data
            df = self.file_reader.read_file(file_path)
            
            if df.empty:
                logger.warning(f"Empty DataFrame from {file_path.name}")
                return pd.DataFrame()
            
            # Clean the data
            df = self.data_cleaner.clean_dataframe(df, file_path.name)
            
            logger.info(f"Successfully processed {file_path.name} with {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Error processing {file_path.name}: {str(e)}")
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
    
    def process_product_query(self, filter_categories: Optional[List[str]] = None, test_limit: Optional[int] = None) -> pd.DataFrame:
        """Process the product query file with specialized transformations.
        
        Applies standard transformations and optional filtering by category.
        
        Args:
            filter_categories: Optional list of categories to filter by
            test_limit: Optional limit for number of records (for testing)
            
        Returns:
            pd.DataFrame: Processed product query data
        """
        query_file = self.incoming_dir / self.PRODUCT_QUERY_FILENAME
        
        if not query_file.exists():
            logger.error(f"Product query file not found: {query_file}")
            return pd.DataFrame()
        
        try:
            # Use our centralized file reader
            df = self.file_reader.read_csv(query_file)
            logger.info(f"Read {len(df)} records from {query_file.name}")
            
            # Validate expected input columns exist
            expected_cols = ['ProductDescription', 'ProductDescription2', 'ProductCategory', 'product_code']
            missing_cols = [col for col in expected_cols if not any(c for c in df.columns if c.lower() == col.lower())]
            if missing_cols:
                raise ValueError(f"Critical columns missing from product query file: {missing_cols}")
            
            # Process the data using the transformer
            df = self.product_transformer.process_product_data(df)
            logger.info(f"Transformed product data: {len(df)} records")
            
            # Analyze categories
            if 'ProductCategory' in df.columns:
                categories = df['ProductCategory'].dropna().unique().tolist()
                logger.info(f"Product categories: {categories}")
                
                # Filter by category if specified
                if filter_categories:
                    before_count = len(df)
                    df = df[df['ProductCategory'].isin(filter_categories)]
                    logger.info(f"Filtered to {len(df)} records for categories: {filter_categories}")
                    logger.info(f"Removed {before_count - len(df)} records that didn't match filter")
            
            # Deduplicate by product_code to avoid processing duplicates
            if 'product_code' in df.columns:
                before_count = len(df)
                df = df.drop_duplicates(subset=['product_code'])
                dupes_removed = before_count - len(df)
                if dupes_removed > 0:
                    logger.info(f"Removed {dupes_removed} duplicate product codes")
            
            # Apply test limit if specified
            if test_limit and isinstance(test_limit, int) and test_limit > 0:
                df = df.head(test_limit)
                logger.info(f"Applied test limit: {test_limit} records")
            
            # Save the processed product query data
            self.save_processed_data(df, self.PROCESSED_QUERY_FILENAME)
            
            return df
            
        except Exception as e:
            logger.error(f"Error processing product query: {str(e)}")
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
