"""
Data Processor Module
Main orchestrator for efficient data ingestion and processing pipeline.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from .reader import FileReader
from .cleaner import DataCleaner

logger = logging.getLogger(__name__)

class DataProcessor:
    """Main data processing orchestrator with optimized operations."""
    
    def __init__(self, incoming_dir: str = "data/incoming", processed_dir: str = "data/processed"):
        """Initialize the data processor.
        
        Args:
            incoming_dir: Directory containing incoming data files
            processed_dir: Directory for processed output
        """
        self.incoming_dir = Path(incoming_dir)
        self.processed_dir = Path(processed_dir)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        self.file_reader = FileReader()
        self.data_cleaner = DataCleaner()
    
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
    
    def save_processed_data(self, df: pd.DataFrame, filename: str = "inventory_base.parquet") -> Path:
        """Save processed data to file.
        
        Args:
            df: DataFrame to save
            filename: Output filename
            
        Returns:
            Path: Path to the saved file
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
        self.save_processed_data(df)
        
        logger.info("Data ingestion pipeline completed successfully")
        return df
