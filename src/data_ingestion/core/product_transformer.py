"""
Product Transformer Module
Handles specialized transformations for product data including description merging,
column renaming, and standardization.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

logger = logging.getLogger(__name__)

class ProductTransformer:
    """Specialized transformer for product data processing."""
    
    def __init__(self):
        """Initialize the product transformer."""
        pass
        
    def merge_product_descriptions(self, 
                                  df: pd.DataFrame, 
                                  primary_col: str = 'ProductDescription', 
                                  secondary_col: str = 'ProductDescription2', 
                                  output_col: str = 'product_description') -> pd.DataFrame:
        """Merge product description columns with specific rules.
        
        Args:
            df: DataFrame containing product data
            primary_col: Name of the primary description column
            secondary_col: Name of the secondary description column
            output_col: Name for the new merged column
            
        Returns:
            pd.DataFrame: DataFrame with merged descriptions
        """
        if primary_col not in df.columns:
            logger.error(f"Primary column '{primary_col}' not found in DataFrame")
            return df
            
        # Create a copy to avoid modifying the original
        result_df = df.copy()
        
        # Handle case where secondary column doesn't exist
        if secondary_col not in df.columns:
            logger.warning(f"Secondary column '{secondary_col}' not found, using only primary column")
            result_df[output_col] = df[primary_col].str.strip()
            return result_df
            
        # Apply merging logic with vectorized operations for better performance
        # First convert any non-string values to empty strings to avoid errors
        sec_values = df[secondary_col].fillna('').astype(str)
        
        # Create merged column with space between when secondary has content
        has_content = (sec_values != '') & (~sec_values.isna())
        
        # Use numpy.where for vectorized conditional operation
        result_df[output_col] = np.where(
            has_content,
            df[primary_col].astype(str) + ' ' + sec_values,
            df[primary_col].astype(str)
        )
        
        # Apply strip to clean up whitespace
        result_df[output_col] = result_df[output_col].str.strip()
        
        return result_df
        
    def rename_columns(self, 
                      df: pd.DataFrame, 
                      column_map: Dict[str, str]) -> pd.DataFrame:
        """Rename columns according to mapping.
        
        Args:
            df: DataFrame to process
            column_map: Dictionary mapping old column names to new ones
            
        Returns:
            pd.DataFrame: DataFrame with renamed columns
        """
        missing_cols = [col for col in column_map.keys() if col not in df.columns]
        if missing_cols:
            logger.warning(f"Columns not found for renaming: {missing_cols}")
            
        # Only rename columns that exist
        valid_map = {k: v for k, v in column_map.items() if k in df.columns}
        if not valid_map:
            return df
            
        return df.rename(columns=valid_map)
        
    def process_product_data(self, 
                            df: pd.DataFrame,
                            preserve_columns: List[str] = None,
                            standardize_columns: bool = True) -> pd.DataFrame:
        """Apply all product transformations in optimal sequence.
        
        Args:
            df: DataFrame to process
            preserve_columns: List of column names to preserve exactly as-is
            standardize_columns: Whether to standardize column names for pipeline compatibility
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        if df.empty:
            logger.warning("Empty DataFrame provided")
            return df
            
        # Apply standard column renaming for product data
        column_map = {
            'BrandDescription': 'BrandName',
            # Add other standard renamings here if needed
        }
        renamed_df = self.rename_columns(df, column_map)
        
        # Merge product descriptions
        processed_df = self.merge_product_descriptions(renamed_df)
        
        # Ensure preserved columns remain untouched
        if preserve_columns:
            for col in preserve_columns:
                if col in df.columns and col in processed_df.columns:
                    processed_df[col] = df[col]
        
        # If standardizing columns is requested, map to expected pipeline format
        if standardize_columns:
            # Map the column names to what the pipeline expects
            standard_mapping = {
                'productcategory': 'category_description',
                'brandname': 'brand_name',
                # Add any other mappings needed
            }
            
            # First ensure all columns that will be mapped exist
            for src_col in standard_mapping.keys():
                if src_col not in processed_df.columns:
                    # Skip columns that don't exist in our data
                    continue
                    
                # Get the destination column name
                dest_col = standard_mapping[src_col]
                
                # Copy the data to the standard column name
                processed_df[dest_col] = processed_df[src_col]
            
            # Ensure required columns exist for the pipeline
            required_cols = ['product_code', 'product_description', 'category_description']
            for col in required_cols:
                if col not in processed_df.columns:
                    logger.warning(f"Adding missing required column: {col}")
                    processed_df[col] = ''
        
        logger.info(f"Processed {len(processed_df)} product records with transformations")
        return processed_df

    def read_and_process_product_csv(self, 
                                    file_path: Union[str, Path],
                                    preserve_columns: List[str] = None,
                                    standardize_columns: bool = True) -> pd.DataFrame:
        """Read a product CSV file and apply all transformations.
        
        Args:
            file_path: Path to the product CSV file
            preserve_columns: List of column names to preserve exactly as-is
            standardize_columns: Whether to standardize column names for pipeline compatibility
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        file_path = Path(file_path) if isinstance(file_path, str) else file_path
        
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Read {len(df)} records from {file_path.name}")
            
            return self.process_product_data(df, preserve_columns, standardize_columns)
            
        except Exception as e:
            logger.error(f"Error processing {file_path.name}: {str(e)}")
            return pd.DataFrame()
