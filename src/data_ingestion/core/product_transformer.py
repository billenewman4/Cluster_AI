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
            logger.error(f"Available columns: {df.columns.tolist()}")
            return df
            
        # Create a copy to avoid modifying the original
        result_df = df.copy()
        
        # Log details of primary column for debugging
        null_primary_count = df[primary_col].isna().sum()
        empty_primary_count = (df[primary_col] == '').sum() if pd.api.types.is_string_dtype(df[primary_col]) else 0
        logger.info(f"Primary description column '{primary_col}' stats: {len(df)} total rows, "
                   f"{null_primary_count} null values, {empty_primary_count} empty strings")
        
        # Handle case where secondary column doesn't exist
        if secondary_col not in df.columns:
            logger.warning(f"Secondary column '{secondary_col}' not found, using only primary column")
            result_df[output_col] = df[primary_col].fillna('').astype(str).str.strip()
            return result_df
        
        # Log details of secondary column for debugging
        null_secondary_count = df[secondary_col].isna().sum()
        empty_secondary_count = (df[secondary_col] == '').sum() if pd.api.types.is_string_dtype(df[secondary_col]) else 0
        logger.info(f"Secondary description column '{secondary_col}' stats: {len(df)} total rows, "
                   f"{null_secondary_count} null values, {empty_secondary_count} empty strings")
            
        # Apply merging logic with vectorized operations for better performance
        # First convert any non-string values to empty strings to avoid errors
        primary_values = df[primary_col].fillna('').astype(str).str.strip()
        sec_values = df[secondary_col].fillna('').astype(str).str.strip()
        
        # Create merged column with space between when secondary has content
        has_content = (sec_values != '')
        
        # Use numpy.where for vectorized conditional operation
        result_df[output_col] = np.where(
            has_content,
            primary_values + ' ' + sec_values,
            primary_values
        )
        
        # Apply strip to clean up whitespace
        result_df[output_col] = result_df[output_col].str.strip()
        
        # Log merged results for debugging
        null_merged_count = result_df[output_col].isna().sum()
        empty_merged_count = (result_df[output_col] == '').sum()
        logger.info(f"Merged description column '{output_col}' stats: {len(result_df)} total rows, "
                  f"{null_merged_count} null values, {empty_merged_count} empty strings")
        
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
            
        # Debug input data
        logger.info(f"Processing product data: {len(df)} records with columns {df.columns.tolist()}")
        
        # Apply standard column renaming for product data
        column_map = {
            'BrandDescription': 'BrandName',
            # Add other standard renamings here if needed
        }
        renamed_df = self.rename_columns(df, column_map)
        
        # Check for product description columns before merging
        has_primary = 'ProductDescription' in renamed_df.columns
        has_secondary = 'ProductDescription2' in renamed_df.columns
        
        if not has_primary:
            logger.error(f"Cannot merge descriptions: Primary column 'ProductDescription' not found")
            logger.error(f"Available columns: {renamed_df.columns.tolist()}")
            # Create an empty column to prevent downstream errors
            renamed_df['ProductDescription'] = ''
            
        # Merge product descriptions (with comprehensive logging already added)
        processed_df = self.merge_product_descriptions(renamed_df)
        
        # Ensure preserved columns remain untouched
        if preserve_columns:
            for col in preserve_columns:
                if col in df.columns and col in processed_df.columns:
                    processed_df[col] = df[col]
        
        # If standardizing columns is requested, map to expected pipeline format
        if standardize_columns:
            # Create case-insensitive column lookup dictionary for O(1) access
            col_map = {col.lower(): col for col in processed_df.columns}
            
            # Map ProductCategory to category_description using case-insensitive lookup
            category_col_key = next((k for k in col_map.keys() if k == 'productcategory'), None)
            if category_col_key:
                actual_col = col_map[category_col_key]
                processed_df['category_description'] = processed_df[actual_col]
                logger.info(f"Mapped {actual_col} to category_description with {processed_df['category_description'].nunique()} unique categories")
            
            # Map other standard columns as needed using case-insensitive lookup
            standard_mapping = {
                'branddescription': 'brand_name',
                'brandname': 'brand_name',  # Map both variations
                'productcode': 'product_code',
                'productdescription': 'product_description',  # Ensure product_description is mapped
            }
            
            # Apply column mappings with case-insensitive matching for O(1) lookups
            for src_key, dest_col in standard_mapping.items():
                matching_key = next((k for k in col_map.keys() if k == src_key.lower()), None)
                if matching_key:
                    actual_src_col = col_map[matching_key]
                    processed_df[dest_col] = processed_df[actual_src_col]
                    logger.info(f"Mapped {actual_src_col} to {dest_col}")
                    
                    # If this mapped the product_code, ensure it's a string type for consistent merging
                    if dest_col == 'product_code' and not pd.api.types.is_string_dtype(processed_df[dest_col]):
                        processed_df[dest_col] = processed_df[dest_col].astype(str)
                        logger.info(f"Converted {dest_col} to string type for consistency")
            
            # Ensure required columns exist for the pipeline with proper data
            required_cols = ['product_code', 'product_description', 'category_description']
            missing_cols = [col for col in required_cols if col not in processed_df.columns]
            
            if missing_cols:
                # Raise error instead of silently creating empty columns
                # This prevents downstream processing errors with null values
                raise ValueError(
                    f"Required columns missing: {missing_cols}. "
                    f"Available columns: {processed_df.columns.tolist()}"
                )
                
            # Validate product descriptions - no nulls or empty strings allowed
            empty_descriptions = (processed_df['product_description'].isna() | 
                                (processed_df['product_description'] == ''))
            empty_count = empty_descriptions.sum()
            
            if empty_count > 0:
                logger.warning(f"Found {empty_count} records with null or empty product descriptions")
                
                # Sample problematic records for debugging
                sample_empty = processed_df[empty_descriptions].head(5)
                logger.warning(f"Sample problematic records:\n{sample_empty[['product_code', 'product_description']]}")
                
                # Drop records with empty descriptions to avoid downstream errors
                processed_df = processed_df[~empty_descriptions].copy()
                logger.info(f"Removed {empty_count} records with empty descriptions. {len(processed_df)} records remaining.")
                
                # If all records were dropped, raise ValueError to prevent further processing
                if len(processed_df) == 0:
                    raise ValueError(f"All {empty_count} records had empty product descriptions. Pipeline cannot continue.")
                    
            # Final validation - ensure all required columns have valid data
            logger.info(f"Final validation successful. Processed {len(processed_df)} records with required columns present.")
            logger.info(f"Product description column stats: {processed_df['product_description'].describe()}")
        
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
