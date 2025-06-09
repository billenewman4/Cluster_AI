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

from .reader import FileReader

logger = logging.getLogger(__name__)

class ProductTransformer:
    """Specialized transformer for product data processing."""
    
    def __init__(self):
        """Initialize the product transformer with dependencies."""
        self.file_reader = FileReader()
        
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
        # Find column regardless of case sensitivity
        available_cols = {col.lower(): col for col in df.columns}
        actual_primary_col = available_cols.get(primary_col.lower(), primary_col)
        actual_secondary_col = available_cols.get(secondary_col.lower(), secondary_col)
        
        if actual_primary_col not in df.columns:
            logger.error(f"Primary column '{primary_col}' not found in DataFrame (case-insensitive)")
            logger.error(f"Available columns: {df.columns.tolist()}")
            return df
            
        # Create a copy to avoid modifying the original
        result_df = df.copy()
        
        # Log details of primary column for debugging
        null_primary_count = df[actual_primary_col].isna().sum()
        empty_primary_count = (df[actual_primary_col] == '').sum() if pd.api.types.is_string_dtype(df[actual_primary_col]) else 0
        logger.info(f"Primary description column '{actual_primary_col}' stats: {len(df)} total rows, "
                   f"{null_primary_count} null values, {empty_primary_count} empty strings")
        
        # Handle case where secondary column doesn't exist
        if actual_secondary_col not in df.columns:
            logger.warning(f"Secondary column '{secondary_col}' not found, using only primary column")
            result_df[output_col] = df[actual_primary_col].fillna('').astype(str).str.strip()
            # Add a sample log to verify content
            if len(df) > 0:
                logger.info(f"Sample merged description (primary only): '{result_df[output_col].iloc[0]}'")
            return result_df
        
        # Log details of secondary column for debugging
        null_secondary_count = df[actual_secondary_col].isna().sum()
        empty_secondary_count = (df[actual_secondary_col] == '').sum() if pd.api.types.is_string_dtype(df[actual_secondary_col]) else 0
        logger.info(f"Secondary description column '{actual_secondary_col}' stats: {len(df)} total rows, "
                   f"{null_secondary_count} null values, {empty_secondary_count} empty strings")
            
        # Apply merging logic with vectorized operations for better performance
        # First convert any non-string values to empty strings to avoid errors
        primary_values = df[actual_primary_col].fillna('').astype(str).str.strip()
        sec_values = df[actual_secondary_col].fillna('').astype(str).str.strip()
        
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
        
        # Log a sample of the merged descriptions for verification
        if len(result_df) > 0:
            sample_idx = 0
            sample_primary = primary_values.iloc[sample_idx]
            sample_secondary = sec_values.iloc[sample_idx]
            sample_merged = result_df[output_col].iloc[sample_idx]
            logger.info(f"Sample merge: '{sample_primary}' + '{sample_secondary}' = '{sample_merged}'")
        
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
        
    def unique_product_codes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate product codes from the DataFrame.
        
        Args:
            df: DataFrame to process
            
        Returns:
            pd.DataFrame: DataFrame with unique product codes, or original DataFrame if ProductCode column doesn't exist
        """
        if 'productcode' in df.columns:
            original_count = len(df)
            result_df = df.drop_duplicates(subset=['productcode'], keep='first')
            dedupe_count = len(result_df)
            logger.info(f"Deduplication: Removed {original_count - dedupe_count} duplicate ProductCode records. {dedupe_count} unique products remaining.")
            return result_df
        else:
            logger.warning("ProductCode column not found. Skipping deduplication.")
            return df
        
    
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

        # Remove duplicate product codes
        df = self.unique_product_codes(df)
        
        # Apply standard column renaming for product data
        column_map = {
            'branddescription': 'brand_name',
            # Add other standard renamings here if needed
        }
        renamed_df = self.rename_columns(df, column_map)
        
        # Create case-insensitive column lookup for finding columns regardless of case
        col_map = {col.lower(): col for col in renamed_df.columns}
        
        # Find primary description column (case-insensitive)
        primary_col = next((col_map[k] for k in col_map if k == 'productdescription'), None)
        secondary_col = next((col_map[k] for k in col_map if k == 'productdescription2'), None)
        
        if not primary_col:
            logger.error(f"Available columns: {renamed_df.columns.tolist()}")
            raise ValueError(f"Cannot merge descriptions: ProductDescription column not found (case-insensitive)")
        
        logger.info(f"Found description columns: Primary={primary_col}, Secondary={secondary_col or 'None'}")
            
        # Merge product descriptions with explicitly identified columns
        processed_df = self.merge_product_descriptions(
            df=renamed_df,
            primary_col=primary_col,
            secondary_col=secondary_col if secondary_col else 'ProductDescription2',
            output_col='product_description'
        )
        
        # Log sample processed data
        if not processed_df.empty:
            sample_row = processed_df.iloc[0]
            logger.info(f"Sample processed row after description merge:")
            if 'product_description' in processed_df.columns:
                logger.info(f"  - product_description: '{sample_row.get('product_description')}' (length: {len(str(sample_row.get('product_description', '')))})")
            
        # Ensure preserved columns remain untouched
        if preserve_columns:
            for col in preserve_columns:
                if col in df.columns and col in processed_df.columns:
                    processed_df[col] = df[col]
        
        # If standardizing columns is requested, map to expected pipeline format
        if standardize_columns:
            # Refresh case-insensitive column lookup dictionary after merge operations
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
            
            # Ensure the product_description column is consistent between original and standardized names
            # This is critical to ensure the LLM gets the combined description
            if 'product_description' not in processed_df.columns and 'ProductDescription' in processed_df.columns:
                processed_df['product_description'] = processed_df['ProductDescription']
                logger.info("Created product_description from ProductDescription column")
            
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
            
            # Verify the combined description is available and properly formatted
            if 'product_description' in processed_df.columns:
                sample_description = processed_df['product_description'].iloc[0] if not processed_df.empty else None
                logger.info(f"Verified product_description column with sample: '{sample_description}'")
                
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
                                    standardize_columns: bool = True,
                                    **kwargs) -> pd.DataFrame:
        """Read a product CSV file and apply all transformations.
        
        Args:
            file_path: Path to the product CSV file
            preserve_columns: List of column names to preserve exactly as-is
            standardize_columns: Whether to standardize column names for pipeline compatibility
            **kwargs: Additional parameters to pass to the file reader
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        try:
            # Use the centralized file reader for consistent handling and optimal performance
            df = self.file_reader.read_csv(file_path, **kwargs)
            logger.info(f"Read {len(df)} records from {Path(file_path).name}")
            
            return self.process_product_data(df, preserve_columns, standardize_columns)
            
        except Exception as e:
            logger.error(f"Error processing {Path(file_path).name}: {str(e)}")
            return pd.DataFrame()
