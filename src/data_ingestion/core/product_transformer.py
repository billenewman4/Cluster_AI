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
import re

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
                                  tertiary_col: str = 'AlphaName',
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
        actual_tertiary_col = available_cols.get(tertiary_col.lower(), tertiary_col)
        
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
        
        # Handle case where tertiary column doesn't exist
        if actual_tertiary_col not in df.columns:
            logger.warning(f"Tertiary column '{tertiary_col}' not found, using only primary and secondary columns")
            result_df[output_col] = df[actual_primary_col].fillna('').astype(str).str.strip()
            # Add a sample log to verify content
            if len(df) > 0:
                logger.info(f"Sample merged description (primary and secondary only): '{result_df[output_col].iloc[0]}'")
            return result_df

        # Log details of secondary and tertiary columns for debugging
        null_secondary_count = df[actual_secondary_col].isna().sum()
        null_tertiary_count = df[actual_tertiary_col].isna().sum()
        empty_secondary_count = (df[actual_secondary_col] == '').sum() if pd.api.types.is_string_dtype(df[actual_secondary_col]) else 0
        empty_tertiary_count = (df[actual_tertiary_col] == '').sum() if pd.api.types.is_string_dtype(df[actual_tertiary_col]) else 0
        logger.info(f"Secondary description column '{actual_secondary_col}' stats: {len(df)} total rows, "
                   f"{null_secondary_count} null values, {empty_secondary_count} empty strings")
        logger.info(f"Tertiary description column '{actual_tertiary_col}' stats: {len(df)} total rows, "
                   f"{null_tertiary_count} null values, {empty_tertiary_count} empty strings")
            
        # Apply merging logic with vectorized operations for better performance
        # First convert any non-string values to empty strings to avoid errors
        primary_values = df[actual_primary_col].fillna('').astype(str).str.strip()
        sec_values = df[actual_secondary_col].fillna('').astype(str).str.strip()
        ter_values = df[actual_tertiary_col].fillna('').astype(str).str.strip()
        
        # Create merged column with proper space handling
        # Only add spaces between non-empty values
        def merge_with_spaces(primary, secondary, tertiary):
            """Merge three values with spaces only between non-empty values."""
            parts = [part for part in [primary, secondary, tertiary] if part]
            merged = ' '.join(parts)
            # Clean up multiple spaces (from original data) to single spaces
            return re.sub(r'\s+', ' ', merged).strip()
        
        # Apply the merging function element-wise
        result_df[output_col] = [
            merge_with_spaces(p, s, t) 
            for p, s, t in zip(primary_values, sec_values, ter_values)
        ]
        
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
            sample_tertiary = ter_values.iloc[sample_idx]
            sample_merged = result_df[output_col].iloc[sample_idx]
            logger.info(f"Sample merge: '{sample_primary}' + '{sample_secondary}' + '{sample_tertiary}' = '{sample_merged}'")

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
        
    def map_category_description(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map ProductCategory to standardized category descriptions for beef cuts.
        
        Args:
            df: DataFrame to process
            
        Returns:
            pd.DataFrame: DataFrame with normalized category descriptions
        """
        if 'category_description' not in df.columns:
            logger.warning("category_description column not found, skipping category mapping")
            return df
            
        # Category mapping for beef cuts to standardized primal names
        CATEGORY_MAPPING = {
            # Chuck variations
            "Beef Chuck": "Beef Chuck",
            "Chuck": "Beef Chuck",
            
            # Rib variations  
            "Beef Rib": "Beef Rib",
            "Rib": "Beef Rib",
            
            # Loin variations (including specific cuts that belong to loin primal)
            "Beef Loin": "Beef Loin", 
            "TENDERLOIN / FILET": "Beef Loin",
            "STRIP": "Beef Loin",
            "PORTERHOUSE": "Beef Loin", 
            "SHORT LOIN": "Beef Loin",
            "Sirloin": "Beef Loin",
            "BALL TIP": "Beef Loin",
            "BALL TIP / TRI TIP": "Beef Loin",
            "SHORT LOIN": "Beef Loin",
            "TOP BUTT": "Beef Loin",
            
            # Round variations
            "Beef Round": "Beef Round",
            "Round": "Beef Round",
            
            # Brisket variations
            "Beef Brisket": "Beef Brisket",
            "Brisket": "Beef Brisket",
            
            # Plate variations
            "Beef Plate": "Beef Plate",
            "Plate": "Beef Plate",
            
            # Flank variations
            "Beef Flank": "Beef Flank",
            "Flank": "Beef Flank",
            
            # Ground beef categories
            "Ground Beef": "Beef Ground",
            "Beef Ground": "Beef Ground", 
            "Ground Beef Armiar Production": "Beef Ground",
            "Ground Beef Anmar Production": "Beef Ground",
            "Stock Ground Beef": "Beef Ground",

            #Beef Trim Category
            "Beef Ground & Trim": "Beef Trim",
            
            # Variety/Miscellaneous categories
            "Beef Variety & Misc": "Beef Variety",
            "Miscellaneous Beef": "Beef Variety",
            "Beef Variety": "Beef Variety",
            "Beef Main": "Beef Variety",
            "Bagged Dry": "Beef Variety",
            "Beef Other": "Beef Variety",
        }
        
        df = df.copy()
        
        # Count original categories for logging
        original_categories = df['category_description'].value_counts()
        logger.info(f"Original categories found: {original_categories.to_dict()}")
        
        # Apply exact mapping first
        df['category_description'] = df['category_description'].map(
            lambda x: CATEGORY_MAPPING.get(x, x) if pd.notna(x) else x
        )
        
        # Count categories after normalization
        normalized_categories = df['category_description'].value_counts()
        logger.info(f"Categories after normalization: {normalized_categories.to_dict()}")
        
        return df

    def remove_cached_products(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove products that are already cached as accepted.
        
        This prevents re-processing of products that have already been
        reviewed and accepted, improving performance.
        
        Args:
            df: DataFrame containing product data
            
        Returns:
            DataFrame with cached products removed
        """
        from src.Caching.cache_manager import load_existing_cache, generate_cache_key
        
        # Ensure we have a product code column
        if 'product_code' not in df.columns:
            logger.warning("No product_code column found, cannot filter cached products")
            return df
        
        initial_count = len(df)
        
        try:
            # Load the cache
            cache_file_path = "data/processed/.accepted_items_cache.json"
            cache_data = load_existing_cache(cache_file_path)
            cached_items = cache_data.get("cached_items", {})
            
            if not cached_items:
                logger.info("No cached items found, skipping filter step")
                return df
                
            # Filter the DataFrame to keep only products not in cache
            filtered_df = df.copy()
            to_drop = []
            
            for idx, row in df.iterrows():
                product_code = row['product_code']
                if not product_code:
                    continue
                    
                try:
                    # Generate the cache key and check if product is cached
                    cache_key = generate_cache_key(product_code)
                    if cache_key in cached_items:
                        to_drop.append(idx)
                except ValueError:
                    # Invalid product code, keep the row
                    pass
            
            # Drop cached products
            if to_drop:
                filtered_df = df.drop(to_drop)
            
            # Log results
            filtered_count = len(filtered_df)
            cached_count = initial_count - filtered_count
            
            if cached_count > 0:
                logger.info(f"Removed {cached_count} already cached products ({cached_count/initial_count:.1%})")
            else:
                logger.info("No cached products found in the dataset")
            
            return filtered_df
            
        except Exception as e:
            logger.warning(f"Error filtering cached products: {e}. Continuing with all products.")
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
            
        # Remove products that are already cached as accepted
        df = self.remove_cached_products(df)
        if len(df) == 0:
            logger.warning("All products were already cached, nothing to process")
            return df
            
        # Debug input data
        logger.info(f"Processing product data: {len(df)} records with columns {df.columns.tolist()}")

        # Remove duplicate product codes
        df = self.unique_product_codes(df)
        
        # Apply comprehensive column renaming for product data
        # Note: Column names should already be normalized to lowercase by DataCleaner
        column_map = {
            'branddescription': 'brand_name',
            'brandname': 'brand_name',  # Map both variations
            'productcode': 'product_code',
            'productcategory': 'category_description',
        }
        renamed_df = self.rename_columns(df, column_map)
        
        # Find description columns (should be lowercase after DataCleaner normalization)
        primary_col = 'productdescription' if 'productdescription' in renamed_df.columns else None
        secondary_col = 'productdescription2' if 'productdescription2' in renamed_df.columns else None
        tertiary_col = 'alphaname' if 'alphaname' in renamed_df.columns else None
        
        if not primary_col:
            logger.error(f"Available columns: {renamed_df.columns.tolist()}")
            raise ValueError(f"Cannot merge descriptions: productdescription column not found (expected after DataCleaner normalization)")
        
        logger.info(f"Found description columns: Primary={primary_col}, Secondary={secondary_col or 'None'}, Tertiary={tertiary_col or 'None'}")
            
        # Merge product descriptions with explicitly identified columns
        processed_df = self.merge_product_descriptions(
            df=renamed_df,
            primary_col=primary_col,
            secondary_col=secondary_col if secondary_col else 'ProductDescription2',
            tertiary_col=tertiary_col if tertiary_col else 'AlphaName',
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
            # Verify that renamed columns exist (they should from the early rename step)
            if 'category_description' not in processed_df.columns:
                # Map productcategory to category_description (should be lowercase after DataCleaner)
                if 'productcategory' in processed_df.columns:
                    processed_df['category_description'] = processed_df['productcategory']
                    logger.info(f"Mapped productcategory to category_description with {processed_df['category_description'].nunique()} unique categories")
                else:
                    logger.warning("No productcategory column found for mapping to category_description")
            
            # Apply category description normalization after column mapping
            processed_df = self.map_category_description(processed_df)
            
            # Ensure product_code is string type for consistent merging
            if 'product_code' in processed_df.columns and not pd.api.types.is_string_dtype(processed_df['product_code']):
                processed_df['product_code'] = processed_df['product_code'].astype(str)
                logger.info(f"Converted product_code to string type for consistency")
            
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
