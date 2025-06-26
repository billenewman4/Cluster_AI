"""
Product Transformer Module
Handles specialized transformations for product data including description merging,
column renaming, and standardization.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Union, Set, Tuple
import re
from pathlib import Path

# Import ProductData for standardized data management
from src.models.product_model import ProductData

from .reader import FileReader

logger = logging.getLogger(__name__)

class ProductTransformer:
    """Specialized transformer for product data processing."""
    
    def __init__(self):
        """Initialize the product transformer with dependencies."""
        self.file_reader = FileReader()
        
    def merge_product_descriptions(self, 
                              df: pd.DataFrame, 
                              list_of_columns: List[str],
                              output_col: str = 'product_description') -> pd.DataFrame:
        """Merge product description columns with specific rules."""
        # Find column regardless of case sensitivity
        available_cols = {col.lower(): col for col in df.columns}
        actual_columns = []
    
        for col in list_of_columns:
            if col.lower() in available_cols:
                actual_columns.append(available_cols[col.lower()])
            else:
                raise ValueError(f"Column '{col}' not found in DataFrame")
    
        if not actual_columns:
            raise ValueError("No valid columns found to merge")
    
        # Create a copy to avoid modifying the original
        result_df = df.copy()
    
        # Check if output column already exists
        if output_col in result_df.columns:
            # Use existing output column as the starting point
            current_values = result_df[output_col].fillna('').astype(str).str.strip()
            
            # Only append other columns to it
            for col in actual_columns:
                if col != output_col:  # Skip the output column if it's in the list
                    values = df[col].fillna('').astype(str).str.strip()
                    # Only add space and value if values are not empty
                    current_values = current_values.str.cat(values, sep=' ', na_rep='')
                    
            result_df[output_col] = current_values
        else:
            # Initialize with the first column
            first_col = actual_columns[0]
            result_df[output_col] = df[first_col].fillna('').astype(str).str.strip()
            
            # Add remaining columns
            for col in actual_columns[1:]:
                values = df[col].fillna('').astype(str).str.strip()
                result_df[output_col] = result_df[output_col].str.cat(values, sep=' ', na_rep='')
        
        # Clean up multiple spaces
        result_df[output_col] = result_df[output_col].str.replace(r'\s+', ' ', regex=True).str.strip()
    
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
        if 'productcode' not in df.columns:
            logger.warning("No productcode column found, cannot filter cached products")
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
                product_code = row['productcode']
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
    
    def process_product_data(self, df: pd.DataFrame) -> List[ProductData]:
        """Apply all product transformations in optimal sequence.
        
        Args:
            df: DataFrame to process
            
        Returns:
            List[ProductData]: List of processed product data objects
        """
        if df.empty:
            raise ValueError("Empty DataFrame provided")
            
        # Debug input data
        logger.info(f"Processing product data: {len(df)} records with columns {df.columns.tolist()}")
        
        # Remove cached products if product_code column exists
        if 'productcode' in df.columns:
            df = self.remove_cached_products(df)
        else:
            logger.warning("No productcode column found, cannot filter cached products")
            
        if len(df) == 0:
            raise ValueError("All products were already cached or filtered out, nothing to process")

        # Use the DataCleaner to normalize column names for consistent processing
        from src.data_ingestion.core.cleaner import DataCleaner
        data_cleaner = DataCleaner()
        df = data_cleaner.normalize_column_names(df)
        logger.info(f"Normalized column names: {df.columns.tolist()}")
        
        # Remove duplicate product codes
        df = self.unique_product_codes(df)
        
        # Debug the columns after cleaning
        logger.info(f"Columns after cleaning: {df.columns.tolist()}")
        
        # Create a list of columns for product description
        list_of_columns = ['productdescription', 'productdescription2', 'alphaname']
        
        # Merge product descriptions
        processed_df = self.merge_product_descriptions(
            df=df,
            list_of_columns=list_of_columns,
            output_col='productdescription'
        )
        
        # Apply category description normalization
        processed_df = self.map_category_description(processed_df)
        
        # Ensure productcode is string type for consistent behavior
        if 'productcode' in processed_df.columns and not pd.api.types.is_string_dtype(processed_df['productcode']):
            processed_df['productcode'] = processed_df['productcode'].astype(str)
            logger.info("Converted productcode to string type for consistency")
        
        # Validate required columns exist using ProductData's standard required fields
        required_cols = ProductData.get_required_field_names()
        missing_cols = [col for col in required_cols if col not in processed_df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns after processing: {missing_cols}")
        
        # Validate product descriptions - no nulls or empty strings allowed
        if 'productdescription' in processed_df.columns:
            empty_descriptions = (processed_df['productdescription'].isna() | 
                                (processed_df['productdescription'] == ''))
            empty_count = empty_descriptions.sum()
            
            if empty_count > 0:
                logger.warning(f"Found {empty_count} records with null or empty product descriptions")
                
                # Drop records with empty descriptions to avoid downstream errors
                processed_df = processed_df[~empty_descriptions].copy()
                logger.info(f"Removed {empty_count} records with empty descriptions. {len(processed_df)} records remaining.")
                
                # If all records were dropped, raise ValueError to prevent further processing
                if len(processed_df) == 0:
                    raise ValueError(f"All {empty_count} records had empty product descriptions. Pipeline cannot continue.")

        # Convert DataFrame to list of ProductData objects - only include fields that ProductData expects
        product_data_list = []
        for _, row in processed_df.iterrows():
            # Get required fields from row
            product_data = {
                'productcode': row.get('productcode', ''),
                'productdescription': row.get('productdescription', ''),
                'category_description': row.get('category_description', '')
            }
            
            # Add any optional fields that may be present
            for field in ['subprimal', 'grade', 'size', 'size_uom', 'brand', 'bone_in', 'family',
                        'approved', 'comments', 'species', 'confidence', 'needs_review']:
                if field in row:
                    product_data[field] = row[field]
            
            # Create ProductData object and add to list
            product_data_list.append(ProductData(**product_data))
        
        # Log final statistics
        logger.info(f"Final validation successful. Processed {len(processed_df)} records with required columns present.")
        
        return product_data_list
        
    def read_and_process_product_csv(self, 
                                file_path: Union[str, Path],
                                **kwargs) -> List[ProductData]:
        """
        Read a product CSV file and process its contents.
        
        Args:
            file_path: Path to the product CSV file
            **kwargs: Additional parameters to pass to the file reader
            
        Returns:
            List[ProductData]: List of processed product data objects
        """
        # Convert string to Path if needed - do this only once
        path_obj = Path(file_path) if isinstance(file_path, str) else file_path
        
        try:
            # Use the centralized file reader for consistent handling and optimal performance
            df = self.file_reader.read_file(path_obj)
            logger.info(f"Read {len(df)} records from {path_obj.name}")
            
            return self.process_product_data(df)
            
        except Exception as e:
            logger.error(f"Error processing {path_obj.name}: {str(e)}")
            return []
