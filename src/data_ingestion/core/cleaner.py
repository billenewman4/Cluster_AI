"""
Data Cleaner Module
Handles data cleaning, normalization, and column mapping with optimized operations.
"""

import pandas as pd
import numpy as np
import logging
import re
from typing import Dict, List, Optional, Union

logger = logging.getLogger(__name__)

class DataCleaner:
    """Handles data cleaning and normalization with optimized algorithms."""
    
    REQUIRED_COLUMNS = ['product_code', 'product_description', 'category_description']
    
    def normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize column names to match expected format.
        
        Args:
            df: Input DataFrame with raw column names
            
        Returns:
            pd.DataFrame: DataFrame with normalized columns
        """
        # Define standardized column mapping
        column_mapping = {
            'product_code': 'product_code',
            'item_code': 'product_code',
            'code': 'product_code',
            'sku': 'product_code',
            'product description 1': 'product_description',
            'product_description': 'product_description',
            'description': 'product_description',
            'product_name': 'product_description',
            'item_name': 'product_description',
            'category': 'category_description',
            'category_description': 'category_description',
            'product category': 'category_description',
            'productcategory': 'category_description',
            'department': 'category_description',
            'group': 'category_description'
        }

        # Create a more robust column mapping that handles various naming patterns
        robust_mapping = {}
        
        # O(1) transformation of input columns
        for col in df.columns:
            if col is None:
                continue
                
            # Try exact match first (most efficient)
            if col in column_mapping:
                robust_mapping[col] = column_mapping[col]
                continue
                
            # Try lowercase version (handles capitalized column names)
            col_lower = str(col).lower().strip()
            if col_lower in column_mapping:
                robust_mapping[col] = column_mapping[col_lower]
                continue
                
            # Handle special cases with more complex transformations
            col_no_spaces = col_lower.replace(' ', '')
            if col_no_spaces in column_mapping:
                robust_mapping[col] = column_mapping[col_no_spaces]
                continue
                
            # Handle specific case for ProductCategory (CamelCase)
            if col_lower == 'productcategory' or col == 'ProductCategory':
                robust_mapping[col] = 'category_description'
        
        # Apply rename in a single operation instead of iterative changes
        if robust_mapping:
            df = df.rename(columns=robust_mapping)
        
        # Lowercase all column names for consistency across the pipeline
        df.columns = [col.lower() for col in df.columns]
            
        return df
    
    def clean_string_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean string data in DataFrame.
        
        Args:
            df: DataFrame with string columns
            
        Returns:
            pd.DataFrame: DataFrame with cleaned string columns
        """
        # Create a copy to avoid modifying the original
        df = df.copy()
        
        # Identify string columns once for efficiency
        string_columns = df.select_dtypes(include=['object']).columns
        
        for col in string_columns:
            # Apply vectorized string operations instead of row-by-row
            if col in df.columns:
                # Replace null values with empty string to avoid errors
                mask = df[col].notna()
                if mask.any():  # Only process if there are non-null values
                    # Vectorized string operations
                    df.loc[mask, col] = df.loc[mask, col].str.strip()
                
        return df
    
    def categorize_descriptions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Categorize product descriptions if category is missing.
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with categories assigned
        """
        # Check if categorization is needed
        if 'category_description' in df.columns and df['category_description'].notna().all():
            return df
            
        df = df.copy()
        
        # Precompile keyword patterns for common meat categories
        category_patterns = {
            'Beef Chuck': re.compile(r'\b(beef\s+chuck|chuck\s+beef|shoulder\s+clod|flat\s+iron|chuck\s+roll)\b', re.IGNORECASE),
            'Beef Loin': re.compile(r'\b(beef\s+loin|tenderloin|filet\s+mignon|strip\s+loin|porterhouse|t-bone)\b', re.IGNORECASE),
            'Beef Rib': re.compile(r'\b(beef\s+rib|ribeye|prime\s+rib|rib\s+roast|rib\s+steak|tomahawk)\b', re.IGNORECASE),
            'Pork': re.compile(r'\b(pork|ham|bacon|loin|tenderloin|shoulder|boston\s+butt|spare\s*ribs|belly)\b', re.IGNORECASE),
            'Chicken': re.compile(r'\b(chicken|broiler|fryer|roaster|breast|thigh|leg|wing|drumstick)\b', re.IGNORECASE),
            'Lamb': re.compile(r'\b(lamb|mutton|rack|loin|leg|shank|shoulder)\b', re.IGNORECASE)
        }
        
        # Initialize category column if missing
        if 'category_description' not in df.columns:
            df['category_description'] = None
            
        # Get indices where category is missing
        missing_category = df['category_description'].isna()
        
        if missing_category.any() and 'product_description' in df.columns:
            for category, pattern in category_patterns.items():
                # Use vectorized operations for matching
                mask = missing_category & df['product_description'].str.contains(pattern, na=False)
                df.loc[mask, 'category_description'] = category
                
            # Set default for anything still uncategorized
            still_missing = df['category_description'].isna()
            df.loc[still_missing, 'category_description'] = 'Uncategorized'
            
        return df
    
    def validate_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure all required columns are present.
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with required columns
        """
        # Check for missing required columns
        missing_columns = [col for col in self.REQUIRED_COLUMNS if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"Missing required columns: {missing_columns}")
            
            # Create missing columns with None values
            for col in missing_columns:
                df[col] = None
                
        return df
                
    def clean_dataframe(self, df: pd.DataFrame, source_filename: str) -> pd.DataFrame:
        """Clean and normalize DataFrame.
        
        Args:
            df: Input DataFrame
            source_filename: Name of the source file
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        if df.empty:
            logger.warning(f"Empty DataFrame from {source_filename}")
            return pd
            
        # Add source filename and row number for tracking
        df['source_filename'] = source_filename
        df['row_number'] = np.arange(len(df))
        
        # Apply cleaning operations in sequence
        df = self.normalize_column_names(df)
        df = self.clean_string_data(df)
        df = self.validate_required_columns(df)
        df = self.categorize_descriptions(df)
        
        return df
