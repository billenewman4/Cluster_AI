1. Change cache function to cache approved families rather than what it is currently doing



Questions:

1. why does this exists: 
        # Create case-insensitive column lookup for finding columns regardless of case
        col_map = {col.lower(): col for col in renamed_df.columns}

And this in data cleanser:


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
            
        return df


        2. Why is this in run_pipeline.py ... it seems like this function already exists in our data ingestion: (clean or product_transformer)

         # Validate expected input columns exist
        expected_cols = ['ProductDescription', 'ProductDescription2', 'ProductCategory']
        missing_cols = [col for col in expected_cols if not any(c for c in df.columns if c.lower() == col.lower())]
        if missing_cols:
            raise ValueError(f"Critical columns missing from product query file: {missing_cols}. Cannot continue processing.")


    3. Why isn't this using our reader.py? or reference_data_loader.py?         # Read the CSV file
        df = pd.read_csv(query_file)

4. - `validate_consistency(df, rules)`: Validates data consistency using custom rules (Is this used anywhere and is it necessary?)
            

            

