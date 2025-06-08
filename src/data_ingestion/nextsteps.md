# Data Ingestion Module - Next Steps

## Identified Redundancies

After detailed analysis of the data ingestion pipeline, several redundancies and optimization opportunities have been identified:

### 1. Redundant File Reading Logic

**Issue:** Multiple implementations of file reading logic exist across several modules:

| Module | File Reading Implementation | Features |
|--------|----------------------------|----------|
| `reader.py` | `FileReader.read_file()` | Encoding detection, error handling, supports multiple formats |
| `product_transformer.py` | `read_csv_with_encoding_detection()` | Encoding fallback, similar to reader.py |
| `reference_data_loader.py` | Direct use of `pd.read_excel()` | No error handling or encoding detection |
| `run_pipeline.py` | Direct use of `pd.read_csv()` | No specialized handling |
| `batch_processor.py` | Direct use of `pd.read_parquet()` | No specialized handling |

**Solution:** 
1. Enhance `FileReader` class to be the single source of truth for all file reading operations
2. Add specialized methods for common formats if needed (e.g., `read_excel_with_sheets()` for reference data)
3. Update all modules to use this centralized file reading service
4. Implement consistent error handling, encoding detection, and performance optimizations in one place

### file_utils unused

make sure that these: 
### `file_utils.py`

Provides efficient file operations and metadata extraction.

**Key Functions:**
- `ensure_directory(directory_path)`: Ensures directory exists, creating if necessary
- `find_newest_file(directory, pattern)`: Finds the newest file matching a pattern
- `batch_file_operations(file_paths, operation_fn, batch_size)`: Processes files in batches

are used in our reader.py where applicable

### Validation redundancy

# Validate expected input columns exist
            expected_cols = ['ProductDescription', 'ProductDescription2', 'ProductCategory', 'product_code']
            missing_cols = [col for col in expected_cols if not any(c for c in df.columns if c.lower() == col.lower())]
            if missing_cols:
                raise ValueError(f"Critical columns missing from product query file: {missing_cols}")
this is a redundant command in our processor.py

### 2. Column Normalization Redundancy

**Issue:** Column name standardization occurs in multiple places:
- `cleaner.py:DataCleaner.normalize_column_names()`
- `product_transformer.py:ProductTransformer.standardize_columns()`
- `run_pipeline.py:process_product_query()` also contains column mapping logic

THis also seems to be unused but should be used to simplify code here: 
def validate_dataframe_schema(
    df: pd.DataFrame,
    required_columns: List[str],
    column_types: Optional[Dict[str, type]] = None
) -> Tuple[bool, List[str]]:
    """Validate DataFrame schema against requirements efficiently.
    
    Args:
        df: DataFrame to validate
        required_columns: List of columns that must be present
        column_types: Dict mapping column names to expected types
        
    Returns:
        Tuple[bool, List[str]]: (is_valid, list of validation errors)
    """
    # List to collect validation errors
    errors = []
    
    # Check for missing columns - O(n) operation
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        errors.append(f"Missing required columns: {', '.join(missing_columns)}")
    
    # Check column types if specified
    if column_types:
        for col, expected_type in column_types.items():
            if col in df.columns:
                # Use pandas built-in type checking for efficiency
                if expected_type == str:
                    if not pd.api.types.is_string_dtype(df[col]):
                        errors.append(f"Column '{col}' should be string type")
                elif expected_type == int:
                    if not pd.api.types.is_integer_dtype(df[col]):
                        errors.append(f"Column '{col}' should be integer type")
                elif expected_type == float:
                    if not pd.api.types.is_float_dtype(df[col]):
                        errors.append(f"Column '{col}' should be float type")
                elif expected_type == bool:
                    if not pd.api.types.is_bool_dtype(df[col]):
                        errors.append(f"Column '{col}' should be boolean type")
    
    return len(errors) == 0, errors

**Solution:** Create a single robust column mapping utility that can be used by all modules.

### 3. Data Validation Duplication

**Issue:** Data validation occurs in:
- `cleaner.py:DataCleaner.validate_required_columns()`
- `validation.py:validate_dataframe_schema()`
- Separate validation in `run_pipeline.py`

**Solution:** Centralize validation in the validation.py module and use it consistently in our data ingestion pipeline then call it in run_pipeline rather than re-implementing

### 4. Product Query Processing Logic

**Issue:** There are separate implementations for product query processing:
- `product_transformer.py:ProductTransformer.process_product_query()`
- `run_pipeline.py:process_product_query()`

**Solution:** Consolidate this functionality in the ProductTransformer class.

### 5. Missing Deduplication Strategy

**Issue:** Product code deduplication is handled inconsistently:
- Added manually in `run_pipeline.py` before batch processing
- Not systematically applied in the data processing pipeline

**Solution:** Implement a consistent deduplication strategy in cleaner.py or processor.py.

### 6. Inefficient Parallel Processing

**Issue:** Parallel processing in DataProcessor.process_all_files() is not optimally batched.

**Solution:** Implement better batching and resource management strategies.

### 7. Inconsistent File Path Handling

**Issue:** Mixture of string paths and Path objects across the codebase.

**Solution:** Standardize on pathlib.Path throughout all modules.

