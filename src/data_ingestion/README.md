# Data Ingestion Module

## Overview

The data_ingestion module provides a comprehensive solution for efficiently loading, cleaning, transforming, and processing product data from various file formats. It's designed with a modular architecture to handle different aspects of the data pipeline, including file reading, data cleaning, normalization, validation, reference data integration, and product transformation.

## Directory Structure

```
data_ingestion/
├── __init__.py
├── core/
│   ├── __init__.py
│   ├── cleaner.py
│   ├── processor.py
│   ├── product_transformer.py
│   └── reader.py
└── utils/
    ├── __init__.py
    ├── file_utils.py
    ├── reference_data_loader.py
    └── validation.py
```

## Core Modules

### `reader.py`

The FileReader class provides optimized file reading capabilities for different file formats.

**Key Functions:**
- `get_supported_files(directory)`: Lists all supported files (Excel, CSV, TSV) in a directory
- `read_file(file_path)`: Reads file data with format-specific optimizations including encoding detection and error handling

### `cleaner.py`

The DataCleaner class handles data cleaning, normalization, categorization, and validation.

**Key Functions:**
- `normalize_column_names(df)`: Maps and standardizes column names using intelligent matching
- `clean_string_data(df)`: Cleans string data using vectorized operations
- `validate_required_columns(df)`: Ensures required columns exist in the DataFrame
- `categorize_descriptions(df)`: Categorizes product descriptions using regex patterns
- `clean_dataframe(df, source_name)`: Main entry point that applies all cleaning steps

### `processor.py`

The DataProcessor class orchestrates the entire data ingestion pipeline.

**Key Functions:**
- `process_file(file_path)`: Processes a single file with appropriate cleaning
- `process_all_files(max_workers)`: Processes multiple files in parallel
- `save_processed_data(df, filename)`: Saves the processed data to parquet format
- `run()`: Runs the complete data ingestion pipeline

### `product_transformer.py`

The ProductTransformer class handles specialized transformations for product data.

**Key Functions:**
- `merge_product_descriptions(df, primary_col, secondary_col, output_col)`: Merges multiple description columns
- `rename_brand_column(df, input_col, output_col)`: Standardizes brand column names
- `process_product_query(file_path)`: Processes product query files with specialized transformations
- `standardize_columns(df, column_mapping)`: Standardizes column names per mapping
- `read_csv_with_encoding_detection(file_path)`: Reads CSV with automatic encoding detection

## Utils Modules

### `reference_data_loader.py`

The ReferenceDataLoader class loads and manages reference data for beef extraction.

**Key Functions:**
- `get_primals()`: Lists all primal cuts
- `get_subprimals(primal)`: Gets subprimal cuts for a primal
- `get_subprimal_synonyms(primal, subprimal)`: Gets synonyms for a subprimal cut
- `get_grades()`: Lists all official grade names
- `get_grade_synonyms(grade)`: Gets synonyms for a specific grade
- `get_synonyms(term_type, term_name, primal)`: Generic method for retrieving synonyms

### `validation.py`

Provides functions for validating data schema and detecting anomalies.

**Key Functions:**
- `validate_dataframe_schema(df, required_columns, column_types)`: Validates DataFrame schema
- `validate_consistency(df, rules)`: Validates data consistency using custom rules (Is this used anywhere and is it necessary?)

### `file_utils.py`

Provides efficient file operations and metadata extraction.

**Key Functions:**
- `ensure_directory(directory_path)`: Ensures directory exists, creating if necessary
- `find_newest_file(directory, pattern)`: Finds the newest file matching a pattern
- `batch_file_operations(file_paths, operation_fn, batch_size)`: Processes files in batches

## Usage Examples

### Basic Data Ingestion Pipeline

```python
from data_ingestion.core.processor import DataProcessor

# Initialize the processor
processor = DataProcessor(incoming_dir="data/incoming", processed_dir="data/processed")

# Run the full pipeline
processed_data = processor.run()
```

### Reference Data Access

```python
from data_ingestion.utils.reference_data_loader import ReferenceDataLoader

# Initialize the loader
ref_loader = ReferenceDataLoader(data_path="data/incoming/beef_cuts.xlsx")

# Get all primal cuts
primals = ref_loader.get_primals()

# Get subprimals for a specific primal
subprimals = ref_loader.get_subprimals("Chuck")

# Get synonyms for a specific subprimal
synonyms = ref_loader.get_subprimal_synonyms("Chuck", "Chuck Roll")
```

### Specialized Product Transformation

```python
from data_ingestion.core.product_transformer import ProductTransformer

# Initialize the transformer
transformer = ProductTransformer()

# Process a product query file
processed_df = transformer.process_product_query("data/incoming/Product_Query_2025_06_06.csv")
```
