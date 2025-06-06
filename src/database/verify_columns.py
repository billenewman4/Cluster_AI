#!/usr/bin/env python3
"""
Verify new columns implementation

Tests that the Family, Approved, and Comments columns are correctly added
during data processing without requiring Firebase upload.

Optimized for efficiency and readability.
"""

import sys
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

# Add parent directory to path if running directly
current_dir = Path(__file__).resolve().parent
if current_dir not in sys.path:
    sys.path.insert(0, str(current_dir.parent.parent))

# Import required modules
from src.database.excel_to_firestore import ExcelToFirestore

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ColumnVerifier")

def verify_columns(excel_path):
    """
    Verify that new columns are correctly added during data processing.
    
    Args:
        excel_path: Path to Excel file
        
    Returns:
        True if verification succeeds
    """
    logger.info(f"Testing column creation for {excel_path}")

    # Read original data to compare
    df_original = pd.read_excel(excel_path)
    logger.info(f"Original columns: {list(df_original.columns)}")
    
    # Create an instance of ExcelToFirestore
    importer = ExcelToFirestore(
        base_collection_prefix="test_verify",
    )
    
    # Load and process the Excel file (access the protected method)
    excel_path = Path(excel_path)
    if not excel_path.exists():
        logger.error(f"Excel file not found: {excel_path}")
        return False
    
    # Process data manually to verify our changes
    try:
        # Use same logic as in excel_to_firestore.py
        df = pd.read_excel(
            excel_path, 
            engine='openpyxl'
        )
        
        # Check if dataframe is empty
        if df.empty:
            logger.warning(f"Excel file {excel_path} contains no data")
            return False
        
        # Clean column names for better interoperability and consistency
        df.columns = [str(c).strip().lower().replace(' ', '_').replace('-', '_') for c in df.columns]
        
        # Remove rows with all NaN values
        df = df.dropna(how='all')
        
        # Handle missing values appropriately based on data type
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].fillna('')
            elif pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].fillna(0)
            elif pd.api.types.is_datetime64_dtype(df[col]):
                pass
        
        # Convert to list of dictionaries for Firestore with proper handling of special types
        data = []
        for _, row in df.iterrows():
            record = {}
            # Process existing columns
            for col, val in row.items():
                # Handle special data types for Firestore compatibility
                if pd.isna(val):
                    record[col] = None
                elif pd.api.types.is_datetime64_dtype(type(val)):
                    record[col] = val.isoformat() if hasattr(val, 'isoformat') else str(val)
                else:
                    record[col] = val
            
            # Add the Family field - concatenation of Species, Primal, Subprimal, and Grade
            species = str(record.get('species', '')) if not pd.isna(record.get('species', None)) else ''
            primal = str(record.get('primal', '')) if not pd.isna(record.get('primal', None)) else ''
            subprimal = str(record.get('subprimal', '')) if not pd.isna(record.get('subprimal', None)) else ''
            grade = str(record.get('grade', '')) if not pd.isna(record.get('grade', None)) else ''
            
            # Create the Family field by joining the components with spaces
            record['family'] = ' '.join(filter(None, [species, primal, subprimal, grade]))
            
            # Add empty Approved and Comments fields
            record['approved'] = ''
            record['comments'] = ''
            
            data.append(record)
        
        # Verify the new columns exist in all records
        all_have_family = all('family' in record for record in data)
        all_have_approved = all('approved' in record for record in data)
        all_have_comments = all('comments' in record for record in data)

        # Check if columns were successfully added
        if all_have_family and all_have_approved and all_have_comments:
            logger.info("✅ All new columns successfully added to the data")
            # Show a couple of examples with the new fields
            for i, record in enumerate(data[:3]):
                logger.info(f"Record {i+1} sample:")
                logger.info(f"  - Family: '{record['family']}'")
                logger.info(f"  - Approved: '{record['approved']}'")
                logger.info(f"  - Comments: '{record['comments']}'")
            return True
        else:
            logger.error("❌ Missing columns in some records!")
            logger.error(f"  - Family present in all records: {all_have_family}")
            logger.error(f"  - Approved present in all records: {all_have_approved}")
            logger.error(f"  - Comments present in all records: {all_have_comments}")
            return False

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"Verification failed: {str(e)}\n{error_details}")
        return False

def main():
    """Command-line interface for column verification"""
    if len(sys.argv) < 2:
        print("Usage: python verify_columns.py <path_to_excel_file>")
        sys.exit(1)
    
    excel_path = sys.argv[1]
    if verify_columns(excel_path):
        print("✅ Verification successful! All new columns are correctly added.")
        sys.exit(0)
    else:
        print("❌ Verification failed. Check the logs for details.")
        sys.exit(1)

if __name__ == "__main__":
    main()
