# Converts Excel file in evals/data/ground_truth to parquet file if not already present

import pandas as pd
from pathlib import Path
import sys

# Add project root to path for imports
project_root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(project_root))

from src.data_ingestion.core.cleaner import DataCleaner

def convert_excel_to_parquet(
    file_path: Path = Path("src/Evals/data/ground_truth/meat_inventory_master_20250610_231446.xlsx"), 
    output_path: Path = Path("src/Evals/data/ground_truth/ground_truth.parquet")
):
    """
    Convert Excel file to Parquet format using our data ingestion pipeline.
    
    Args:
        file_path: Path to the input Excel file
        output_path: Path for the output Parquet file
    """
    # Read the Excel file
    df = pd.read_excel(file_path)
    
    # Use our data ingestion pipeline to normalize column names
    cleaner = DataCleaner()
    df = cleaner.normalize_column_names(df)
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write to parquet
    df.to_parquet(output_path, index=False)
    
    print(f"Converted {file_path} to {output_path}")
    print(f"Columns: {list(df.columns)}")
    print(f"Shape: {df.shape}")

if __name__ == "__main__":
    convert_excel_to_parquet()