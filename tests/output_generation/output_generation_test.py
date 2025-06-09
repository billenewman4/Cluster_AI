# Test extraction for a single product

import os
import sys
import pandas as pd

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.llm_extraction.extraction_controller import ExtractionController
from src.data_ingestion.processor import DataProcessor
from src.output_generation.file_writer import FileWriter

# Define paths relative to the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
reference_data_path = os.path.join(project_root, "data", "incoming", "beef_cuts.xlsx")
test_data_path = os.path.join(project_root, "tests", "test_data", "Product_Query_2025_06_06_test.csv")

def test_single_extraction():
    # Initialize extraction controller with correct paths
    extractor = ExtractionController(reference_data_path=reference_data_path)
    
    # Use the processor to process the test data
    processor = DataProcessor()
    df = processor.process_file(test_data_path, category=['Beef Chuck'], limit_per_category=1)
    
    print(df.head())
    
    # Extract data
    results_df = extractor.extract_batch(df)
    
    # Write outputs
    writer = FileWriter()
    writer.write_all_outputs(df, results_df)
    
    return "test_single_extraction() completed"

if __name__ == "__main__":
    print(test_single_extraction())
