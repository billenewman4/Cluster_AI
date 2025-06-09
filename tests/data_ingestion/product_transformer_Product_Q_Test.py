import os
import sys

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.data_ingestion.core.product_transformer_Product_Q import ProductTransformer
from src.data_ingestion.core.reader import FileReader
from src.data_ingestion.core.cleaner import DataCleaner

file_path = "tests/test_data/Product_Query_2025_06_06_test.csv"


def test_cleaner():
    reader = FileReader()
    df = reader.read_file(file_path)
    cleaner = DataCleaner()
    df = cleaner.clean_dataframe(df, file_path)
    transformer = ProductTransformer()
    df = transformer.process_product_data(df)
    
    #check that the dataframe is not empty
    assert not df.empty
    #check that product_description is not empty
    assert not df['product_description'].empty
    #check that brand_name column exists
    assert 'brand_name' in df.columns
    
    # Check that some brand names exist (but allow nulls since that's realistic)
    non_null_brands = df['brand_name'].dropna()
    assert len(non_null_brands) > 0, "Should have at least some non-null brand names"
    
    # Print statistics
    total_records = len(df)
    null_brands = df['brand_name'].isnull().sum()
    print(f"Total records: {total_records}")
    print(f"Records with brand names: {total_records - null_brands}")
    print(f"Records without brand names: {null_brands}")
    print(f"Percentage with brands: {((total_records - null_brands) / total_records * 100):.1f}%")
    print()
    print("Sample data:")
    print(df[['product_code', 'product_description', 'brand_name']].head())
    
    return ("test_cleaner() passed")

if __name__ == "__main__":
    print(test_cleaner())