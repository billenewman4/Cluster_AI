import os
import sys

# Add src to path - go up two directories to reach the root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.data_ingestion.core.cleaner import DataCleaner
from src.data_ingestion.core.reader import FileReader

def test_cleaner():
    file_path = "tests/test_data/beef_cuts_test.xlsx"
    reader = FileReader()
    df = reader.read_file(file_path)
    cleaner = DataCleaner()
    df = cleaner.clean_dataframe(df, file_path)
    assert not df.empty
    print(df.head())
    return ("test_cleaner() passed")

if __name__ == "__main__":
    print(test_cleaner())