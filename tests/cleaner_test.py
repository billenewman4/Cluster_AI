import os
import sys

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.data_ingestion.core.cleaner import DataCleaner


def test_cleaner():
    cleaner = DataCleaner()
    df = cleaner.clean_file("test_data/beef_cuts_test.xlsx")
    assert not df.empty
    print(df.head())
    return ("test_cleaner() passed")

if __name__ == "__main__":
    print(test_cleaner())