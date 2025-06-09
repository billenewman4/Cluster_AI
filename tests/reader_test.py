import os
import sys

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.data_ingestion.core.reader import FileReader


def test_read_and_validate_file():
    reader = FileReader()
    df = reader.read_file("test_data/beef_cuts_test.xlsx")
    assert not df.empty
    print(df.head())
    return ("test_read_and_validate_file() passed")

if __name__ == "__main__":
    print(test_read_and_validate_file())