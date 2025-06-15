# Reads the generated parquet file and extracts the product code, description, subprimal and grade

import pandas as pd
from pathlib import Path

def find_descr_subprimal_grade(parquet_path: Path = Path("evals/data/processed/beef_cuts.parquet")):
    # Read the parquet file
    df = pd.read_parquet(parquet_path)

    # Extract the product code, description, subprimal and grade
    df = df[["product_code", "product_description", "subprimal", "grade"]]
    return df