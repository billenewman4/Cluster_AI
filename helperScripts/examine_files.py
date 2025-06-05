#!/usr/bin/env python3
"""Examine supplier file structures"""

import pandas as pd
import sys
sys.path.append('src')

def examine_files():
    """Examine the structure of supplier files."""
    files_to_check = [
        'data/incoming/Queen Inventory Valuation.xls',
        'data/incoming/Transaction_Report_Actual.xlsx',
        'data/incoming/Fulton Inventory Valuation.xls'
    ]
    
    for file_path in files_to_check:
        try:
            print(f"\n{'='*50}")
            print(f"Examining: {file_path}")
            print(f"{'='*50}")
            
            df = pd.read_excel(file_path, nrows=5)
            print(f'Columns ({len(df.columns)}): {list(df.columns)}')
            print(f'\nFirst few rows:')
            print(df.head())
            
        except Exception as e:
            print(f'Error reading {file_path}: {e}')

if __name__ == "__main__":
    examine_files() 