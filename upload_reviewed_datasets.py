#!/usr/bin/env python3
"""
Upload reviewed Excel files as a single combined LangSmith dataset.
Uses the existing LangSmith code structure.
"""

import sys
from pathlib import Path

# Add src to path for imports
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root / "src"))

from Evals.Data_processing.langsmith.langSmithTabels import upload_combined_reviewed_files_as_single_dataset

if __name__ == "__main__":
    print("🔄 Starting upload of reviewed files as a single combined LangSmith dataset...")
    
    try:
        dataset_created = upload_combined_reviewed_files_as_single_dataset()
        
        if dataset_created:
            print(f"\n✅ Upload complete! Created dataset: '{dataset_created}'")
            print("\nYou can now run evaluations on this dataset using:")
            print(f"  python3 -c \"from src.Evals.eval_process import eval_process; eval_process('{dataset_created}')\"")
        else:
            print("\n⚠️  No dataset was created.")
            
    except Exception as e:
        print(f"\n❌ Error during upload: {e}")
        sys.exit(1) 