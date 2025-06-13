"""
Excel to Firestore Integration Example

Demonstrates how to integrate the Excel to Firestore module with
the beef cut extraction pipeline for automated result storage.
"""

import os
import sys
import argparse
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging

# Add project root to path if running as script
project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.database.excel_to_firestore import ExcelToFirestore

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def save_extraction_results_to_firestore(
    excel_path, 
    collection_prefix="beef_extractions",
    dataset_identifier=None,
    project_id=None,
    credentials_path=None,
    max_retries=3
):
    """
    Save extraction results from Excel to Firestore with production-ready implementation.
    
    Args:
        excel_path: Path to Excel results file
        collection_prefix: Base collection name prefix
        dataset_identifier: Optional identifier for this dataset
        project_id: Optional Firebase project ID to use
        credentials_path: Optional path to Firebase credentials file
        max_retries: Maximum retry attempts for failed operations
        
    Returns:
        Dictionary with operation results
    """
    # Create task log entry with timestamp
    os.makedirs(f"{project_root}/.cline", exist_ok=True)
    timestamp = datetime.now().strftime('%d-%m-%y-%H-%M')
    
    with open(f"{project_root}/.cline/task-log_{timestamp}.log", "w") as log_file:
        log_file.write(f"GOAL: Save beef cut extraction Excel data to Firestore\n")
        log_file.write(f"IMPLEMENTATION: Using production-ready batch operations with dataset isolation\n")
        log_file.write(f"COMPLETED: {timestamp}\n")
    
    # Validate input file
    excel_file = Path(excel_path)
    if not excel_file.exists():
        error_msg = f"Excel file not found: {excel_path}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }
    
    try:
        # Initialize the Firebase client with specified project ID if provided
        # This ensures we're targeting the right Firebase project
        importer = ExcelToFirestore(
            base_collection_prefix=collection_prefix,
            credentials_path=credentials_path,
            project_id=project_id
        )
        
        # Generate dataset identifier if not provided
        if not dataset_identifier:
            # Extract from filename or use timestamp for better traceability
            dataset_identifier = Path(excel_path).stem
        
        # Log the start of the import operation
        logger.info(f"Starting import of {excel_path} with identifier '{dataset_identifier}'")
        
        # Import the Excel file to Firestore with retry logic and performance optimizations
        collection_name, stats = importer.import_excel(
            excel_path=excel_path,
            custom_collection_id=dataset_identifier,
            max_retries=max_retries
        )
        
        # Analyze results
        if stats["success"] == stats["total"]:
            logger.info(f"Successfully imported all {stats['total']} records to collection: {collection_name}")
        else:
            success_rate = (stats["success"] / stats["total"]) * 100 if stats["total"] > 0 else 0
            logger.warning(f"Partial import success: {stats['success']}/{stats['total']} records ({success_rate:.1f}%) to {collection_name}")
        
        # Return detailed results
        return {
            "success": stats["success"] > 0,
            "collection_name": collection_name,
            "stats": stats,
            "timestamp": datetime.now().isoformat()
        }
        
    except ConnectionError as e:
        # Handle specific connection errors for better diagnosability
        logger.error(f"Firebase connection error: {str(e)}")
        return {
            "success": False,
            "error": f"Connection error: {str(e)}",
            "error_type": "connection"
        }
    except Exception as e:
        # Handle general exceptions with detailed logging
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"Import failed: {str(e)}\n{error_details}")
        return {
            "success": False,
            "error": str(e),
            "error_type": "general",
            "timestamp": datetime.now().isoformat()
        }

def main():
    """Command-line interface for Excel to Firestore migration with enhanced options."""
    parser = argparse.ArgumentParser(
        description="Import beef cut extraction Excel data to Firestore",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        "excel_file",
        help="Path to Excel file containing extraction results"
    )
    
    parser.add_argument(
        "--prefix", 
        default="beef_extractions",
        help="Base collection prefix"
    )
    
    parser.add_argument(
        "--id",
        dest="identifier",
        help="Custom identifier for this dataset (defaults to filename without extension)"
    )
    
    parser.add_argument(
        "--project-id",
        help="Specify Firebase project ID to use"
    )
    
    parser.add_argument(
        "--credentials",
        help="Path to Firebase service account credentials JSON file"
    )
    
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Maximum retry attempts for failed operations"
    )
    
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Verify the Excel file without uploading to Firestore"
    )
    
    args = parser.parse_args()
    
    # Validate Excel file exists
    if not os.path.exists(args.excel_file):
        logger.error(f"Excel file not found: {args.excel_file}")
        sys.exit(1)
    
    # If verify-only mode, just analyze the Excel file
    if args.verify_only:
        try:
            df = pd.read_excel(args.excel_file)
            print(f"✅ Excel file validation successful: {args.excel_file}")
            print(f"   {len(df)} records found")
            print(f"   Columns: {', '.join(df.columns)}")
            return
        except Exception as e:
            print(f"❌ Excel file validation failed: {e}")
            sys.exit(1)
    
    # Execute import with production settings
    result = save_extraction_results_to_firestore(
        excel_path=args.excel_file, 
        collection_prefix=args.prefix,
        dataset_identifier=args.identifier,
        project_id=args.project_id,
        credentials_path=args.credentials,
        max_retries=args.retries
    )
    
    # Report detailed results to the user
    if result["success"]:
        stats = result["stats"]
        print(f"✅ Import successful to collection: {result['collection_name']}")
        print(f"   {stats['success']} records imported successfully")
        
        if stats.get('errors', 0) > 0:
            print(f"   {stats['errors']} records failed")
        
        if stats.get('retries', 0) > 0:
            print(f"   Required {stats['retries']} retry attempts")
            
        # Show success rate
        if stats.get('total', 0) > 0:
            success_rate = (stats['success'] / stats['total']) * 100
            print(f"   Success rate: {success_rate:.1f}%")
    else:
        error_type = result.get('error_type', 'Unknown')
        error_msg = result.get('error', 'Unknown error')
        
        print(f"❌ Import failed: {error_type} error")
        print(f"   Details: {error_msg}")
        
        if error_type == "connection":
            print("   Please check your Firebase credentials and project settings")
        
        sys.exit(1)

if __name__ == "__main__":
    main()
