#!/usr/bin/env python3
"""
Firebase Excel Upload Testing Script
------------------------------------
Production-ready script for testing and validating Firebase Excel uploads.
This script verifies connectivity, file validity, and provides detailed results.
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("FirebaseExcelTest")

# Add parent directory to path if running directly 
current_dir = Path(__file__).resolve().parent
if current_dir not in sys.path:
    sys.path.insert(0, str(current_dir.parent.parent))

# Import required modules
from src.database.firebase_client import FirebaseClient
from src.database.examples.excel_firestore_integration import save_extraction_results_to_firestore

def verify_firebase_connection(project_id=None, credentials_path=None):
    """
    Verify Firebase connection and return project details.
    
    Args:
        project_id: Optional Firebase project ID
        credentials_path: Optional path to Firebase credentials
        
    Returns:
        dict: Connection status and project information
    """
    try:
        # Create task log
        project_root = Path(__file__).resolve().parent.parent.parent
        os.makedirs(f"{project_root}/.cline", exist_ok=True)
        timestamp = datetime.now().strftime('%d-%m-%y-%H-%M')
        
        with open(f"{project_root}/.cline/task-log_{timestamp}.log", "w") as log_file:
            log_file.write(f"GOAL: Verify Firebase connectivity and configuration\n")
            log_file.write(f"IMPLEMENTATION: Testing connection to Firebase project and checking credentials\n")
            log_file.write(f"COMPLETED: {timestamp}\n")
        
        # Initialize Firebase client
        logger.info(f"Initializing Firebase client with project_id={project_id or 'default'}")
        client = FirebaseClient(credentials_path=credentials_path, project_id=project_id)
        
        # Get project information from Firebase MCP
        project_info = {
            "status": "connected",
            "success": True,
            "project_id": client._app.project_id,
            "database_url": client._app._options.get('databaseURL', 'Not configured'),
            "storage_bucket": client._app._options.get('storageBucket', 'Not configured'),
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"Successfully connected to Firebase project: {project_info['project_id']}")
        return project_info
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        
        logger.error(f"Firebase connection error: {str(e)}")
        logger.debug(f"Error details: {error_details}")
        
        return {
            "status": "error",
            "success": False,
            "error": str(e),
            "error_type": "connection",
            "timestamp": datetime.now().isoformat()
        }

def test_excel_upload(
    excel_path, 
    collection_prefix="test_upload",
    project_id=None,
    credentials_path=None,
    verify_only=False,
    max_retries=3
):
    """
    Test and validate Excel upload to Firebase with comprehensive validation.
    
    Args:
        excel_path: Path to Excel file
        collection_prefix: Collection prefix for the test
        project_id: Firebase project ID
        credentials_path: Path to Firebase credentials
        verify_only: Only verify Excel without uploading
        max_retries: Maximum retry attempts
        
    Returns:
        dict: Test results with detailed diagnostics
    """
    start_time = datetime.now()
    
    # Create task log
    project_root = Path(__file__).resolve().parent.parent.parent
    os.makedirs(f"{project_root}/.cline", exist_ok=True)
    timestamp = start_time.strftime('%d-%m-%y-%H-%M')
    
    with open(f"{project_root}/.cline/task-log_{timestamp}.log", "w") as log_file:
        log_file.write(f"GOAL: Test Excel upload to Firebase Firestore\n")
        log_file.write(f"IMPLEMENTATION: Production-ready upload test with real data\n")
        log_file.write(f"COMPLETED: {timestamp}\n")
        
    # First verify Firebase connectivity
    connection_result = verify_firebase_connection(project_id, credentials_path)
    if not connection_result["success"]:
        return {
            "status": "failed",
            "stage": "firebase_connection",
            "success": False,
            "error": connection_result["error"],
            "timestamp": datetime.now().isoformat()
        }
    
    # Check Excel file existence and validity
    excel_file_path = Path(excel_path)
    if not excel_file_path.exists():
        return {
            "status": "failed",
            "stage": "file_validation",
            "success": False,
            "error": f"Excel file not found: {excel_path}",
            "timestamp": datetime.now().isoformat()
        }
    
    try:
        # Use the integration module for the actual upload
        dataset_identifier = f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        if verify_only:
            import pandas as pd
            df = pd.read_excel(excel_path)
            return {
                "status": "verified",
                "stage": "excel_validation",
                "success": True,
                "file_path": str(excel_file_path),
                "record_count": len(df),
                "columns": list(df.columns),
                "timestamp": datetime.now().isoformat()
            }
        
        # Perform the actual upload
        upload_result = save_extraction_results_to_firestore(
            excel_path=excel_path,
            collection_prefix=collection_prefix,
            dataset_identifier=dataset_identifier,
            project_id=project_id,
            credentials_path=credentials_path,
            max_retries=max_retries
        )
        
        # Calculate metrics
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        if upload_result["success"]:
            # Add more details to results
            stats = upload_result["stats"]
            results = {
                "status": "success",
                "stage": "completed",
                "success": True,
                "collection_name": upload_result["collection_name"],
                "dataset_id": dataset_identifier,
                "project_id": connection_result["project_id"],
                "stats": stats,
                "execution_time_seconds": execution_time,
                "timestamp": datetime.now().isoformat()
            }
            
            # Calculate records per second for performance metrics
            if stats.get("total", 0) > 0 and execution_time > 0:
                results["records_per_second"] = stats["total"] / execution_time
                
            return results
        else:
            return {
                "status": "failed",
                "stage": "upload",
                "success": False,
                "error": upload_result.get("error", "Unknown error"),
                "error_type": upload_result.get("error_type", "unknown"),
                "execution_time_seconds": execution_time,
                "timestamp": datetime.now().isoformat()
            }
            
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"Test failed: {str(e)}\n{error_details}")
        
        return {
            "status": "failed",
            "stage": "execution",
            "success": False,
            "error": str(e),
            "error_details": error_details,
            "timestamp": datetime.now().isoformat()
        }

def check_firestore_api(project_id):
    """
    Check if Firestore API is enabled for the project.
    
    Args:
        project_id: Firebase project ID to check
        
    Returns:
        dict: Status of API check
    """
    try:
        # Try to access Firestore API
        from google.cloud import firestore
        db = firestore.Client(project=project_id)
        # Try to perform a simple read operation
        db.collection('_system_test').document('_api_check').get()
        
        return {
            "status": "enabled",
            "success": True,
            "project_id": project_id
        }
    except Exception as e:
        error_str = str(e).lower()
        
        if "service_disabled" in error_str or "not been used" in error_str:
            return {
                "status": "disabled",
                "success": False,
                "project_id": project_id,
                "error": str(e),
                "activation_url": f"https://console.developers.google.com/apis/api/firestore.googleapis.com/overview?project={project_id}"
            }
        else:
            return {
                "status": "error",
                "success": False,
                "project_id": project_id,
                "error": str(e)
            }

def main():
    """Command-line interface for Firebase Excel upload testing"""
    parser = argparse.ArgumentParser(
        description="Test Firebase Excel Upload Integration",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        "--excel-file",
        default=None,
        help="Path to Excel file for testing"
    )
    
    parser.add_argument(
        "--collection",
        default="test_uploads",
        help="Collection prefix for test uploads"
    )
    
    parser.add_argument(
        "--project-id",
        help="Firebase project ID (defaults to environment config)"
    )
    
    parser.add_argument(
        "--credentials",
        help="Path to Firebase service account credentials JSON"
    )
    
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only verify connectivity without uploading data"
    )
    
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Maximum retry attempts for failed operations"
    )
    
    parser.add_argument(
        "--output-json",
        help="Write test results to JSON file"
    )
    
    # Production-ready code enforces proper API activation - no bypass options
    
    args = parser.parse_args()
    
    # If no Excel file provided, just verify Firebase connection
    if args.excel_file is None or args.verify_only:
        result = verify_firebase_connection(args.project_id, args.credentials)
        
        if result["success"]:
            print(f"✅ Firebase connection successful")
            print(f"   Project ID: {result['project_id']}")
            print(f"   Database URL: {result['database_url']}")
            print(f"   Storage Bucket: {result['storage_bucket']}")
            
            # Always check if Firestore API is enabled when we have a project ID
            # Production-ready code enforces API activation as mandatory
            if args.project_id:
                api_status = check_firestore_api(args.project_id)
                if api_status["success"]:
                    print(f"✅ Firestore API is enabled for project {args.project_id}")
                else:
                    print(f"⚠️ Firestore API is not enabled for project {args.project_id}")
                    print(f"   Error: {api_status.get('error', 'API not activated')}")
                    print(f"   To enable the API, visit: {api_status.get('activation_url')}")
                    print(f"   ❗ API activation is required for production use")
                    sys.exit(1)
        else:
            print(f"❌ Firebase connection failed: {result.get('error', 'Unknown error')}")
            sys.exit(1)
    
    # If Excel file provided, test upload
    if args.excel_file:
        # If we have a project ID, always verify API is enabled in production-ready code
        if args.project_id and not args.verify_only:
            api_status = check_firestore_api(args.project_id)
            if not api_status["success"]:
                print(f"⚠️ Firestore API is not enabled for project {args.project_id}")
                print(f"   Error: {api_status.get('error', 'API not activated')}")
                print(f"   To enable the API, visit: {api_status.get('activation_url')}")
                print(f"   ❗ API activation is required for production use")
                sys.exit(1)
        
        # Proceed with Excel upload test
        result = test_excel_upload(
            excel_path=args.excel_file,
            collection_prefix=args.collection,
            project_id=args.project_id,
            credentials_path=args.credentials,
            verify_only=args.verify_only,
            max_retries=args.retries
        )
        
        if result["status"] == "verified":
            print(f"✅ Excel file verification successful: {args.excel_file}")
            print(f"   Records: {result['record_count']}")
            print(f"   Columns: {', '.join(result['columns'])}")
        elif result["success"]:
            stats = result["stats"]
            print(f"✅ Firebase upload test successful to collection: {result['collection_name']}")
            print(f"   Dataset ID: {result['dataset_id']}")
            print(f"   Records: {stats['success']}/{stats.get('total', stats['success'])} successful")
            
            if stats.get('errors', 0) > 0:
                print(f"   Errors: {stats['errors']}")
            
            if result.get('records_per_second'):
                print(f"   Performance: {result['records_per_second']:.2f} records/second")
                
            print(f"   Time: {result['execution_time_seconds']:.2f} seconds")
        else:
            error_msg = result.get('error', 'Unknown error')
            print(f"❌ Test failed during {result['stage']} stage")
            print(f"   Error: {error_msg}")
            
            # Provide helpful guidance for API-related errors
            if "SERVICE_DISABLED" in error_msg or "not been used" in error_msg:
                activation_url = f"https://console.developers.google.com/apis/api/firestore.googleapis.com/overview?project={args.project_id}"
                print(f"   ℹ️ Firestore API needs to be enabled. Visit: {activation_url}")
            
            sys.exit(1)
    
    # Write results to JSON if requested
    if args.output_json and 'result' in locals():
        with open(args.output_json, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"📝 Results written to {args.output_json}")

if __name__ == "__main__":
    main()
