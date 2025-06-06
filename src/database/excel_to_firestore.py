"""
Excel to Firestore Data Migration

Provides optimized functionality for importing Excel data into Firestore
with run-specific collections to maintain data isolation between processing runs.
"""

import os
import sys
import re
import json
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# Import Firebase client
from src.database.firebase_client import FirebaseClient

# Configure logging
logger = logging.getLogger(__name__)

class ExcelToFirestore:
    """
    Handles migration of Excel data to Firestore with run isolation.
    
    Each execution creates a new collection to avoid mixing data from
    different processing runs. Provides optimal batch operations and
    progress tracking.
    """
    
    def __init__(
        self,
        base_collection_prefix: str = "beef_cuts",
        batch_size: int = 100,
        project_id: Optional[str] = None,
        credentials_path: Optional[str] = None
    ):
        """
        Initialize the Excel to Firestore migration handler with production-ready configuration.
        
        Args:
            base_collection_prefix: Prefix for all Firestore collections
            batch_size: Number of documents to process in each batch for optimal performance
            project_id: Optional Firebase project ID for explicit targeting
            credentials_path: Optional path to Firebase service account credentials JSON file
        """
        self.base_prefix = base_collection_prefix
        self.batch_size = batch_size
        
        # Initialize Firebase client with provided credentials and project ID
        # This ensures we're connecting to the correct project with proper authentication
        self.firebase_client = FirebaseClient(
            credentials_path=credentials_path,
            collection_name=base_collection_prefix,
            project_id=project_id
        )
    
    def generate_collection_name(self, custom_identifier: Optional[str] = None) -> str:
        """
        Generate a timestamped collection name to ensure data isolation between runs.
        
        Args:
            custom_identifier: Optional custom identifier to include in collection name
            
        Returns:
            Unique collection name for this processing run
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Check if custom_identifier already contains a timestamp to avoid duplication
        has_timestamp = False
        if custom_identifier and re.search(r'\d{8}_\d{6}', custom_identifier):
            has_timestamp = True
            
        if custom_identifier:
            # Clean identifier to ensure it's valid for Firestore
            clean_id = ''.join(c if c.isalnum() or c == '_' else '_' for c in custom_identifier)
            
            # Avoid duplicating the base prefix if the custom_identifier already contains it
            if clean_id.startswith(self.base_prefix):
                if has_timestamp:
                    # ID already has both prefix and timestamp
                    return clean_id
                else:
                    # Custom ID already contains base prefix, just add timestamp
                    return f"{clean_id}_{timestamp}"
            else:
                # Add base prefix, custom ID, and timestamp
                return f"{self.base_prefix}_{clean_id}_{timestamp}"
            
        return f"{self.base_prefix}_{timestamp}"
    
    def import_excel(
        self,
        excel_path: Union[str, Path],
        custom_collection_id: Optional[str] = None,
        sheet_name: Optional[Union[str, int]] = 0,
        id_column: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ) -> Tuple[str, Dict[str, int]]:
        """
        Import Excel data to a new Firestore collection with production-ready optimizations.
        
        Args:
            excel_path: Path to Excel file
            custom_collection_id: Optional identifier for collection naming
            sheet_name: Excel sheet to import (name or index)
            id_column: Optional column to use as document IDs
            max_retries: Maximum number of retries for batch operations
            retry_delay: Delay between retries in seconds
            
        Returns:
            Tuple of (collection_name, stats_dict)
        """
        # Validate file
        excel_path = Path(excel_path)
        if not excel_path.exists():
            raise FileNotFoundError(f"Excel file not found: {excel_path}")
            
        logger.info(f"Importing {excel_path} to Firestore")
        
        # Create collection name
        collection_name = self.generate_collection_name(custom_collection_id)
        logger.info(f"Using collection: {collection_name}")
        
        # Load data with proper error handling and validation
        try:
            # Use efficient reading with explicit dtypes for better memory usage
            df = pd.read_excel(
                excel_path, 
                sheet_name=sheet_name,
                engine='openpyxl'  # More robust engine
            )
            
            # Check if dataframe is empty
            if df.empty:
                logger.warning(f"Excel file {excel_path} contains no data")
                return collection_name, {"total": 0, "processed": 0, "success": 0, "errors": 0}
            
            # Clean column names for better interoperability and consistency
            df.columns = [str(c).strip().lower().replace(' ', '_').replace('-', '_') for c in df.columns]
            
            # Data validation and cleanup
            # Remove rows with all NaN values
            df = df.dropna(how='all')
            
            # Handle missing values appropriately based on data type
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].fillna('')
                elif pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna(0)
                elif pd.api.types.is_datetime64_dtype(df[col]):
                    # Keep NaT values for dates to preserve data integrity
                    pass
            
            # Convert to list of dictionaries for Firestore with proper handling of special types
            data = []
            for _, row in df.iterrows():
                record = {}
                # Process existing columns
                for col, val in row.items():
                    # Handle special data types for Firestore compatibility
                    if pd.isna(val):
                        record[col] = None
                    elif pd.api.types.is_datetime64_dtype(type(val)):
                        record[col] = val.isoformat() if hasattr(val, 'isoformat') else str(val)
                    else:
                        record[col] = val
                
                # Add the Family field - concatenation of Species, Primal, Subprimal, and Grade
                species = str(record.get('species', '')) if not pd.isna(record.get('species', None)) else ''
                primal = str(record.get('primal', '')) if not pd.isna(record.get('primal', None)) else ''
                subprimal = str(record.get('subprimal', '')) if not pd.isna(record.get('subprimal', None)) else ''
                grade = str(record.get('grade', '')) if not pd.isna(record.get('grade', None)) else ''
                
                # Create the Family field by joining the components with spaces
                record['family'] = ' '.join(filter(None, [species, primal, subprimal, grade]))
                
                # Add empty Approved and Comments fields
                record['approved'] = ''
                record['comments'] = ''
                
                data.append(record)
            
            logger.info(f"Loaded {len(data)} records from Excel")
            
        except Exception as e:
            logger.error(f"Error reading Excel file: {e}")
            raise
        
        # Process in optimized batches with retry logic
        total_records = len(data)
        processed = 0
        success = 0
        errors = 0
        retries = 0
        
        # Add comprehensive metadata
        metadata = {
            "source_file": str(excel_path.name),
            "import_time": datetime.now().isoformat(),
            "record_count": total_records,
            "columns": list(df.columns),
            "import_id": collection_name.split('_')[-1],  # Timestamp part of collection
            "sheet_name": str(sheet_name),
        }
        
        # Store metadata document with retry logic
        for attempt in range(max_retries):
            try:
                self.firebase_client.add_document(
                    metadata,
                    collection=collection_name,
                    doc_id="_import_metadata"  # Changed to non-reserved ID
                )
                logger.info("Successfully stored metadata document")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Error storing metadata (attempt {attempt+1}/{max_retries}): {e}")
                    import time
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Failed to store metadata after {max_retries} attempts: {e}")
        
        # Process records in optimized batches with progressive batch size adjustment
        adaptive_batch_size = self.batch_size
        
        # Batch data by chunks for optimal memory usage
        for i in range(0, total_records, adaptive_batch_size):
            batch_data = data[i:i+adaptive_batch_size]
            batch_ops = []
            
            for j, record in enumerate(batch_data):
                # Generate a consistent and collision-resistant ID
                if id_column and id_column in record and record[id_column]:
                    # Clean the ID to ensure it's valid for Firestore
                    doc_id = str(record[id_column]).strip()
                    # Replace invalid characters
                    doc_id = ''.join(c if c.isalnum() or c in '_-' else '_' for c in doc_id)
                    # Ensure it's not empty after cleaning
                    if not doc_id:
                        doc_id = f"item_{i+j:06d}"
                else:
                    doc_id = f"item_{i+j:06d}"
                
                # Add audit metadata
                record['_imported_at'] = datetime.now().isoformat()
                record['_batch_index'] = i // adaptive_batch_size
                record['_import_id'] = metadata['import_id']
                record['_row_index'] = i + j
                
                # Add to batch operations
                batch_ops.append({
                    "op": "set",
                    "doc_id": doc_id,
                    "data": record
                })
            
            # Execute batch write with optimized error handling and retry logic
            batch_success = False
            batch_success_count = 0
            
            for retry_attempt in range(max_retries):
                try:
                    batch_success_count = self.firebase_client.batch_write(
                        batch_ops,
                        collection=collection_name
                    )
                    
                    if batch_success_count > 0:
                        batch_success = True
                        success += batch_success_count
                        if batch_success_count < len(batch_ops):
                            errors += len(batch_ops) - batch_success_count
                            logger.warning(f"Partial batch success: {batch_success_count}/{len(batch_ops)}")
                        break
                    else:
                        logger.warning(f"Batch write returned 0 successes on attempt {retry_attempt+1}/{max_retries}")
                        
                except Exception as e:
                    if retry_attempt < max_retries - 1:
                        logger.warning(f"Error in batch write (attempt {retry_attempt+1}/{max_retries}): {e}")
                        import time
                        time.sleep(retry_delay * (retry_attempt + 1))  # Exponential backoff
                        retries += 1
                    else:
                        logger.error(f"Failed batch write after {max_retries} attempts: {e}")
                        errors += len(batch_ops)
            
            if not batch_success:
                errors += len(batch_ops)
                
                # Reduce batch size dynamically on persistent failures
                if adaptive_batch_size > 10 and retries > 2:
                    adaptive_batch_size = max(10, adaptive_batch_size // 2)
                    logger.warning(f"Reducing batch size to {adaptive_batch_size} after multiple failures")
            
            processed += len(batch_data)
            progress_percent = (processed / total_records) * 100 if total_records > 0 else 100
            logger.info(f"Progress: {processed}/{total_records} records processed ({progress_percent:.1f}%)")
        
        # Return comprehensive stats
        stats = {
            "total": total_records,
            "processed": processed,
            "success": success,
            "errors": errors,
            "retries": retries,
            "collection_name": collection_name,
            "source_file": str(excel_path.name),
            "final_batch_size": adaptive_batch_size,
            "completion_time": datetime.now().isoformat()
        }
        
        # Update metadata with final stats
        try:
            self.firebase_client.update_document(
                doc_id="_import_metadata",  # Use non-reserved ID consistently
                data={"import_stats": stats},
                collection=collection_name
            )
        except Exception as e:
            logger.warning(f"Could not update metadata with final stats: {e}")
        
        if success == total_records:
            logger.info(f"Import completed successfully: {success}/{total_records} records imported")
        else:
            logger.warning(f"Import completed with issues: {success} successful, {errors} errors out of {total_records} records")
            
        return collection_name, stats
    
    def get_collection_list(self) -> List[str]:
        """
        Get list of collections created by this importer.
        
        Returns:
            List of collection names matching the base prefix
        """
        try:
            # This is a custom method that would need to be added to FirebaseClient
            # For now, we'll just return a placeholder message
            logger.info("Collection listing not implemented in FirebaseClient")
            return []
        except Exception as e:
            logger.error(f"Error retrieving collections: {e}")
            return []


def import_master_excel(
    excel_path: Union[str, Path],
    identifier: Optional[str] = None
) -> Dict[str, Any]:
    """
    Utility function to import a master Excel file to Firestore.
    
    Args:
        excel_path: Path to Excel file
        identifier: Optional identifier for the collection name
        
    Returns:
        Dictionary with import results
    """
    try:
        importer = ExcelToFirestore()
        
        if identifier is None:
            # Extract identifier from filename if not provided
            excel_name = Path(excel_path).stem
            identifier = excel_name.replace(' ', '_').lower()
        
        collection_name, stats = importer.import_excel(
            excel_path=excel_path,
            custom_collection_id=identifier
        )
        
        return {
            "success": True,
            "collection": collection_name,
            "stats": stats,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Import failed: {e}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }
