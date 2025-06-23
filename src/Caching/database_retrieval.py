"""
Database Retrieval Functions for Caching System

Simple functions that use existing FirebaseClient to retrieve data for caching.
No unnecessary classes - just reuse what works.
"""

import logging
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime

# Import existing Firebase client
from src.database.firebase_client import FirebaseClient

# Configure logging
logger = logging.getLogger(__name__)


def get_all_items_from_collection(
    collection_name: str,
    project_id: Optional[str] = None,
    credentials_path: Optional[str] = None,
    limit: int = 10000
) -> List[Dict[str, Any]]:
    """
    Retrieve all items from a Firebase collection using existing FirebaseClient.
    
    Args:
        collection_name: Name of the Firebase collection to retrieve
        project_id: Optional Firebase project ID
        credentials_path: Optional path to Firebase credentials
        limit: Maximum number of documents to retrieve
        
    Returns:
        List of all retrieved documents
        
    Raises:
        Exception: If unable to connect to Firebase or retrieve documents
        
    REUSES:
    - FirebaseClient.__init__() for connection setup
    - FirebaseClient.get_documents() for batch retrieval
    """
    logger.info(f"Retrieving all items from collection: {collection_name}")
    
    try:
        # Initialize Firebase client (reuses existing connection logic)
        firebase_client = FirebaseClient(
            project_id=project_id,
            credentials_path=credentials_path,
            collection_name=collection_name
        )
        
        # Get all documents (reuses existing batch retrieval with built-in pagination)
        all_items = firebase_client.get_documents(
            collection=collection_name,
            limit=limit
        )
        
        logger.info(f"Successfully retrieved {len(all_items)} items from {collection_name}")
        return all_items
        
    except Exception as e:
        logger.error(f"Failed to retrieve items from {collection_name}: {e}")
        raise


# REMOVED: get_approved_items_from_collection() 
# This function is now handled by src/Caching/acceptance_filter.py to avoid duplication


def get_collection_metadata(
    collection_name: str,
    project_id: Optional[str] = None,
    credentials_path: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Get metadata about a collection (if it exists).
    
    Args:
        collection_name: Name of the collection to check
        project_id: Optional Firebase project ID
        credentials_path: Optional path to Firebase credentials
        
    Returns:
        Metadata dictionary or None if not found
        
    REUSES:
    - FirebaseClient.get_document() for metadata retrieval
    - Pattern from ret_comments.py for collection checking
    """
    logger.info(f"Checking metadata for collection: {collection_name}")
    
    try:
        # Initialize Firebase client
        firebase_client = FirebaseClient(
            project_id=project_id,
            credentials_path=credentials_path,
            collection_name=collection_name
        )
        
        # Try to get metadata document (common pattern in existing code)
        metadata = firebase_client.get_document(
            doc_id="_import_metadata",
            collection=collection_name
        )
        
        if metadata:
            logger.info(f"Found metadata for collection: {collection_name}")
            return metadata
        else:
            logger.warning(f"No metadata found for collection: {collection_name}")
            return None
            
    except Exception as e:
        logger.error(f"Error checking metadata for {collection_name}: {e}")
        return None


def get_database_summary(
    collection_name: str,
    project_id: Optional[str] = None,
    credentials_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get a summary of database contents for caching decisions.
    
    Args:
        collection_name: Name of the collection to summarize
        project_id: Optional Firebase project ID
        credentials_path: Optional path to Firebase credentials
        
    Returns:
        Summary dictionary with counts and statistics
    """
    logger.info(f"Generating database summary for: {collection_name}")
    
    try:
        # Get all items
        all_items = get_all_items_from_collection(
            collection_name=collection_name,
            project_id=project_id,
            credentials_path=credentials_path
        )
        
        # Note: Approved items filtering now handled by acceptance_filter.py
        # This function just gets all items for filtering
        
        # Get metadata
        metadata = get_collection_metadata(
            collection_name=collection_name,
            project_id=project_id,
            credentials_path=credentials_path
        )
        
        # Calculate summary statistics
        total_count = len(all_items)
        # Note: Approved count calculation moved to acceptance_filter.py
        approved_count = 0  # Placeholder - use acceptance_filter.py for actual filtering
        approval_rate = 0   # Placeholder - use acceptance_filter.py for actual filtering
        
        # Count items with product codes (needed for caching)
        items_with_product_codes = sum(
            1 for item in all_items 
            if item.get('product_code') and str(item.get('product_code')).strip()
        )
        
        summary = {
            "collection_name": collection_name,
            "total_items": total_count,
            "approved_items": approved_count,
            "approval_rate_percent": round(approval_rate, 2),
            "items_with_product_codes": items_with_product_codes,
            "cacheable_items": items_with_product_codes,  # Only items with product codes can be cached
            "metadata_available": metadata is not None,
            "collection_metadata": metadata,
            "summary_generated_at": datetime.now().isoformat()
        }
        
        logger.info(f"Database summary: {approved_count}/{total_count} approved items ({approval_rate:.1f}%)")
        return summary
        
    except Exception as e:
        logger.error(f"Failed to generate database summary: {e}")
        return {
            "collection_name": collection_name,
            "error": str(e),
            "summary_generated_at": datetime.now().isoformat()
        }


# Convenience function for the main caching workflow
def get_items_for_caching(
    collection_name: Optional[str] = None,
    base_prefix: str = "beef_cuts",
    project_id: Optional[str] = None,
    credentials_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Main function to get items ready for caching.
    
    Args:
        collection_name: Specific collection name or None to find latest
        base_prefix: Base prefix for collection search if collection_name is None
        project_id: Optional Firebase project ID
        credentials_path: Optional path to Firebase credentials
        
    Returns:
        Dictionary with approved items and metadata for caching
    """
    # Check if collection name is provided
    if not collection_name:
        raise ValueError(f"No collection found with prefix: {base_prefix}")
        
    logger.info(f"Getting items for caching from: {collection_name}")
    
    # Get all items (filtering moved to acceptance_filter.py)
    all_items = get_all_items_from_collection(
        collection_name=collection_name,
        project_id=project_id,
        credentials_path=credentials_path
    )
    
    # Get summary
    summary = get_database_summary(
        collection_name=collection_name,
        project_id=project_id,
        credentials_path=credentials_path
    )
    
    return {
        "collection_name": collection_name,
        "all_items": all_items,  # Raw items - use acceptance_filter.py to get approved items
        "database_summary": summary,
        "retrieved_at": datetime.now().isoformat()
    } 