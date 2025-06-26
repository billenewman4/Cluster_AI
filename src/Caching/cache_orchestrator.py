"""
Cache Orchestrator Module

Orchestrates the complete cache refresh process by coordinating database retrieval,
filtering, and cache management operations.
"""

import os
import logging
import time
from typing import Dict, Any, Optional, List
from datetime import datetime

# Import from existing modules
from src.database.firebase_client import FirebaseClient
from src.Caching.acceptance_filter import filter_approved_items
from src.Caching.cache_manager import (
    load_existing_cache, 
    update_cache, 
    save_cache, 
    get_cache_statistics,
    DEFAULT_CACHE_PATH
)
from src.Caching.cache_query import validate_cache_freshness

# Configure logging
logger = logging.getLogger(__name__)


def refresh_cache(collection_name: str, 
                 cache_file_path: str = DEFAULT_CACHE_PATH, 
                 project_id: Optional[str] = None,
                 credentials_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Main function to refresh the entire cache.
    
    Args:
        collection_name: Firebase collection to query
        cache_file_path: Path to save cache file
        project_id: Optional Firebase project ID (uses default if None)
        credentials_path: Optional path to credentials file (uses default if None)
        
    Returns:
        Dictionary with refresh results and statistics
    """
    start_time = time.time()
    logger.info(f"Starting cache refresh for collection '{collection_name}'")
    
    # Step 1: Load existing cache
    try:
        existing_cache = load_existing_cache(cache_file_path)
        logger.info(f"Loaded existing cache with {len(existing_cache.get('cached_items', {}))} items")
    except Exception as e:
        logger.error(f"Error loading existing cache: {e}")
        existing_cache = {"metadata": {}, "cached_items": {}}
    
    # Step 2: Retrieve data from Firebase
    try:
        firebase_client = FirebaseClient(
            project_id=project_id,
            credentials_path=credentials_path,
            collection_name=collection_name
        )
        
        all_items = firebase_client.get_documents(
            collection=collection_name,
            limit=100000  # Handles large datasets automatically
        )
        
        item_count = len(all_items)
        logger.info(f"Retrieved {item_count} items from Firebase collection '{collection_name}'")
        
    except Exception as e:
        logger.error(f"Error retrieving data from Firebase: {e}")
        return {
            "success": False,
            "error": f"Firebase retrieval error: {str(e)}",
            "elapsed_seconds": time.time() - start_time
        }
    
    # Step 3: Filter for approved items
    try:
        filter_result = filter_approved_items(all_items)
        approved_items = filter_result["approved_items"]
        rejected_items = filter_result["rejected_items"]
        filter_stats = filter_result["statistics"]
        
        logger.info(f"Filtered {filter_stats['approved_items']}/{filter_stats['total_items']} approved items " +
                   f"({filter_stats['approval_rate_percent']}%)")
        
    except Exception as e:
        logger.error(f"Error filtering approved items: {e}")
        return {
            "success": False,
            "error": f"Filtering error: {str(e)}",
            "elapsed_seconds": time.time() - start_time
        }
    
    # Step 4: Update cache with new approved items
    try:
        updated_cache = update_cache(
            existing_cache=existing_cache,
            all_accepted_items=approved_items,
            collection_name=collection_name
        )
    except Exception as e:
        logger.error(f"Error updating cache: {e}")
        return {
            "success": False,
            "error": f"Cache update error: {str(e)}",
            "elapsed_seconds": time.time() - start_time
        }
    
    # Step 5: Save updated cache to disk
    try:
        save_result = save_cache(updated_cache, cache_file_path)
        if not save_result:
            logger.error("Failed to save cache to disk")
            return {
                "success": False,
                "error": "Failed to save cache to disk",
                "elapsed_seconds": time.time() - start_time
            }
    except Exception as e:
        logger.error(f"Error saving cache: {e}")
        return {
            "success": False,
            "error": f"Cache save error: {str(e)}",
            "elapsed_seconds": time.time() - start_time
        }
    
    # Step 6: Generate result statistics
    elapsed_seconds = time.time() - start_time
    cache_stats = get_cache_statistics(updated_cache)
    
    result = {
        "success": True,
        "timestamp": datetime.now().isoformat(),
        "elapsed_seconds": round(elapsed_seconds, 2),
        "collection_name": collection_name,
        "cache_file": cache_file_path,
        "total_items_processed": len(all_items),
        "filter_statistics": filter_stats,
        "cache_statistics": cache_stats
    }
    
    logger.info(f"Cache refresh complete in {elapsed_seconds:.2f} seconds")
    logger.info(f"Cache now contains {cache_stats['total_items']} items")
    
    return result


def incremental_cache_update(collection_name: str, 
                            cache_file_path: str = DEFAULT_CACHE_PATH,
                            last_update_timestamp: Optional[str] = None,
                            project_id: Optional[str] = None,
                            credentials_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Update cache with only items modified since last update.
    
    Args:
        collection_name: Firebase collection to query
        cache_file_path: Path to cache file
        last_update_timestamp: Timestamp to filter changes (if None, uses cache metadata)
        project_id: Optional Firebase project ID
        credentials_path: Optional credentials path
        
    Returns:
        Dictionary with update results
    """
    start_time = time.time()
    logger.info(f"Starting incremental cache update for collection '{collection_name}'")
    
    # Load existing cache to get last update timestamp if not provided
    try:
        existing_cache = load_existing_cache(cache_file_path)
        
        if not last_update_timestamp:
            metadata = existing_cache.get("metadata", {})
            last_update_timestamp = metadata.get("last_updated")
            
            if not last_update_timestamp:
                logger.warning("No last update timestamp found, performing full refresh")
                return refresh_cache(
                    collection_name=collection_name,
                    cache_file_path=cache_file_path,
                    project_id=project_id,
                    credentials_path=credentials_path
                )
    except Exception as e:
        logger.error(f"Error loading existing cache: {e}")
        return {
            "success": False,
            "error": f"Error loading cache: {str(e)}",
            "elapsed_seconds": time.time() - start_time
        }
    
    # Initialize Firebase client
    try:
        firebase_client = FirebaseClient(
            project_id=project_id,
            credentials_path=credentials_path,
            collection_name=collection_name
        )
        
        # Query for items updated since last cache update
        # Note: This requires Firebase to track update timestamps
        # This is a simplified version - actual implementation would depend on how
        # timestamps are tracked in your Firebase collection
        all_items = firebase_client.get_documents(
            collection=collection_name,
            limit=10000,
            # Include filter for timestamp once available in your data model
            # where={"updated_at": {">=": last_update_timestamp}}
        )
        
        item_count = len(all_items)
        logger.info(f"Retrieved {item_count} items from Firebase collection '{collection_name}'")
        
    except Exception as e:
        logger.error(f"Error retrieving data from Firebase: {e}")
        return {
            "success": False,
            "error": f"Firebase retrieval error: {str(e)}",
            "elapsed_seconds": time.time() - start_time
        }
    
    # Follow same flow as full refresh from here
    # Filter, update, and save cache
    try:
        filter_result = filter_approved_items(all_items)
        approved_items = filter_result["approved_items"]
        
        updated_cache = update_cache(
            existing_cache=existing_cache,
            new_accepted_items=approved_items,
            collection_name=collection_name
        )
        
        save_result = save_cache(updated_cache, cache_file_path)
        if not save_result:
            return {
                "success": False,
                "error": "Failed to save cache to disk",
                "elapsed_seconds": time.time() - start_time
            }
            
        elapsed_seconds = time.time() - start_time
        cache_stats = get_cache_statistics(updated_cache)
        
        return {
            "success": True,
            "mode": "incremental",
            "timestamp": datetime.now().isoformat(),
            "elapsed_seconds": round(elapsed_seconds, 2),
            "items_processed": item_count,
            "filter_statistics": filter_result["statistics"],
            "cache_statistics": cache_stats
        }
        
    except Exception as e:
        logger.error(f"Error in incremental update: {e}")
        return {
            "success": False,
            "error": f"Incremental update error: {str(e)}",
            "elapsed_seconds": time.time() - start_time
        }


def validate_cache_integrity(cache_file_path: str = DEFAULT_CACHE_PATH) -> Dict[str, Any]:
    """
    Validate cache structure and data integrity.
    
    Args:
        cache_file_path: Path to cache file
        
    Returns:
        Dictionary with validation results
    """
    start_time = time.time()
    logger.info(f"Validating cache integrity: {cache_file_path}")
    
    # Check if file exists
    if not os.path.exists(cache_file_path):
        return {
            "valid": False,
            "errors": ["Cache file does not exist"],
            "elapsed_seconds": time.time() - start_time
        }
    
    # Validate cache structure
    try:
        cache_data = load_existing_cache(cache_file_path)
        
        # Check basic structure
        validation_errors = []
        
        if "metadata" not in cache_data:
            validation_errors.append("Missing metadata section")
        
        if "cached_items" not in cache_data:
            validation_errors.append("Missing cached_items section")
        
        # Check required metadata fields
        metadata = cache_data.get("metadata", {})
        for field in ["last_updated", "cache_version", "total_cached_items"]:
            if field not in metadata:
                validation_errors.append(f"Missing metadata field: {field}")
                
        # Check item structure for a sample of items
        cached_items = cache_data.get("cached_items", {})
        item_count = len(cached_items)
        
        if item_count > 0:
            # Check up to 10 random items
            import random
            sample_size = min(10, item_count)
            sample_keys = random.sample(list(cached_items.keys()), sample_size)
            
            for key in sample_keys:
                item = cached_items[key]
                if "product_code" not in item:
                    validation_errors.append(f"Missing product_code in item {key}")
                if "item_data" not in item:
                    validation_errors.append(f"Missing item_data in item {key}")
        
        # Validate cache size
        try:
            cache_size_bytes = os.path.getsize(cache_file_path)
            cache_size_mb = cache_size_bytes / (1024 * 1024)
            
            # Flag very large cache files (> 50MB as an example threshold)
            if cache_size_mb > 50:
                validation_errors.append(f"Cache file is very large: {cache_size_mb:.2f} MB")
        except Exception:
            validation_errors.append("Unable to determine cache file size")
        
        # Return validation results
        is_valid = len(validation_errors) == 0
        
        return {
            "valid": is_valid,
            "errors": validation_errors if not is_valid else [],
            "cache_file": cache_file_path,
            "item_count": item_count,
            "file_size_mb": round(cache_size_mb, 2) if 'cache_size_mb' in locals() else None,
            "elapsed_seconds": round(time.time() - start_time, 2)
        }
        
    except Exception as e:
        logger.error(f"Cache validation error: {e}")
        return {
            "valid": False,
            "errors": [f"Error validating cache: {str(e)}"],
            "elapsed_seconds": round(time.time() - start_time, 2)
        }


if __name__ == "__main__":
    # Setup basic logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Use the latest collection as specified
    collection_name = "reviewed_beef_cuts_latest_master_20250616_20250617_102108"
    cache_file_path = "data/processed/.accepted_items_cache.json"
    
    print(f"Starting cache refresh from collection: {collection_name}")
    print(f"Cache will be saved to: {cache_file_path}")
    
    # Run cache refresh
    result = refresh_cache(
        collection_name=collection_name,
        cache_file_path=cache_file_path
    )
    
    # Display results
    if result['success']:
        print(f"✅ Cache refresh succeeded!")
        print(f"Processed {result.get('total_items_processed', 0)} items in {result.get('elapsed_seconds', 0):.2f} seconds")
        print(f"Cache now contains {result.get('cache_statistics', {}).get('total_items', 0)} items")
        print(f"Approval rate: {result.get('filter_statistics', {}).get('approval_rate_percent', 0):.1f}%")
    else:
        print(f"❌ Cache refresh failed: {result.get('error', 'Unknown error')}")
