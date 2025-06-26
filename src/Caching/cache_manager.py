"""
Cache Manager Module

Handles creation, reading, updating, and deletion of cached items.
Implements the cache file structure and provides atomic write operations.
"""

import os
import json
import logging
import tempfile
import shutil
from typing import Dict, Any, Optional, List
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)

# Default constants
DEFAULT_CACHE_PATH = "data/processed/.accepted_items_cache.json"
CACHE_VERSION = "1.0"


def generate_cache_key(product_code: str) -> str:
    """
    Generate a unique, repeatable cache key using product code.
    
    Args:
        product_code: The unique product code from the data source
    
    Returns:
        Normalized product code string
        
    Raises:
        ValueError: If product code is empty or None
    """
    if not product_code:
        raise ValueError("Product code is required for cache key generation")
    
    # Convert to string and normalize (uppercase, strip whitespace)
    normalized_code = str(product_code).strip().upper()
    
    return normalized_code


def load_existing_cache(cache_file_path: str = DEFAULT_CACHE_PATH) -> Dict[str, Any]:
    """
    Load existing cache or create new empty cache structure.
    
    Args:
        cache_file_path: Path to the cache file
        
    Returns:
        Dictionary containing the complete cache structure
    """
    logger.info(f"Loading existing cache from {cache_file_path}")
    
    # Check if cache file exists
    if not os.path.exists(cache_file_path):
        logger.info("Cache file not found, initializing new empty cache")
        return _initialize_empty_cache()
    
    try:
        with open(cache_file_path, 'r', encoding='utf-8') as cache_file:
            cache_data = json.load(cache_file)
            
        # Validate cache structure
        if not isinstance(cache_data, dict) or 'metadata' not in cache_data or 'cached_items' not in cache_data:
            logger.warning("Found invalid cache structure, initializing new cache")
            return _initialize_empty_cache()
            
        logger.info(f"Successfully loaded cache with {len(cache_data.get('cached_items', {}))} items")
        return cache_data
        
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Error reading cache file: {e}")
        logger.warning("Initializing new empty cache due to error")
        return _initialize_empty_cache()


def _initialize_empty_cache() -> Dict[str, Any]:
    """
    Create a new empty cache structure with proper metadata.
    
    Returns:
        Dictionary with initialized empty cache structure
    """
    current_time = datetime.now().isoformat()
    
    return {
        "metadata": {
            "last_updated": current_time,
            "total_cached_items": 0,
            "cache_version": CACHE_VERSION,
            "source_collection": "",
            "cache_key_strategy": "product_code",
            "filtering_criteria": {
                "approved_field_values": [
                    "approved", "yes", "y", "true", "1", "accept", "accepted", "✓"
                ]
            }
        },
        "cached_items": {}
    }


def update_cache(existing_cache: Dict[str, Any], 
                all_accepted_items: List[Dict[str, Any]], 
                collection_name: str = "") -> Dict[str, Any]:
    """
    Update cache with new accepted items and metadata.
    
    Args:
        existing_cache: Current cache data dictionary
        all_accepted_items: List of new accepted items to cache
        collection_name: Name of the source collection
        
    Returns:
        Updated cache dictionary with new items and metadata
    """
    logger.info(f"Updating cache with {len(all_accepted_items)} new accepted items")
    
    # Create a copy of the existing cache to avoid modifying the original
    updated_cache = {
        "metadata": dict(existing_cache.get("metadata", {})),
        "cached_items": dict(existing_cache.get("cached_items", {}))
    }
    
    # Track changes for reporting
    items_added = 0
    items_unchanged = 0
    
    # Process each new accepted item
    for item in all_accepted_items:
        # Get product code for the item
        product_code = item.get('product_code')
        
        # Skip items without product code
        if not product_code:
            logger.warning(f"Skipping item without product code: {item.get('document_id', 'unknown')}")
            continue
        
        # Generate cache key
        try:
            cache_key = generate_cache_key(product_code)
        except ValueError as e:
            logger.warning(f"Skipping item: {str(e)}")
            continue
            
        # Prepare cache entry with metadata
        cache_entry = {
            "product_code": product_code,
            "document_id": item.get('document_id', ''),
            "cached_timestamp": datetime.now().isoformat(),
            "cache_reason": "approved",
            "item_data": item
        }
        
        # Add or update in cache
        if cache_key in updated_cache["cached_items"]:
            items_unchanged += 1
        else:
            items_added += 1
            
        updated_cache["cached_items"][cache_key] = cache_entry
    
    # Handle items in cache that are no longer in the accepted items (removed)
    items_removed = _remove_stale_items(updated_cache, all_accepted_items)
    
    # Update metadata
    updated_cache["metadata"]["last_updated"] = datetime.now().isoformat()
    updated_cache["metadata"]["total_cached_items"] = len(updated_cache["cached_items"])
    
    if collection_name:
        updated_cache["metadata"]["source_collection"] = collection_name
    
    # Log update results
    logger.info(f"Cache update complete: {items_added} added, {items_unchanged} unchanged, {items_removed} removed")
    
    return updated_cache


def _remove_stale_items(cache_data: Dict[str, Any], all_accepted_items: List[Dict[str, Any]]) -> int:
    """
    Remove items from cache that are no longer in the accepted items list.
    
    Args:
        cache_data: The cache data dictionary to update
        all_accepted_items: List of currently accepted items
        
    Returns:
        Number of items removed from cache
    """
    # Create set of product codes from new accepted items
    current_product_codes = set()
    for item in all_accepted_items:
        product_code = item.get('product_code')
        if product_code:
            try:
                cache_key = generate_cache_key(product_code)
                current_product_codes.add(cache_key)
            except ValueError:
                continue
    
    # Find keys to remove (in cache but not in new accepted items)
    cached_keys = set(cache_data["cached_items"].keys())
    keys_to_remove = cached_keys - current_product_codes
    
    # Remove stale items
    for key in keys_to_remove:
        del cache_data["cached_items"][key]
    
    return len(keys_to_remove)


def save_cache(cache_data: Dict[str, Any], cache_file_path: str = DEFAULT_CACHE_PATH) -> bool:
    """
    Save updated cache to file with atomic write operation.
    
    Args:
        cache_data: Cache data dictionary to save
        cache_file_path: Path to save cache file
        
    Returns:
        True if save was successful, False otherwise
    """
    logger.info(f"Saving cache with {len(cache_data.get('cached_items', {}))} items to {cache_file_path}")
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(cache_file_path), exist_ok=True)
    
    # Use atomic write pattern with tempfile
    temp_file = None
    try:
        # Create temp file in same directory for atomic move
        dir_name = os.path.dirname(cache_file_path) or '.'
        fd, temp_path = tempfile.mkstemp(dir=dir_name)
        temp_file = os.fdopen(fd, 'w', encoding='utf-8')
        
        # Write cache data to temp file
        json.dump(cache_data, temp_file, indent=2, ensure_ascii=False)
        temp_file.flush()
        os.fsync(temp_file.fileno())
        temp_file.close()
        temp_file = None
        
        # Atomic replace (atomic on POSIX systems)
        shutil.move(temp_path, cache_file_path)
        
        logger.info(f"Cache successfully saved to {cache_file_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving cache: {e}")
        return False
        
    finally:
        # Ensure temp file is closed if still open
        if temp_file:
            temp_file.close()


def get_cache_statistics(cache_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate cache statistics and health metrics.
    
    Args:
        cache_data: Cache data dictionary
        
    Returns:
        Dictionary of cache statistics
    """
    cached_items = cache_data.get("cached_items", {})
    metadata = cache_data.get("metadata", {})
    
    # Calculate cache age
    last_updated = metadata.get("last_updated", "")
    cache_age_hours = 0
    
    if last_updated:
        try:
            last_update_time = datetime.fromisoformat(last_updated)
            cache_age_seconds = (datetime.now() - last_update_time).total_seconds()
            cache_age_hours = cache_age_seconds / 3600
        except (ValueError, TypeError):
            pass
    
    return {
        "total_items": len(cached_items),
        "cache_version": metadata.get("cache_version", "unknown"),
        "last_updated": last_updated,
        "cache_age_hours": round(cache_age_hours, 2),
        "source_collection": metadata.get("source_collection", "unknown"),
        "cache_file_size_kb": _get_cache_file_size(cache_data),
        "cache_health": "good" if cache_age_hours < 24 else "stale"
    }


def _get_cache_file_size(cache_data: Dict[str, Any]) -> int:
    """
    Estimate the cache file size in KB.
    
    Args:
        cache_data: Cache data dictionary
        
    Returns:
        Estimated size in KB
    """
    try:
        # Serialize to string and get size
        json_str = json.dumps(cache_data)
        size_bytes = len(json_str.encode('utf-8'))
        return round(size_bytes / 1024)  # Convert to KB
    except Exception:
        return 0