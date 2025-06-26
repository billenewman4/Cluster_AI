"""
Cache Query Module

Provides interfaces for checking if items are in cache and retrieving cached data.
Supports both single item and bulk lookups for efficient processing.
"""

import os
import json
import logging
from typing import Dict, Any, Optional, List, Set, Tuple

from src.Caching.cache_manager import generate_cache_key, load_existing_cache, DEFAULT_CACHE_PATH

# Configure logging
logger = logging.getLogger(__name__)


def is_item_cached(item_identifier: str, cache_file_path: str = DEFAULT_CACHE_PATH) -> bool:
    """
    Check if a single item is in cache based on its product code.
    
    Args:
        item_identifier: Product code or unique identifier
        cache_file_path: Path to cache file
        
    Returns:
        True if item is in cache, False otherwise
    """
    # First normalize the identifier to ensure consistent lookup
    try:
        cache_key = generate_cache_key(item_identifier)
    except ValueError:
        logger.warning(f"Invalid item identifier for cache check: {item_identifier}")
        return False
        
    # Perform minimal cache read (avoid loading full item data)
    try:
        with open(cache_file_path, 'r', encoding='utf-8') as cache_file:
            cache_data = json.load(cache_file)
            cached_items = cache_data.get("cached_items", {})
            return cache_key in cached_items
    except (json.JSONDecodeError, IOError, FileNotFoundError) as e:
        logger.warning(f"Error checking cache for item {item_identifier}: {e}")
        return False


def get_cached_item_data(item_identifier: str, cache_file_path: str = DEFAULT_CACHE_PATH) -> Optional[Dict[str, Any]]:
    """
    Retrieve full cached data for an item by its product code.
    
    Args:
        item_identifier: Product code or unique identifier
        cache_file_path: Path to cache file
        
    Returns:
        Item data dictionary or None if not found
    """
    # Generate consistent lookup key
    try:
        cache_key = generate_cache_key(item_identifier)
    except ValueError:
        logger.warning(f"Invalid item identifier for data retrieval: {item_identifier}")
        return None
        
    # Load cache and extract specific item
    try:
        with open(cache_file_path, 'r', encoding='utf-8') as cache_file:
            cache_data = json.load(cache_file)
            cached_items = cache_data.get("cached_items", {})
            cached_entry = cached_items.get(cache_key)
            
            if cached_entry:
                return cached_entry.get("item_data")
            return None
            
    except (json.JSONDecodeError, IOError, FileNotFoundError) as e:
        logger.warning(f"Error retrieving cache data for {item_identifier}: {e}")
        return None


def get_cached_items_bulk(item_identifiers: List[str], 
                         cache_file_path: str = DEFAULT_CACHE_PATH) -> Dict[str, Any]:
    """
    Bulk lookup for multiple items - more efficient than checking one by one.
    
    Args:
        item_identifiers: List of product codes to check
        cache_file_path: Path to cache file
        
    Returns:
        Dictionary with results:
        {
            "cached_items": {id: data, ...},  # Items found in cache with their data
            "non_cached_items": [id1, id2, ...],  # Items not found in cache
            "invalid_identifiers": [id1, id2, ...],  # Items with invalid identifiers
            "statistics": {
                "total_checked": int,
                "found_in_cache": int, 
                "not_in_cache": int,
                "invalid_items": int
            }
        }
    """
    # Initialize result structure
    result = {
        "cached_items": {},
        "non_cached_items": [],
        "invalid_identifiers": [],
        "statistics": {
            "total_checked": len(item_identifiers),
            "found_in_cache": 0,
            "not_in_cache": 0,
            "invalid_items": 0
        }
    }
    
    # Load cache once for efficiency
    try:
        cache_data = load_existing_cache(cache_file_path)
        cached_items = cache_data.get("cached_items", {})
    except Exception as e:
        logger.error(f"Error loading cache for bulk lookup: {e}")
        # Mark all items as not cached if we can't load the cache
        result["non_cached_items"] = item_identifiers
        result["statistics"]["not_in_cache"] = len(item_identifiers)
        return result
    
    # Process each identifier
    for identifier in item_identifiers:
        try:
            cache_key = generate_cache_key(identifier)
            if cache_key in cached_items:
                item_data = cached_items[cache_key].get("item_data")
                result["cached_items"][identifier] = item_data
                result["statistics"]["found_in_cache"] += 1
            else:
                result["non_cached_items"].append(identifier)
                result["statistics"]["not_in_cache"] += 1
                
        except ValueError:
            result["invalid_identifiers"].append(identifier)
            result["statistics"]["invalid_items"] += 1
    
    logger.info(f"Bulk cache lookup: {result['statistics']['found_in_cache']} items found, "
               f"{result['statistics']['not_in_cache']} not in cache")
    
    return result


def filter_non_cached_items(items_list: List[Dict[str, Any]], 
                          cache_file_path: str = DEFAULT_CACHE_PATH,
                          product_code_field: str = "product_code") -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Filter a list of items to return only non-cached items for processing.
    
    Args:
        items_list: List of item dictionaries to check
        cache_file_path: Path to cache file
        product_code_field: Field name containing product code in each item
        
    Returns:
        Tuple containing:
        - List of items not found in cache
        - Statistics dictionary
    """
    logger.info(f"Filtering {len(items_list)} items for cache status")
    
    non_cached_items = []
    cache_hit_count = 0
    missing_code_count = 0
    
    # Load cache once for efficiency
    try:
        cache_data = load_existing_cache(cache_file_path)
        cached_items = cache_data.get("cached_items", {})
    except Exception as e:
        logger.error(f"Error loading cache for filtering: {e}")
        # If we can't load cache, consider all items as non-cached
        return items_list, {
            "total_items": len(items_list),
            "cache_hits": 0,
            "cache_misses": len(items_list),
            "missing_product_codes": 0,
            "hit_rate": 0.0
        }
    
    # Filter items
    for item in items_list:
        product_code = item.get(product_code_field)
        
        if not product_code:
            missing_code_count += 1
            non_cached_items.append(item)  # Include items without product code
            continue
        
        try:
            cache_key = generate_cache_key(product_code)
            if cache_key not in cached_items:
                non_cached_items.append(item)
            else:
                cache_hit_count += 1
                
        except ValueError:
            non_cached_items.append(item)  # Include items with invalid product codes
            
    # Calculate statistics
    total_items = len(items_list)
    cache_miss_count = len(non_cached_items) - missing_code_count
    hit_rate = (cache_hit_count / total_items * 100) if total_items > 0 else 0
    
    statistics = {
        "total_items": total_items,
        "cache_hits": cache_hit_count,
        "cache_misses": cache_miss_count,
        "missing_product_codes": missing_code_count,
        "hit_rate": round(hit_rate, 2)
    }
    
    logger.info(f"Filtering complete: {cache_hit_count} cache hits, {cache_miss_count} cache misses, "
               f"{missing_code_count} missing product codes, {hit_rate:.1f}% hit rate")
               
    return non_cached_items, statistics


def validate_cache_freshness(cache_file_path: str = DEFAULT_CACHE_PATH, 
                           max_age_hours: int = 24) -> Dict[str, Any]:
    """
    Validate if the cache is fresh enough to be reliable.
    
    Args:
        cache_file_path: Path to cache file
        max_age_hours: Maximum age in hours for cache to be considered fresh
        
    Returns:
        Dictionary with validation results
    """
    import datetime
    
    validation_result = {
        "is_valid": False,
        "exists": False,
        "is_readable": False,
        "is_fresh": False,
        "age_hours": None,
        "item_count": 0,
        "last_updated": None
    }
    
    # Check if file exists
    if not os.path.isfile(cache_file_path):
        logger.warning(f"Cache validation failed: file {cache_file_path} does not exist")
        return validation_result
    
    validation_result["exists"] = True
    
    # Try to load and parse
    try:
        cache_data = load_existing_cache(cache_file_path)
        validation_result["is_readable"] = True
        
        # Get metadata
        metadata = cache_data.get("metadata", {})
        cached_items = cache_data.get("cached_items", {})
        
        validation_result["item_count"] = len(cached_items)
        validation_result["last_updated"] = metadata.get("last_updated")
        
        # Check freshness if last_updated exists
        if validation_result["last_updated"]:
            try:
                last_update = datetime.datetime.fromisoformat(validation_result["last_updated"])
                age_delta = datetime.datetime.now() - last_update
                age_hours = age_delta.total_seconds() / 3600
                
                validation_result["age_hours"] = round(age_hours, 2)
                validation_result["is_fresh"] = age_hours <= max_age_hours
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid timestamp format in cache: {e}")
        
        # Overall validity
        validation_result["is_valid"] = (
            validation_result["exists"] and 
            validation_result["is_readable"] and 
            validation_result["is_fresh"] and
            validation_result["item_count"] > 0
        )
        
        return validation_result
        
    except Exception as e:
        logger.error(f"Cache validation error: {e}")
        validation_result["is_readable"] = False
        return validation_result