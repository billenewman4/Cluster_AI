"""
Cache Management System

A modular caching system for storing and retrieving approved items from Firebase.
"""

# Import public API components for easier access
from src.Caching.acceptance_filter import is_item_approved, filter_approved_items
from src.Caching.cache_manager import (
    generate_cache_key,
    load_existing_cache, 
    update_cache,
    save_cache,
    get_cache_statistics,
    DEFAULT_CACHE_PATH
)
from src.Caching.cache_query import (
    is_item_cached,
    get_cached_item_data,
    get_cached_items_bulk,
    filter_non_cached_items,
    validate_cache_freshness
)
from src.Caching.cache_orchestrator import (
    refresh_cache,
    incremental_cache_update,
    validate_cache_integrity
)

# Package metadata
__version__ = "1.0.0"
__author__ = "Cluster AI Team"
