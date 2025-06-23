# Caching System PRD - Product Requirements Document

## Overview
This document outlines the step-by-step implementation requirements for a caching system that stores accepted/approved items from Firebase, enabling future analysis to efficiently skip already processed items.

## Objectives
1. Retrieve latest database from Firebase (production database)
2. Filter for all currently accepted items
3. Cache accepted items while removing previously cached items that are no longer accepted
4. Provide cache structure that allows future analysis to easily filter out cached items

## Technical Architecture

### Prerequisites
- Use existing `src/database/firebase_client.py` for database access
- Use existing `src/database/beef_cuts_store.py` for specialized operations
- Leverage existing approval logic from `src/database/run_scripts/extra_approved.py`

## Cache Item Identification Strategy

### Unique and Repeatable Item IDs

To ensure cache items are both **unique** (no collisions) and **repeatable** (same item from frontend generates same ID), we'll use **Product Codes** as the primary cache identifier strategy:

**Primary Strategy: Product Code Based Identification**
```python
def generate_cache_key(product_code: str) -> str:
    """
    Generate a unique, repeatable cache key for an item using product code.
    
    Args:
        product_code: The unique product code from the data source
    
    Returns:
        Normalized product code string (no hashing needed)
    """
    # Normalize product code to ensure consistency
    if not product_code:
        raise ValueError("Product code is required for cache key generation")
    
    # Convert to string and normalize (uppercase, strip whitespace)
    normalized_code = str(product_code).strip().upper()
    
    return normalized_code
```

**Why Product Codes Are Superior:**

1. **Natural Uniqueness**: Product codes are business identifiers designed to be unique
2. **Maximum Stability**: Product codes don't change when descriptions are modified/merged
3. **Future-Proof**: Adding new description fields won't affect cache keys
4. **Simple & Fast**: No hashing overhead, direct string comparison
5. **Business Logic**: Aligns with how products are actually identified in business systems
6. **Frontend Simplicity**: Frontend just needs the product code (no field merging)

**System Evidence:**
- `ProductTransformer.unique_product_codes()` ensures product code uniqueness
- `product_code` is a required column throughout the system
- Product codes are consistently converted to string type for reliability
- All processing workflows use `product_code` as the primary identifier

**Cache Key Examples:**
```python
# Simple, direct mapping
product_code = "12345"          → cache_key = "12345"
product_code = " abc-123 "      → cache_key = "ABC-123"
product_code = "xyz_789"        → cache_key = "XYZ_789"
```

**Clean Start Approach:**
- Start with a fresh cache file: `data/processed/.accepted_items_cache.json`
- No migration from existing `.fast_batch_cache.json` needed
- Product codes only - no fallback strategies required
- System validates that all items have valid product codes before caching

## Step-by-Step Implementation

### Step 1: Database Retrieval
**Implementation:** Use existing `FirebaseClient` directly - **NO NEW FILE NEEDED**

**Requirements:**
- Use existing `FirebaseClient` from `src/database/firebase_client.py` 
- Reuse existing connection, credential loading, and error handling
- Follow same pattern as `extra_approved.py` for simplicity

**Implementation Pattern (from existing code):**
```python
from src.database.firebase_client import FirebaseClient

# Initialize client (reuses existing singleton, credentials, error handling)
firebase_client = FirebaseClient(
    project_id=project_id,
    credentials_path=credentials_path,
    collection_name=collection_name
)

# Get all documents (reuses existing batch retrieval with built-in pagination)
all_items = firebase_client.get_documents(
    collection=collection_name,
    limit=10000  # Handles large datasets automatically
)

logger.info(f"Retrieved {len(all_items)} items from Firebase")
```

**Why no new class needed:**
- `FirebaseClient.get_documents()` already handles batch retrieval, pagination, and error handling
- Connection setup and credential loading already robust and tested
- Same pattern successfully used in `extra_approved.py` and `ret_comments.py`

### Step 2: Acceptance Filtering
**File to create:** `src/Caching/acceptance_filter.py`

**Requirements:**
- Use existing approval logic from `src/database/run_scripts/extra_approved.py`
- Filter items based on **approved status only** (keep it simple):
  - **ONLY CRITERIA:** `approved` field values: ['approved', 'yes', 'y', 'true', '1', 'accept', 'accepted', '✓']

**Key Functions:**
```python
def is_item_approved(item_data):
    """Simple check if item is approved - reuses exact logic from extra_approved.py"""
    pass

def filter_approved_items(all_items):
    """Filter list of items for approved ones only"""
    pass
```

### Step 3: Cache Management
**File to create:** `src/Caching/cache_manager.py`

**Requirements:**
- Create/update cache file at `data/processed/.accepted_items_cache.json`
- Compare new accepted items with existing cache
- Remove items from cache that are no longer accepted
- Add newly accepted items to cache
- Maintain cache metadata (last_updated, total_count, cache_version)

**Cache Structure:**
```json
{
  "metadata": {
    "last_updated": "ISO-8601-timestamp",
    "total_cached_items": "number",
    "cache_version": "1.0",
    "source_collection": "collection_name",
    "cache_key_strategy": "product_code",
    "filtering_criteria": {
      "approved_field_values": ["approved", "yes", "y", "true", "1", "accept", "accepted", "✓"]
    }
  },
  "cached_items": {
    "PRODUCT_CODE_123": {
      "product_code": "PRODUCT_CODE_123",
      "document_id": "firebase_doc_id",
      "cached_timestamp": "ISO-8601-timestamp",
      "cache_reason": "approved",
      "item_data": {
        "product_description": "merged_description",
        "category": "Beef Chuck",
        "subprimal": "Chuck Roll",
        "grade": "USDA Choice",
        "confidence": 0.95,
        "approved": "approved",
        "needs_review": false
      }
    }
  }
}
```

**Key Functions:**
```python
def generate_cache_key(product_code: str) -> str:
    """Generate unique, repeatable cache key using product code"""
    if not product_code:
        raise ValueError("Product code is required for cache key generation")
    return str(product_code).strip().upper()

def load_existing_cache(cache_file_path):
    """Load existing cache or create new empty cache structure"""
    pass

def update_cache(existing_cache, new_accepted_items, removed_items):
    """Update cache with new accepted items and remove no-longer-accepted items"""
    pass

def save_cache(cache_data, cache_file_path):
    """Save updated cache to file with atomic write operation"""
    pass

def get_cache_statistics(cache_data):
    """Generate cache statistics and health metrics"""
    pass
```

### Step 4: Cache Query Interface
**File to create:** `src/Caching/cache_query.py`

**Requirements:**
- Provide easy interface for future analysis to check if items are cached
- Support bulk lookups for efficiency
- Return cached item data when requested
- Provide cache hit/miss statistics

**Key Functions:**
```python
def is_item_cached(item_identifier, cache_file_path):
    """Check if a single item is in cache"""
    pass

def get_cached_items_bulk(item_identifiers, cache_file_path):
    """Bulk lookup for multiple items - returns dict of cached vs non-cached"""
    pass

def get_cached_item_data(item_identifier, cache_file_path):
    """Retrieve full cached data for an item"""
    pass

def filter_non_cached_items(items_list, cache_file_path):
    """Filter a list of items to return only non-cached items for processing"""
    pass
```

### Step 5: Main Orchestrator
**File to create:** `src/Caching/cache_orchestrator.py`

**Requirements:**
- Orchestrate the complete cache refresh process
- Use existing database access files (no rewriting)
- Provide detailed logging and progress tracking
- Handle errors gracefully with rollback capabilities
- Generate summary reports

**Key Functions:**
```python
def refresh_cache(collection_name, cache_file_path, project_id=None, credentials_path=None):
    """Main function to refresh the entire cache"""
    pass

def incremental_cache_update(collection_name, cache_file_path, last_update_timestamp):
    """Update cache with only items modified since last update"""
    pass

def validate_cache_integrity(cache_file_path):
    """Validate cache structure and data integrity"""
    pass
```

## Usage Examples

### For Cache Refresh
```python
from src.Caching.cache_orchestrator import refresh_cache

# Full cache refresh
result = refresh_cache(
    collection_name="beef_cuts_extracted",
    cache_file_path="data/processed/.accepted_items_cache.json"
)
```

### For Analysis Tools
```python
from src.Caching.cache_query import filter_non_cached_items, is_item_cached

# Check single item by product code
if is_item_cached("PRODUCT_CODE_123", "data/processed/.accepted_items_cache.json"):
    print("✅ Item already processed - skip analysis")
else:
    print("🔄 New item - proceed with analysis")

# Filter out cached items before processing
items_to_process = filter_non_cached_items(
    items_list=all_extracted_items,
    cache_file_path="data/processed/.accepted_items_cache.json"
)
```

## Integration Points

### With Existing Systems
1. **Firebase Client Integration**: Use `FirebaseClient` singleton pattern from `src/database/firebase_client.py`
2. **Approval Logic**: Reuse approval criteria from `src/database/run_scripts/extra_approved.py`
3. **Product Code Validation**: Leverage `ProductTransformer.unique_product_codes()` for validation
4. **Clean Architecture**: Start fresh - no dependency on existing `.fast_batch_cache.json`

### With Future Analysis
1. **Pre-processing Check**: Analysis tools should check cache before processing items
2. **Cache Validation**: Validate cache freshness before relying on it
3. **Bypass Option**: Provide option to bypass cache for full reprocessing

## Performance Requirements
- Cache refresh should complete within 10 minutes for 50K items
- Cache queries should respond within 100ms for single items
- Bulk cache queries should handle 1000+ items efficiently
- Memory usage should not exceed 1GB during refresh operations

## Monitoring and Logging
- Log cache refresh statistics (added, removed, unchanged items)
- Track cache hit/miss rates during analysis
- Monitor cache file size and growth patterns
- Alert on cache corruption or significant changes in acceptance rates

## File Structure
```
src/Caching/
├── cache_management_PRD.md          # This document
├── database_retriever.py            # Step 1: Database access
├── acceptance_filter.py             # Step 2: Filtering logic
├── cache_manager.py                 # Step 3: Cache CRUD operations
├── cache_query.py                   # Step 4: Cache query interface
├── cache_orchestrator.py            # Step 5: Main orchestration
└── __init__.py                      # Package initialization
```

## Success Criteria
1. ✅ Cache system uses existing database files without modification
2. ✅ Accepted items are correctly identified and cached
3. ✅ Previously cached items that are no longer accepted are removed
4. ✅ Future analysis can easily filter out cached items
5. ✅ System provides comprehensive logging and error handling
6. ✅ Cache refresh process is atomic and recoverable 