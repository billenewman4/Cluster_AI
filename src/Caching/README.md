# Accepted Items Caching System

## Overview

The Caching System provides an efficient way to store and retrieve approved items from Firebase collections. It implements a modular architecture that separates concerns of database access, item filtering, cache management, and query operations.

## Key Features

- **Performance Optimization**: Reduces Firebase query load by caching approved items locally
- **Modular Design**: Separates concerns into acceptance filtering, cache management, and querying
- **Atomic File Operations**: Prevents cache corruption during writes
- **Incremental Updates**: Supports both full cache refresh and incremental updates
- **Cache Validation**: Built-in integrity checking and freshness validation

## Components

The caching system consists of the following components:

1. **`acceptance_filter.py`**: Filters items based on approval status
2. **`cache_manager.py`**: Handles cache CRUD operations and key generation
3. **`cache_query.py`**: Provides interfaces for cache lookups and filtering
4. **`cache_orchestrator.py`**: Coordinates the entire cache refresh process

## Usage Examples

### Basic Cache Refresh

```python
from src.Caching import refresh_cache

# Refresh the cache from a specific collection
result = refresh_cache(
    collection_name="reviewed_beef_cuts_latest_master_20250616_20250617_102108"
)

# Check the results
if result['success']:
    print(f"Cache now contains {result.get('cache_statistics', {}).get('total_items', 0)} items")
else:
    print(f"Cache refresh failed: {result.get('error')}")
```

### Checking if an Item is Cached

```python
from src.Caching import is_item_in_cache

# Check if a specific product code is in the cache
product_code = "ABC123"
is_cached = is_item_in_cache(product_code)

if is_cached:
    print(f"Product {product_code} is already cached")
else:
    print(f"Product {product_code} is not cached")
```

### Performing Bulk Cache Lookup

```python
from src.Caching import batch_lookup_items

# Get cached data for multiple product codes
product_codes = ["ABC123", "DEF456", "GHI789"]
results = batch_lookup_items(product_codes)

for code, item in results.items():
    if item:
        print(f"Found cached item: {code}")
    else:
        print(f"Item not cached: {code}")
```

### Cache Validation

```python
from src.Caching import validate_cache_integrity

# Validate cache structure and data
validation = validate_cache_integrity()

if validation['valid']:
    print("Cache integrity check passed!")
    print(f"Last updated: {validation.get('last_updated')}")
else:
    print(f"Cache validation failed: {validation.get('errors')}")
```

## Cache File Structure

The cache is stored as a JSON file at `data/processed/.accepted_items_cache.json` with the following structure:

```json
{
  "metadata": {
    "last_updated": "2025-06-25T15:28:51.210",
    "source_collection": "reviewed_beef_cuts_latest_master_20250616_20250617_102108",
    "item_count": 272
  },
  "cached_items": {
    "ABC123": { /* item data */ },
    "DEF456": { /* item data */ }
  }
}
```

## Performance Considerations

- Cache size scales with the number of approved items
- Incremental updates minimize processing time for frequent refreshes
- Case-normalized product codes ensure consistent lookups
