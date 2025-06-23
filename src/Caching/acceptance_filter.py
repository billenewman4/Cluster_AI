"""
Acceptance Filtering Module

Simple filtering logic to get only approved items for caching.
Reuses existing approval logic from extra_approved.py.
"""

import logging
from typing import List, Dict, Any, Optional

# Configure logging
logger = logging.getLogger(__name__)


def is_item_approved(item_data: Dict[str, Any]) -> bool:
    """
    Simple check if an item is approved - reuses exact logic from extra_approved.py
    
    Args:
        item_data: Dictionary containing item data from Firebase
        
    Returns:
        True if item is approved, False otherwise
        
    REUSES:
    - Exact approval logic from extra_approved.py
    """
    approved_status = str(item_data.get('approved', '')).strip().lower()
    approved_values = [
        'approved', 'yes', 'y', 'true', '1', 'accept', 'accepted', '✓'
    ]
    
    return approved_status in approved_values


def filter_approved_items(all_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Filter list of items for approved ones only - simple and clean.
    
    Args:
        all_items: List of all items from database
        
    Returns:
        Dictionary with filtered results:
        {
            "approved_items": List[Dict],
            "rejected_items": List[Dict], 
            "statistics": Dict
        }
    """
    logger.info(f"Filtering {len(all_items)} items for approved status")
    
    approved_items = []
    rejected_items = []
    
    # Simple filtering using the approved status check
    for item in all_items:
        if is_item_approved(item):
            approved_items.append(item)
        else:
            rejected_items.append(item)
    
    # Calculate simple statistics
    total_items = len(all_items)
    approved_count = len(approved_items)
    rejected_count = len(rejected_items)
    approval_rate = (approved_count / total_items * 100) if total_items > 0 else 0
    
    statistics = {
        "total_items": total_items,
        "approved_items": approved_count,
        "rejected_items": rejected_count,
        "approval_rate_percent": round(approval_rate, 2)
    }
    
    logger.info(f"Filtering complete: {approved_count}/{total_items} items approved ({approval_rate:.1f}%)")
    
    return {
        "approved_items": approved_items,
        "rejected_items": rejected_items,
        "statistics": statistics
    }


# Removed complex analysis functions - keeping it simple with just approved filtering 