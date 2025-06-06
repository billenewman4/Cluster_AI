"""
Beef Cuts Firebase Storage Module

Provides specialized methods for storing and retrieving beef cut extraction data
in Firebase with optimized access patterns and data validation.
"""

import os
import logging
import pandas as pd
from typing import Dict, List, Optional, Union, Any
from datetime import datetime

from src.database.firebase_client import FirebaseClient

# Configure logging
logger = logging.getLogger(__name__)

class BeefCutsStore:
    """
    Specialized Firebase storage handler for beef cut extraction data.
    
    Provides efficient methods for storing extraction results, retrieving
    cut data, and managing extraction history with optimized queries.
    """
    
    def __init__(self, collection_prefix: str = "beef_cuts"):
        """
        Initialize the beef cuts storage module with configuration.
        
        Args:
            collection_prefix: Prefix for Firebase collections
        """
        self.firebase = FirebaseClient(collection_name=f"{collection_prefix}_extracted")
        self.history_collection = f"{collection_prefix}_history"
        self.reference_collection = f"{collection_prefix}_reference"
    
    def store_extraction_batch(self, extraction_data: List[Dict], 
                              category: Optional[str] = None) -> Dict[str, Any]:
        """
        Store a batch of extraction results with optimized batch writes.
        
        Args:
            extraction_data: List of extracted beef cut dictionaries
            category: Optional category label for the batch
            
        Returns:
            Dict with success/failure counts and batch_id
        """
        if not extraction_data:
            return {"success": 0, "failed": 0, "batch_id": None}
            
        batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        if category:
            batch_id = f"{category}_{batch_id}"
            
        # Create batch operations
        operations = []
        batch_summary = {
            "batch_id": batch_id,
            "timestamp": datetime.now().isoformat(),
            "category": category,
            "total_records": len(extraction_data),
            "confidence_avg": 0,
            "needs_review_count": 0
        }
        
        confidence_sum = 0
        needs_review = 0
        
        # Process each extraction result
        for i, item in enumerate(extraction_data):
            doc_id = f"{batch_id}_{i:04d}"
            
            # Add metadata
            item_with_meta = item.copy()
            item_with_meta["batch_id"] = batch_id
            item_with_meta["timestamp"] = datetime.now().isoformat()
            item_with_meta["category"] = category
            
            # Track metrics
            if "confidence" in item:
                confidence_sum += float(item["confidence"]) if item["confidence"] else 0
                
            if "needs_review" in item and item["needs_review"]:
                needs_review += 1
                
            # Add to operations
            operations.append({
                "op": "set",
                "doc_id": doc_id,
                "data": item_with_meta
            })
        
        # Update batch summary with metrics
        if extraction_data:
            batch_summary["confidence_avg"] = confidence_sum / len(extraction_data)
        batch_summary["needs_review_count"] = needs_review
            
        # Store batch summary
        self.firebase.add_document(
            batch_summary, 
            collection=self.history_collection,
            doc_id=batch_id
        )
        
        # Execute batch write
        success_count = self.firebase.batch_write(operations)
        
        return {
            "success": success_count,
            "failed": len(extraction_data) - success_count,
            "batch_id": batch_id
        }
    
    def store_extraction_from_dataframe(self, df: pd.DataFrame, 
                                       category: Optional[str] = None) -> Dict[str, Any]:
        """
        Store extraction results from a pandas DataFrame with memory optimization.
        
        Args:
            df: DataFrame with extraction results 
            category: Optional category label
            
        Returns:
            Dict with success/failure counts and batch_id
        """
        extraction_data = df.to_dict(orient="records")
        return self.store_extraction_batch(extraction_data, category)
    
    def get_extraction_batch(self, batch_id: str) -> List[Dict]:
        """
        Retrieve all extraction results for a specific batch.
        
        Args:
            batch_id: ID of the batch to retrieve
            
        Returns:
            List of extraction dictionaries
        """
        filters = [{"field": "batch_id", "op": "==", "value": batch_id}]
        return self.firebase.get_documents(filters=filters)
    
    def get_items_needing_review(self, 
                               category: Optional[str] = None,
                               limit: int = 100) -> List[Dict]:
        """
        Get items flagged for manual review with efficient query.
        
        Args:
            category: Optional category filter
            limit: Maximum number of items to return
            
        Returns:
            List of items needing review
        """
        filters = [{"field": "needs_review", "op": "==", "value": True}]
        
        if category:
            filters.append({"field": "category", "op": "==", "value": category})
            
        return self.firebase.get_documents(
            filters=filters,
            limit=limit,
            order_by="timestamp",
            direction="DESCENDING"
        )
    
    def store_reference_data(self, reference_data: Dict[str, Any], 
                           reference_id: str = "beef_cuts_reference") -> bool:
        """
        Store reference data used for extraction in Firebase.
        
        Args:
            reference_data: Dictionary of reference data
            reference_id: Document ID for the reference data
            
        Returns:
            True if successful, False otherwise
        """
        reference_data["updated_at"] = datetime.now().isoformat()
        
        return bool(self.firebase.add_document(
            reference_data,
            collection=self.reference_collection,
            doc_id=reference_id
        ))
    
    def get_reference_data(self, reference_id: str = "beef_cuts_reference") -> Optional[Dict]:
        """
        Retrieve reference data from Firebase.
        
        Args:
            reference_id: Document ID for the reference data
            
        Returns:
            Reference data dictionary or None if not found
        """
        return self.firebase.get_document(
            doc_id=reference_id,
            collection=self.reference_collection
        )
    
    def update_extraction_review_status(self, doc_id: str, 
                                      status: str, 
                                      reviewer: Optional[str] = None) -> bool:
        """
        Update the review status of an extraction result.
        
        Args:
            doc_id: Document ID to update
            status: New review status (e.g., 'approved', 'rejected', 'fixed')
            reviewer: Optional name of reviewer
            
        Returns:
            True if successful, False otherwise
        """
        update_data = {
            "review_status": status,
            "reviewed_at": datetime.now().isoformat()
        }
        
        if reviewer:
            update_data["reviewer"] = reviewer
            
        return self.firebase.update_document(doc_id, update_data)
    
    def get_extraction_stats(self) -> Dict[str, Any]:
        """
        Get statistics about extraction data with efficient aggregation.
        
        Returns:
            Dictionary of extraction statistics
        """
        # Get batch histories
        batch_data = self.firebase.get_documents(
            collection=self.history_collection,
            limit=100,
            order_by="timestamp",
            direction="DESCENDING"
        )
        
        # Count needs review
        review_filter = [{"field": "needs_review", "op": "==", "value": True}]
        review_count = len(self.firebase.get_documents(filters=review_filter, limit=1000))
        
        # Count total records
        total_docs = sum(batch["total_records"] for batch in batch_data if "total_records" in batch)
        
        # Calculate average confidence
        weighted_conf = sum(
            batch["confidence_avg"] * batch["total_records"] 
            for batch in batch_data 
            if "confidence_avg" in batch and "total_records" in batch
        )
        avg_confidence = weighted_conf / total_docs if total_docs > 0 else 0
        
        return {
            "total_records": total_docs,
            "needs_review_count": review_count,
            "average_confidence": avg_confidence,
            "batch_count": len(batch_data),
            "latest_extraction": batch_data[0]["timestamp"] if batch_data else None
        }
