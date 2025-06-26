"""
Batch Processor for LLM Extraction
Handles processing multiple records with rate limiting, caching, and error handling.
Uses the new base extractor architecture.
"""

import pandas as pd
import logging
import time
import hashlib
import json
from typing import Dict, List, Optional
from pathlib import Path
from dataclasses import asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Using the more optimized base extractor class only - no need for specific extractor imports
from .base_extractor import BaseLLMExtractor
from src.models.product_model import ProductData

logger = logging.getLogger(__name__)

class BatchProcessor:
    """Process large batches of records with LLM extraction."""
    
    def __init__(self, extractors: Dict[str, BaseLLMExtractor], cache_file: Optional[str] = None):
        self.extractors = extractors
        self.cache_file = cache_file or "data/processed/.llm_cache.json"
        self.cache = self._load_cache()
        self.cache_lock = Lock()  # Thread safety for cache
        
        # Rate limiting - reduce for speed but avoid hitting limits
        self.max_concurrent = 5  # Reduced from 10 to avoid rate limits
        self.requests_per_minute = 100  # Reduced from 150 to avoid rate limits
        self.request_interval = 60.0 / self.requests_per_minute
        self.last_request_time = 0.0
    
    def _load_cache(self) -> Dict[str, Dict]:
        """Load extraction cache to avoid re-processing same descriptions."""
        try:
            cache_path = Path(self.cache_file)
            if cache_path.exists():
                with open(cache_path, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load cache: {e}")
        return {}
    
    def _save_cache(self):
        """Save extraction cache."""
        try:
            cache_path = Path(self.cache_file)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(cache_path, 'w') as f:
                json.dump(self.cache, f, indent=2)
        except Exception as e:
            logger.warning(f"Could not save cache: {e}")
    
    def _get_cache_key(self, description: str, category: str) -> str:
        """Generate cache key for description + category."""
        # Defensive programming - ensure we have valid strings
        safe_description = str(description) if description is not None else ""
        safe_category = str(category) if category is not None else ""
        content = f"{safe_category}:{safe_description}"
        return hashlib.sha256(content.encode()).hexdigest()
    
    def _rate_limit(self):
        """Apply rate limiting with reduced delays for speed."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.request_interval:
            sleep_time = self.request_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _process_single_record(self, product: ProductData, category: str) -> ProductData:
        """Process a single ProductData object with caching and error handling.
        
        Args:
            product: ProductData object to enrich with extracted fields
            category: Category name for selecting appropriate extractor
            
        Returns:
            The same ProductData object with extracted fields populated
        """
        description = product.productdescription
        
        # Validate description - don't waste API calls on empty/null descriptions
        if description is None or not str(description).strip():
            logger.warning(f"Skipping extraction for record with null/empty description")
            product.subprimal = None
            product.grade = None
            product.size = None
            product.size_uom = None
            product.brand = None
            product.bone_in = False
            product.confidence = 0.0
            product.needs_review = True
            return product
            
        cache_key = self._get_cache_key(str(description), category)
        
        # Check cache first
        with self.cache_lock:
            if cache_key in self.cache:
                logger.debug(f"Cache hit for: {description[:50]}...")
                cached_result = self.cache[cache_key]
                # Update ProductData with cached values
                for field, value in cached_result.items():
                    if hasattr(product, field):
                        setattr(product, field, value)
                return product
        
        # Get extractor
        extractor = self.extractors.get(category.lower())
        if not extractor:
            logger.error(f"No extractor found for category: {category}")
            product.subprimal = None
            product.grade = None
            product.size = None
            product.size_uom = None
            product.brand = None
            product.bone_in = False
            product.confidence = 0.0
            product.needs_review = True
            return product
        
        # Apply rate limiting
        self._rate_limit()
        
        # Extract with retry logic
        max_retries = 2  # Reduced for speed
        for attempt in range(max_retries + 1):
            try:
                # Pass the ProductData object to extractor to be updated in place
                updated_product = extractor.extract(product)
                
                # Cache the result
                cacheable_fields = [
                    'subprimal', 'grade', 'size', 'size_uom', 'brand', 'bone_in', 
                    'confidence', 'needs_review', 'primal', 'species'
                ]
                cache_data = {field: getattr(updated_product, field, None) 
                             for field in cacheable_fields 
                             if hasattr(updated_product, field)}
                             
                with self.cache_lock:
                    self.cache[cache_key] = cache_data
                
                logger.debug(f"Processed: {description[:50]}... -> {updated_product.subprimal}")
                return updated_product
                
            except Exception as e:
                if attempt < max_retries:
                    wait_time = (2 ** attempt) * 0.5  # Reduced backoff
                    logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"All attempts failed for {description[:50]}...: {e}")
                    
                    # Mark product as failed
                    product.subprimal = None
                    product.grade = None
                    product.size = None
                    product.size_uom = None
                    product.brand = None
                    product.bone_in = False
                    product.confidence = 0.0
                    product.needs_review = True
                    return product
    
    def process_batch(self, products: List[ProductData], category: str) -> List[ProductData]:
        """Process a batch of ProductData objects with parallel execution.
        
        Args:
            products: List of ProductData objects to enrich
            category: Category name for selecting appropriate extractor
            
        Returns:
            List of enriched ProductData objects
        """
        logger.info(f"Processing {len(products)} products for {category}")
        logger.info(f"First product description: {products[0].productdescription[:50] if products else None}...")
        
        # Process in parallel
        processed_products = []
        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            # Submit all tasks
            future_to_product = {
                executor.submit(self._process_single_record, product, category): product
                for product in products
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_product):
                try:
                    processed_product = future.result()
                    processed_products.append(processed_product)
                except Exception as e:
                    product = future_to_product[future]
                    logger.error(f"Failed to process product: {e}")
                    # Mark failed product
                    product.subprimal = None
                    product.grade = None
                    product.size = None
                    product.size_uom = None
                    product.brand = None
                    product.bone_in = False
                    product.confidence = 0.0
                    product.needs_review = True
                    processed_products.append(product)
            
        # Save cache after batch
        self._save_cache()
        
        logger.info(f"Completed processing {len(processed_products)} products for {category}")
        return processed_products
    
    def process_category_batch(self, products: List[ProductData], category: str) -> List[ProductData]:
        """Process a batch of ProductData objects for a specific category.
        
        Args:
            products: List of ProductData objects to process
            category: Category name for selecting appropriate extractor
            
        Returns:
            List of processed ProductData objects
        """
        # Get category-appropriate extractor
        extractor = self.extractors.get(category.lower())
        if not extractor:
            logger.error(f"No extractor found for category: {category}")
            # Return original data - no processing
            return products
        
        # Log information about this category batch
        logger.info(f"Category batch: {category}, {len(products)} products")
        
        # Process records with extractor
        processed_products = []
        
        for idx, product in enumerate(products):
            logger.debug(f"Processing {category} product {idx}/{len(products)}")
            
            try:
                # Process with caching and error handling
                processed = self._process_single_record(product, category)
                processed_products.append(processed)
                    
            except Exception as e:
                logger.error(f"Failed to process product {idx}: {str(e)}")
                # Mark as failed product
                product.species = 'Beef' if 'beef' in category.lower() else None
                product.primal = 'Chuck' if 'chuck' in category.lower() else 'Unknown'
                product.subprimal = None
                product.grade = None
                product.size = None
                product.size_uom = None
                product.brand = None
                product.bone_in = False
                product.confidence = 0.0
                product.needs_review = True
                processed_products.append(product)
        
        # Log summary statistics
        if processed_products:
            # Calculate statistics from ProductData objects
            confidences = [p.confidence for p in processed_products if hasattr(p, 'confidence') and p.confidence is not None]
            needs_review_count = sum(1 for p in processed_products if hasattr(p, 'needs_review') and p.needs_review)
            avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0
            
            # Cache statistics
            unique_requests = len([k for k in self.cache.keys() if k.startswith(category.lower())])
            cache_hit_rate = (len(products) - unique_requests) / len(products) if products else 0
            
            logger.info(f"Batch processing complete for {category}:")
            logger.info(f"  Products processed: {len(processed_products)}")
            logger.info(f"  Average confidence: {avg_confidence:.3f}")
            logger.info(f"  Products needing review: {needs_review_count}")
            logger.info(f"  Cache hit rate: {cache_hit_rate:.1%}")
        
        return processed_products
    
    def process_category(self, category: str) -> List[ProductData]:
        """Process all records for a given category.
        
        Args:
            category: Category name to process
            
        Returns:
            List of processed ProductData objects
        """
        # Load category data
        from ..utils.data_loaders import load_category_data
        from ..models.product_model import ProductData
        
        logger.info(f"Loading data for category: {category}")
        category_df = load_category_data(category)
        
        if category_df is None or len(category_df) == 0:
            logger.error(f"No data found for category: {category}")
            return []
            
        logger.info(f"Loaded {len(category_df)} records for {category}")
        
        # Convert DataFrame to ProductData objects
        products = []
        for _, row in category_df.iterrows():
            product = ProductData(
                productdescription=row.get('productdescription', ''),
                category=category,
                productcode=row.get('productcode'),
                source_filename=row.get('source_filename'),
                row_number=row.get('row_number')
            )
            products.append(product)
        
        # Process batch
        processed_products = self.process_category_batch(products, category)
        
        return processed_products
    
    def get_cache_stats(self) -> Dict:
        """Get caching statistics."""
        return {
            'cache_size': len(self.cache),
            'total_requests_made': len(self.request_times),
            'supported_categories': list(self.extractors.keys())
        } 