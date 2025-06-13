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
    
    def _process_single_record(self, record: Dict, category: str) -> Dict:
        """Process a single record with caching and error handling."""
        description = record.get('product_description')
        
        # Validate description - don't waste API calls on empty/null descriptions
        if description is None or not str(description).strip():
            logger.warning(f"Skipping extraction for record with null/empty description")
            result = record.copy()
            result.update({
                'subprimal': None,
                'grade': None,
                'size': None,
                'size_uom': None,
                'brand': None,
                'bone_in': False,
                'confidence': 0.0,
                'needs_review': True
            })
            return result
            
        cache_key = self._get_cache_key(str(description), category)
        
        # Check cache first
        with self.cache_lock:
            if cache_key in self.cache:
                logger.debug(f"Cache hit for: {description[:50]}...")
                cached_result = self.cache[cache_key]
                # Add original record data
                result = record.copy()
                result.update(cached_result)
                return result
        
        # Get extractor
        extractor = self.extractors.get(category.lower())
        if not extractor:
            logger.error(f"No extractor found for category: {category}")
            result = record.copy()
            result.update({
                'subprimal': None,
                'grade': None,
                'size': None,
                'size_uom': None,
                'brand': None,
                'bone_in': False,
                'confidence': 0.0,
                'needs_review': True
            })
            return result
        
        # Apply rate limiting
        self._rate_limit()
        
        # Extract with retry logic
        max_retries = 2  # Reduced for speed
        for attempt in range(max_retries + 1):
            try:
                extraction_result = extractor.extract(description)
                
                # Cache the result
                cache_data = asdict(extraction_result)
                with self.cache_lock:
                    self.cache[cache_key] = cache_data
                
                # Combine with original record
                result = record.copy()
                result.update(cache_data)
                
                logger.debug(f"Processed: {description[:50]}... -> {extraction_result.subprimal}")
                return result
                
            except Exception as e:
                if attempt < max_retries:
                    wait_time = (2 ** attempt) * 0.5  # Reduced backoff
                    logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"All attempts failed for {description[:50]}...: {e}")
                    
                    # Return failed result
                    result = record.copy()
                    result.update({
                        'subprimal': None,
                        'grade': None,
                        'size': None,
                        'size_uom': None,
                        'brand': None,
                        'bone_in': False,
                        'confidence': 0.0,
                        'needs_review': True
                    })
                    return result
    
    def process_batch(self, df: pd.DataFrame, category: str) -> pd.DataFrame:
        """Process a batch of records with parallel execution."""
        logger.info(f"Processing {len(df)} records for {category}")
        
        # Convert to list of dicts for parallel processing
        records = df.to_dict('records')
        logger.info(f"Converted {len(df)} records to list of dicts")
        logger.info(f"First record: {records[0]}")
        
        # Process in parallel
        results = []
        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            # Submit all tasks
            future_to_record = {
                executor.submit(self._process_single_record, record, category): record
                for record in records
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_record):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    record = future_to_record[future]
                    logger.error(f"Failed to process record: {e}")
                    # Add failed result
                    failed_result = record.copy()
                    failed_result.update({
                        'subprimal': None,
                        'grade': None,
                        'size': None,
                        'size_uom': None,
                        'brand': None,
                        'bone_in': False,
                        'confidence': 0.0,
                        'needs_review': True
                    })
                    results.append(failed_result)
        
        # Save cache after batch
        self._save_cache()
        
        # Convert back to DataFrame
        result_df = pd.DataFrame(results)
        
        logger.info(f"Batch processing complete. {len(result_df)} records processed.")
        return result_df
    
    def process_category_batch(self, df: pd.DataFrame, category: str) -> pd.DataFrame:
        """Process a batch of records for a specific category."""
        logger.info(f"Processing {len(df)} {category} records")
        
        results = []
        
        # Process in chunks for better progress reporting
        chunk_size = 20
        total_chunks = (len(df) + chunk_size - 1) // chunk_size
        
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            chunk_num = i // chunk_size + 1
            
            logger.info(f"Processing chunk {chunk_num}/{total_chunks} ({len(chunk)} records)")
            
            for idx, row in chunk.iterrows():
                try:
                    result = self.process_batch(pd.DataFrame([row]), category).iloc[0]
                    results.append(result)
                    
                except Exception as e:
                    raise ValueError(f"Failed to process record {idx}: {str(e)}")
                    # Create a fallback record
                    fallback_result = {
                        'source_filename': row['source_filename'],
                        'row_number': row['row_number'],
                        'product_code': row['product_code'],
                        'raw_description': row['product_description'],
                        'category_description': row['category_description'],
                        'species': 'Beef',
                        'primal': 'Chuck' if 'chuck' in category.lower() else 'Unknown',
                        'subprimal': None,
                        'grade': None,
                        'size': None,
                        'size_uom': None,
                        'brand': None,
                        'bone_in': False,
                        'confidence': 0.0,
                        'needs_review': True
                    }
                    results.append(fallback_result)
        
        result_df = pd.DataFrame(results)
        
        # Log summary statistics
        if len(result_df) > 0:
            avg_confidence = result_df['confidence'].mean()
            needs_review_count = result_df['needs_review'].sum()
            unique_requests = len([k for k in self.cache.keys() if k.startswith(category.lower())])
            cache_hit_rate = (len(df) - unique_requests) / len(df) if len(df) > 0 else 0
            
            logger.info(f"Batch processing complete for {category}:")
            logger.info(f"  Records processed: {len(result_df)}")
            logger.info(f"  Average confidence: {avg_confidence:.3f}")
            logger.info(f"  Records needing review: {needs_review_count}")
            logger.info(f"  Cache hit rate: {cache_hit_rate:.1%}")
        
        return result_df
    
    def process_category(self, category: str) -> pd.DataFrame:
        """Process all records for a given category. Kept for backward compatibility.""" 
        # Load data
        df = pd.read_parquet('data/processed/inventory_base.parquet')
        
        # Filter for category
        filtered_df = df[df['category_description'].str.lower() == category.lower()]
        
        if len(filtered_df) == 0:
            logger.warning(f"No records found for category: {category}")
            return pd.DataFrame()
        
        # Use the new batch processing method
        return self.process_batch(filtered_df, category)
    
    def get_cache_stats(self) -> Dict:
        """Get caching statistics."""
        return {
            'cache_size': len(self.cache),
            'total_requests_made': len(self.request_times),
            'supported_categories': list(self.extractors.keys())
        } 