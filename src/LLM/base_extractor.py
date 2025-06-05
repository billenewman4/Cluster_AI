"""
Base Extractor Module
Provides the foundation for all specialized LLM extractors with common functionality.
"""

import os
import json
import hashlib
import logging
import random
import time
from pathlib import Path
from typing import Dict, Optional, List, Any

import pandas as pd
from openai import OpenAI

from .models import ExtractionResult

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BaseExtractor:
    """Base class for all LLM extractors with common functionality.
    
    Handles API rate limiting, caching, and other shared utilities
    needed by all specialized extractors.
    """
    
    def __init__(self, processed_dir: str = "data/processed"):
        """Initialize the base extractor.
        
        Args:
            processed_dir: Directory containing processed data files
        """
        self.processed_dir = Path(processed_dir)
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = os.getenv("OPENAI_MODEL", "gpt-4")
        self.max_requests_per_minute = int(os.getenv("MAX_REQUESTS_PER_MINUTE", "100"))
        
        # Rate limiting
        self.request_times = []
        
        # Caching for duplicate descriptions
        self.cache = {}
        
        # Set up reference data
        self.setup_reference_data()
        
    def setup_reference_data(self) -> None:
        """
        Set up reference data needed by extractors.
        This method should be overridden by specialized extractors.
        """
        pass
    
    def get_description_hash(self, description: str) -> str:
        """Generate hash for description to enable caching.
        
        Args:
            description: Product description to hash
            
        Returns:
            str: SHA-256 hash of the description
        """
        return hashlib.sha256(description.encode()).hexdigest()
    
    def enforce_rate_limit(self) -> None:
        """Enforce rate limiting for API calls."""
        current_time = time.time()
        
        # Remove requests older than 1 minute
        self.request_times = [t for t in self.request_times if current_time - t < 60]
        
        # If we're at the limit, wait
        if len(self.request_times) >= self.max_requests_per_minute:
            sleep_time = 60 - (current_time - self.request_times[0]) + random.uniform(1, 3)
            logger.info(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.request_times.append(current_time)
    
    def call_llm(self, system_prompt: str, user_prompt: str, max_retries: int = 3) -> Optional[str]:
        """Make API call to OpenAI with retries and rate limiting.
        
        Args:
            system_prompt: System prompt for LLM
            user_prompt: User prompt containing the description
            max_retries: Maximum number of retry attempts
            
        Returns:
            Optional[str]: LLM response or None if failed
        """
        for attempt in range(max_retries):
            try:
                # Enforce rate limiting
                self.enforce_rate_limit()
                
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=0.1,
                    max_tokens=500
                )
                
                return response.choices[0].message.content.strip()
                
            except Exception as e:
                logger.warning(f"API call attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    # Exponential backoff with jitter
                    sleep_time = (2 ** attempt) + random.uniform(0, 1)
                    time.sleep(sleep_time)
                else:
                    logger.error(f"All API attempts failed for prompt")
                    return None
        
        return None
    
    def parse_llm_response(self, response: str) -> Optional[Dict]:
        """Parse LLM JSON response.
        
        Args:
            response: Raw LLM response text
            
        Returns:
            Optional[Dict]: Parsed JSON or None if parsing failed
        """
        if not response:
            return None
            
        try:
            # Try to extract JSON from the response
            # Look for JSON block
            import re
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                json_str = json_match.group()
                return json.loads(json_str)
            else:
                # Try parsing the entire response as JSON
                return json.loads(response)
                
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse JSON response: {response[:100]}...")
            return None
    
    def extract_from_description(self, description: str) -> ExtractionResult:
        """
        Extract structured data from a single product description.
        
        This method should be implemented by specialized extractors.
        
        Args:
            description: Product description to extract from
            
        Returns:
            ExtractionResult: Extraction results
        """
        raise NotImplementedError("Subclasses must implement extract_from_description")
    
    def process_category(self, category: str) -> pd.DataFrame:
        """
        Process all records for a specific category.
        
        Args:
            category: Category name to process
            
        Returns:
            pd.DataFrame: DataFrame with extraction results
        """
        logger.info(f"Starting LLM extraction for category: {category}")
        
        # Load processed data
        input_path = self.processed_dir / "inventory_base.parquet"
        if not input_path.exists():
            raise FileNotFoundError(f"Processed data not found at {input_path}")
        
        df = pd.read_parquet(input_path)
        logger.info(f"Loaded {len(df)} total records")
        
        # Filter for category (case insensitive)
        category_df = df[df['category_description'].str.lower() == category.lower()].copy()
        logger.info(f"Found {len(category_df)} records for category '{category}'")
        
        if len(category_df) == 0:
            logger.warning(f"No records found for category '{category}'")
            return pd.DataFrame()
        
        # Process in chunks to manage memory
        chunk_size = 50
        results = []
        
        for i in range(0, len(category_df), chunk_size):
            chunk = category_df.iloc[i:i+chunk_size]
            logger.info(f"Processing chunk {i//chunk_size + 1}/{(len(category_df) + chunk_size - 1)//chunk_size}")
            
            for idx, row in chunk.iterrows():
                description = row['product_description']
                
                # Extract structured data
                extraction_result = self.extract_from_description(description)
                
                # Combine with original row data
                result_dict = {
                    'source_filename': row['source_filename'],
                    'row_number': row['row_number'],
                    'product_code': row['product_code'],
                    'raw_description': description,
                    'category_description': row['category_description'],
                    'species': extraction_result.species,
                    'primal': extraction_result.primal,
                    'subprimal': extraction_result.subprimal,
                    'grade': extraction_result.grade,
                    'size': extraction_result.size,
                    'size_uom': extraction_result.size_uom,
                    'brand': extraction_result.brand,
                    'llm_confidence': extraction_result.llm_confidence,
                    'needs_review': extraction_result.needs_review
                }
                
                results.append(result_dict)
        
        result_df = pd.DataFrame(results)
        logger.info(f"Completed LLM extraction for {len(result_df)} records")
        
        return result_df
