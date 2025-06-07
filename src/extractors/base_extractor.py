"""
Unified Base Extractor Module
Provides the foundation for all extractors with enhanced functionality and performance.
"""

import os
import json
import hashlib
import logging
import time
from pathlib import Path
from typing import Dict, Optional, List, Any, Union
from abc import ABC, abstractmethod

import pandas as pd
from openai import OpenAI
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class ExtractionResult:
    """Unified result structure for all extraction types."""
    
    def __init__(self,
                 subprimal: Optional[str] = None,
                 grade: Optional[str] = None, 
                 size: Optional[float] = None,
                 size_uom: Optional[str] = None,
                 brand: Optional[str] = None,
                 bone_in: bool = False,
                 confidence: float = 0.0,
                 needs_review: bool = False,
                 **extra_attrs):
        """
        Initialize extraction result with common attributes.
        
        Args:
            subprimal: The subprimal cut name
            grade: The meat grade (Prime, Choice, etc.)
            size: The size/weight value
            size_uom: The unit of measure for size/weight
            brand: The brand name if identified
            bone_in: Whether the cut is bone-in or boneless
            confidence: Confidence score (0-1) for the extraction
            needs_review: Flag for results that need human verification
            extra_attrs: Any additional attributes specific to an extractor
        """
        self.subprimal = subprimal
        self.grade = grade
        self.size = size
        self.size_uom = size_uom
        self.brand = brand
        self.bone_in = bone_in
        self.confidence = confidence
        self.needs_review = needs_review
        
        # Add any additional attributes
        for key, value in extra_attrs.items():
            setattr(self, key, value)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary for serialization."""
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ExtractionResult':
        """Create result from dictionary."""
        return cls(**data)


class BaseExtractor(ABC):
    """Unified base class for all attribute extractors.
    
    Provides common functionality like caching, API management, and
    result handling shared by all specialized extractors.
    """
    
    # Common valid attributes across all meat types
    VALID_GRADES = {
        'prime', 'choice', 'select', 'utility', 'wagyu', 'angus', 'certified angus', 
        'creekstone angus', 'no grade'
    }
    
    VALID_SIZE_UNITS = {'oz', 'lb', '#', 'g', 'kg', 'in', 'inch', 'inches'}
    
    def __init__(self, processed_dir: str = "data/processed"):
        """
        Initialize the base extractor.
        
        Args:
            processed_dir: Directory for storing processed data and cache
        """
        self.processed_dir = processed_dir
        self.cache_file = os.path.join(processed_dir, ".extraction_cache.json")
        self.cache = self._load_cache()
        
        # Initialize OpenAI client
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        
        # Rate limiting settings
        self.requests_per_minute = 100
        self.request_interval = 60.0 / self.requests_per_minute
        self.last_request_time = 0.0
    
    def _load_cache(self) -> Dict[str, Dict]:
        """Load extraction cache to avoid re-processing same descriptions."""
        try:
            cache_path = Path(self.cache_file)
            if cache_path.exists():
                with open(cache_path, "r") as f:
                    return json.load(f)
            else:
                # Ensure the processed directory exists
                os.makedirs(os.path.dirname(cache_path), exist_ok=True)
                return {}
        except Exception as e:
            logger.warning(f"Failed to load cache: {e}")
            return {}
    
    def _save_cache(self) -> None:
        """Save extraction cache to disk."""
        try:
            with open(self.cache_file, "w") as f:
                json.dump(self.cache, f)
        except Exception as e:
            logger.warning(f"Failed to save cache: {e}")
    
    def _get_cache_key(self, description: str, **context) -> str:
        """Generate cache key for a description with optional context."""
        # Include context in hash to differentiate between extractions
        # with the same description but different context (e.g., primal cut)
        context_str = json.dumps(context, sort_keys=True) if context else ""
        hash_input = f"{description}{context_str}"
        return hashlib.md5(hash_input.encode()).hexdigest()
    
    def _apply_rate_limit(self) -> None:
        """Apply rate limiting to avoid hitting API limits."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.request_interval:
            sleep_time = self.request_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    @abstractmethod
    def get_category_name(self) -> str:
        """Return the category name (e.g., 'Beef Chuck', 'Beef Rib')."""
        pass
    
    @abstractmethod
    def extract(self, description: str, **context) -> ExtractionResult:
        """
        Extract attributes from product description.
        
        Args:
            description: Product description text
            context: Optional contextual information for extraction
        
        Returns:
            ExtractionResult containing extracted attributes
        """
        pass
    
    def extract_with_cache(self, description: str, **context) -> ExtractionResult:
        """Extract with caching for efficiency."""
        if not description:
            return ExtractionResult(needs_review=True, confidence=0.0)
        
        # Generate cache key including context
        cache_key = self._get_cache_key(description, **context)
        
        # Check cache first - O(1) lookup
        if cache_key in self.cache:
            return ExtractionResult.from_dict(self.cache[cache_key])
        
        # Apply rate limiting before making API call
        self._apply_rate_limit()
        
        # Perform extraction
        result = self.extract(description, **context)
        
        # Cache result
        self.cache[cache_key] = result.to_dict()
        self._save_cache()
        
        return result
