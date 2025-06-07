"""
Unified Beef Extractor
A highly optimized extractor capable of handling any beef primal cut dynamically.
Works with all categories in the beef_cuts.xlsx reference data.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional, Set

import pandas as pd

from .base_extractor import BaseExtractor, ExtractionResult
from ..data_ingestion.utils.reference_data_loader import ReferenceDataLoader

# Configure logging
logger = logging.getLogger(__name__)

class BeefExtractor(BaseExtractor):
    """
    Optimized beef extractor that handles any primal cut dynamically.
    
    Key performance optimizations:
    - O(1) lookup for primal cuts via dictionary-based mapping
    - Efficient caching to avoid redundant LLM calls
    - Pre-loaded reference data for all beef primal cuts
    - Dynamic prompt generation based on specific beef category
    """
    
    def __init__(self, 
                 reference_data_path: str = "data/incoming/beef_cuts.xlsx", 
                 processed_dir: str = "data/processed"):
        """
        Initialize with reference data for all beef primal cuts.
        
        Args:
            reference_data_path: Path to beef_cuts.xlsx reference data
            processed_dir: Directory for storing processed data and cache
        """
        super().__init__(processed_dir)
        
        # Load reference data with all beef categories
        self.reference_data = ReferenceDataLoader(reference_data_path)
        logger.info(f"Loaded reference data for {len(self.reference_data.get_primals())} primal cuts")
        
        # Store available primals for O(1) lookups
        self.supported_primals = self.reference_data.get_primals()
        
        # Current active primal for extractions (can be changed dynamically)
        self._active_primal = None
    
    def set_primal(self, primal: str) -> bool:
        """
        Set the active primal cut for subsequent extractions.
        
        Args:
            primal: The beef primal cut name (e.g., 'Chuck', 'Rib')
            
        Returns:
            bool: True if primal is valid and set, False otherwise
        """
        # Case insensitive matching for better UX
        primal = primal.strip().title()
        
        # O(1) lookup to check if primal is supported
        if primal in self.supported_primals:
            self._active_primal = primal
            return True
        
        logger.warning(f"Primal cut '{primal}' not found in reference data")
        return False
    
    def get_active_primal(self) -> Optional[str]:
        """Get the currently active primal cut."""
        return self._active_primal
    
    def get_supported_primals(self) -> List[str]:
        """Get list of all supported primal cuts from reference data."""
        return list(self.supported_primals)
    
    def get_subprimals_for_primal(self, primal: str) -> List[str]:
        """Get all subprimal cuts for a given primal."""
        return self.reference_data.get_subprimals(primal)
    
    def infer_primal_from_category(self, category: str) -> Optional[str]:
        """
        Infer the beef primal cut from a category name.
        
        Args:
            category: Category name (e.g., 'Beef Chuck', 'Beef Rib')
            
        Returns:
            str: Primal cut name if found, None otherwise
        """
        category = category.lower()
        
        # Handle 'beef' prefix
        if category.startswith('beef '):
            primal_candidate = category[5:].strip().title()
            if primal_candidate in self.supported_primals:
                return primal_candidate
        
        # Try without 'beef' prefix
        primal_candidate = category.strip().title()
        if primal_candidate in self.supported_primals:
            return primal_candidate
        
        return None
    
    def get_category_name(self) -> str:
        """Return the full category name based on active primal cut."""
        if not self._active_primal:
            return "Beef (Generic)"
        return f"Beef {self._active_primal}"
    
    def generate_prompt(self, description: str, primal: Optional[str] = None) -> str:
        """
        Generate a specialized extraction prompt for a beef description.
        
        Args:
            description: Product description text
            primal: Optional override for the active primal cut
            
        Returns:
            str: Optimized extraction prompt
        """
        # Use provided primal or fall back to active primal
        primal_cut = primal or self._active_primal
        
        if not primal_cut:
            raise ValueError("No primal cut specified for prompt generation")
        
        # Get reference data for this primal
        subprimals = self.reference_data.get_subprimals(primal_cut)
        subprimals_str = ", ".join(subprimals)
        
        # Create specialized prompt based on the specific primal cut
        prompt = f"""
        Extract beef attributes from this product description: "{description}"
        
        Product is from the Beef {primal_cut} primal cut.
        
        Known subprimal cuts for {primal_cut} include: {subprimals_str}
        
        Extract these attributes:
        1. Subprimal cut (from the list above or similar)
        2. Grade (e.g., Prime, Choice, Select, No Grade)
        3. Weight/Size (with unit, e.g., 10 lb)
        4. Bone-in status (Yes/No)
        5. Brand (if mentioned)
        
        Format response as JSON with fields: {{
            "subprimal": string or null,
            "grade": string or null,
            "weight": number or null,
            "unit": string or null,
            "bone_in": boolean,
            "brand": string or null,
            "confidence": number (0-1),
            "needs_review": boolean
        }}
        
        Set needs_review to true if you're unsure about any attribution.
        """
        
        return prompt.strip()
    
    def extract(self, description: str, primal: Optional[str] = None, **context) -> ExtractionResult:
        """
        Extract beef attributes from product description.
        
        Args:
            description: Product description text
            primal: Optional override for the active primal cut
            context: Additional extraction context
            
        Returns:
            ExtractionResult containing extracted beef attributes
        """
        # Validate input - handle None or empty description
        if description is None or not str(description).strip():
            logger.warning("Received null or empty product description - cannot extract attributes")
            return ExtractionResult(
                data={
                    "subprimal": None,
                    "grade": None,
                    "weight": None,
                    "unit": None,
                    "bone_in": False,
                    "brand": None,
                    "confidence": 0.0,
                    "needs_review": True
                },
                needs_review=True,
                confidence=0.0
            )
            
        # Use provided primal or fall back to active primal
        primal_cut = primal or self._active_primal
        
        if not primal_cut:
            raise ValueError("No primal cut specified for extraction")
        
        # Generate optimized prompt based on primal
        prompt = self.generate_prompt(str(description), primal_cut)
        
        try:
            # Make LLM API call with proper error handling
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            
            # Extract and parse JSON response
            content = response.choices[0].message.content.strip()
            try:
                data = json.loads(content)
            except json.JSONDecodeError:
                logger.error(f"Failed to parse LLM response as JSON: {content}")
                return ExtractionResult(needs_review=True, confidence=0.0)
            
            # Create result from parsed data with validation
            result = ExtractionResult(
                subprimal=data.get('subprimal'),
                grade=data.get('grade'),
                size=data.get('weight'),
                size_uom=data.get('unit'),
                brand=data.get('brand'),
                bone_in=data.get('bone_in', False),
                confidence=data.get('confidence', 0.8),
                needs_review=data.get('needs_review', False),
                primal_cut=primal_cut  # Store the primal cut used for extraction
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            return ExtractionResult(needs_review=True, confidence=0.0)
    
    def batch_extract(self, descriptions: List[str], primal: Optional[str] = None) -> List[ExtractionResult]:
        """
        Process a batch of descriptions with the same primal cut.
        
        Args:
            descriptions: List of product descriptions
            primal: Optional override for the active primal cut
            
        Returns:
            List of ExtractionResults for each description
        """
        results = []
        
        # Set active primal temporarily if provided
        original_primal = self._active_primal
        if primal and primal != self._active_primal:
            self.set_primal(primal)
        
        try:
            for desc in descriptions:
                result = self.extract_with_cache(desc)
                results.append(result)
        finally:
            # Restore original primal if it was changed
            if primal and original_primal != primal:
                self._active_primal = original_primal
        
        return results
