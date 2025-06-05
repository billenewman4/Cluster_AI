"""
Dynamic Beef Extractor Module

Provides a flexible extractor that can handle any primal beef cut using dynamic prompts.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional, Union

import pandas as pd

from ..base_extractor import BaseExtractor
from ..models import ExtractionResult
from ..prompts.dynamic_prompt_generator import DynamicPromptGenerator
from ...data_ingestion.utils.reference_data_loader import ReferenceDataLoader

# Configure logging
logger = logging.getLogger(__name__)

class DynamicBeefExtractor(BaseExtractor):
    """
    Dynamic extractor for beef cuts that works with any primal cut.
    
    Uses reference data from beef_cuts.xlsx to generate appropriate prompts
    and extraction logic for any primal cut.
    """
    
    def __init__(self, 
                 reference_data_path: str = "data/incoming/beef_cuts.xlsx",
                 processed_dir: str = "data/processed"):
        """
        Initialize the dynamic beef extractor.
        
        Args:
            reference_data_path: Path to the beef cuts reference Excel file
            processed_dir: Directory containing processed data files
        """
        super().__init__(processed_dir)
        
        # Load reference data
        self.reference_data = ReferenceDataLoader(reference_data_path)
        
        # Create prompt generator
        self.prompt_generator = DynamicPromptGenerator(self.reference_data)
        
        # Keep track of all supported primals
        self.supported_primals = self.reference_data.get_primals()
        
        logger.info(f"Initialized dynamic beef extractor with {len(self.supported_primals)} primal cuts")
    
    def setup_reference_data(self) -> None:
        """
        Set up reference data for extraction.
        This is called by the BaseExtractor during initialization.
        """
        # Reference data is loaded in __init__, so nothing to do here
        pass
    
    def extract(self, 
               description: str, 
               primal: Optional[str] = None, 
               **kwargs) -> ExtractionResult:
        """
        Extract structured information from a product description.
        
        Args:
            description: Product description text
            primal: Optional primal cut to use as context. If not provided, will be inferred.
            **kwargs: Additional extraction parameters
            
        Returns:
            ExtractionResult with extracted information
        """
        # Calculate cache key for this extraction
        cache_key = self._generate_cache_key(description, primal)
        
        # Check cache first
        if cache_key in self.cache:
            logger.debug(f"Cache hit for: {description}")
            return self.cache[cache_key]
        
        # Determine primal cut if not provided
        if not primal:
            # Try to infer primal from description
            primal = self._infer_primal_cut(description)
            if not primal:
                logger.warning(f"Could not determine primal cut for: {description}")
                # Default to a generic approach if we can't determine the primal
                primal = "Generic"
        
        # Generate appropriate prompts
        system_prompt = self.prompt_generator.generate_system_prompt(primal)
        user_prompt = self.prompt_generator.generate_user_prompt(primal, description)
        
        # Get post-processing rules
        rules = self.prompt_generator.get_post_processing_rules(primal)
        
        # Make API call
        try:
            # Enforce rate limits
            self._enforce_rate_limit()
            
            # Make the API call
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,
                max_tokens=300
            )
            
            # Parse the response
            content = response.choices[0].message.content.strip()
            json_start = content.find('{')
            json_end = content.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_str = content[json_start:json_end]
                try:
                    # Parse as JSON
                    result = json.loads(json_str)
                    
                    # Apply post-processing
                    result = self._post_process_result(result, description, rules)
                    
                    # Create and cache extraction result
                    extraction_result = ExtractionResult(
                        description=description,
                        extracted_data=result,
                        primal=primal,
                        successful=True,
                        error=None
                    )
                    
                    # Cache the result
                    self.cache[cache_key] = extraction_result
                    
                    return extraction_result
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON response: {e}")
                    logger.debug(f"Response content: {content}")
                    
                    return ExtractionResult(
                        description=description,
                        extracted_data={},
                        primal=primal,
                        successful=False,
                        error=f"JSON parse error: {str(e)}"
                    )
            else:
                logger.error("No JSON found in response")
                logger.debug(f"Response content: {content}")
                
                return ExtractionResult(
                    description=description,
                    extracted_data={},
                    primal=primal,
                    successful=False,
                    error="No JSON found in response"
                )
                
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            
            return ExtractionResult(
                description=description,
                extracted_data={},
                primal=primal,
                successful=False,
                error=str(e)
            )
    
    def _infer_primal_cut(self, description: str) -> Optional[str]:
        """
        Infer the primal cut from a product description.
        
        Args:
            description: Product description text
            
        Returns:
            Inferred primal cut name or None
        """
        # Normalize description for matching
        desc_lower = description.lower()
        
        # Check for each primal in the description
        for primal in self.supported_primals:
            primal_lower = primal.lower()
            # Check if primal name appears in description
            if primal_lower in desc_lower:
                return primal
        
        # If no match found, return None
        return None
    
    def _post_process_result(self, 
                           result: Dict[str, Any], 
                           description: str,
                           rules: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply post-processing rules to the extracted result.
        
        Args:
            result: Extracted JSON result
            description: Original description
            rules: Post-processing rules
            
        Returns:
            Post-processed result
        """
        desc_lower = description.lower()
        
        # Process grade if needed
        if not result.get('grade') or result.get('grade') == "null":
            # Try to extract grade from description
            for pattern, grade in rules.get('grade_regex_patterns', []):
                if re.search(pattern, desc_lower):
                    result['grade'] = grade
                    break
        
        # Process size and size_uom if needed
        if (not result.get('size') or result.get('size') == "null") and rules.get('size_regex_pattern'):
            # Try to extract size from description
            size_match = re.search(rules['size_regex_pattern'], description)
            if size_match:
                result['size'] = float(size_match.group(1))
                result['size_uom'] = size_match.group(2)
        
        return result
    
    def _generate_cache_key(self, description: str, primal: Optional[str] = None) -> str:
        """
        Generate a unique cache key for a description and primal.
        
        Args:
            description: Product description
            primal: Primal cut name (if known)
            
        Returns:
            Cache key string
        """
        key_parts = [description]
        if primal:
            key_parts.append(primal)
            
        return "_".join(key_parts)
    
    def extract_batch(self, 
                    descriptions: List[str], 
                    primal: Optional[str] = None,
                    **kwargs) -> List[ExtractionResult]:
        """
        Extract information from multiple descriptions.
        
        Args:
            descriptions: List of product descriptions
            primal: Optional primal cut to use for all descriptions
            **kwargs: Additional extraction parameters
            
        Returns:
            List of ExtractionResult objects
        """
        results = []
        
        for description in descriptions:
            result = self.extract(description, primal, **kwargs)
            results.append(result)
            
        return results
        
    def get_supported_primals(self) -> List[str]:
        """
        Get list of supported primal cuts.
        
        Returns:
            List of primal cut names
        """
        return self.supported_primals
