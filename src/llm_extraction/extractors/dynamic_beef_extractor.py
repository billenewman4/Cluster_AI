"""
Dynamic Beef Extractor
Specialized extractor for beef products using dynamic prompt generation.
"""

"""Dynamic Beef Extractor Module

Provides specialized extraction for beef products using optimized prompts per primal cut.
"""

import os
import json
import re
import logging
from typing import Dict, List, Optional, Set, Any
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# Import the base extractor class
from ..base_extractor import BaseLLMExtractor, ExtractionResult
from ..dynamic_prompt_generator import DynamicPromptGenerator
from ...data_ingestion.utils.reference_data_loader import ReferenceDataLoader

# Ensure environment variables are loaded
load_dotenv()

logger = logging.getLogger(__name__)

class DynamicBeefExtractor(BaseLLMExtractor):
    """Dynamic beef extractor using the OpenAI API.
    
    Uses primal cut specific prompts to improve extraction quality.
    """
    
    def __init__(self, reference_data_path: str = "data/incoming/beef_cuts.xlsx", processed_dir: str = "data/processed"):
        """Initialize with reference data for beef products."""
        # Initialize base extractor first
        super().__init__()
        
        # DynamicBeefExtractor specific attributes
        self.processed_dir = processed_dir
        self.current_primal = None
        self.reference_data_path = reference_data_path
        
        # Load reference data
        try:
            self.reference_data = ReferenceDataLoader(reference_data_path)
            self.primals = self.reference_data.get_primals()
            self.subprimal_mapping = {}
            
            # Build mapping of subprimals per primal
            for primal in self.primals:
                subprimals = self.reference_data.get_subprimals(primal)
                variations = {}
                for subprimal in subprimals:
                    terms = self.reference_data.get_subprimal_terms(primal, subprimal)
                    variations[subprimal] = list(terms)
                self.subprimal_mapping[primal] = variations
            
            logger.info(f"Loaded reference data for {len(self.primals)} beef primals")
            
            # Initialize dynamic prompt generator
            self.prompt_generator = DynamicPromptGenerator(self.reference_data)
            
        except Exception as e:
            logger.error(f"Failed to load reference data: {e}")
            # Try to load the reference data from default location
            try:
                # Try multiple possible locations for the reference data
                possible_paths = [
                    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 
                                "data", "incoming", "beef_cuts.xlsx"),
                    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                                "data", "beef_cuts.xlsx"),
                    "./data/incoming/beef_cuts.xlsx",
                    "./data/beef_cuts.xlsx"
                ]
                
                for path in possible_paths:
                    if os.path.exists(path):
                        logger.info(f"Attempting to load reference data from path: {path}")
                        self.reference_data = ReferenceDataLoader(path)
                        self.primals = self.reference_data.get_primals()
                        
                        # Build mapping of subprimals per primal
                        self.subprimal_mapping = {}
                        for primal in self.primals:
                            subprimals = self.reference_data.get_subprimals(primal)
                            variations = {}
                            for subprimal in subprimals:
                                terms = self.reference_data.get_subprimal_terms(primal, subprimal)
                                variations[subprimal] = list(terms)
                            self.subprimal_mapping[primal] = variations
                        
                        # Initialize dynamic prompt generator
                        self.prompt_generator = DynamicPromptGenerator(self.reference_data)
                        logger.info(f"Successfully loaded reference data from path: {path}")
                        return
                        
                raise FileNotFoundError("Could not find beef_cuts.xlsx in any expected location")
                        
            except Exception as inner_e:
                logger.error(f"All attempts to load reference data failed: {inner_e}")
                raise RuntimeError(f"Cannot initialize beef extractor without valid reference data: {inner_e}")
            
    def get_supported_primals(self) -> List[str]:
        """Return list of supported primals."""
        return self.primals
    
    def _get_primal_variations(self, primal: str) -> Dict[str, List[str]]:
        """Get subprimal variations for a specific primal."""
        if primal in self.subprimal_mapping:
            return self.subprimal_mapping[primal]
        return {}
        
    def infer_primal_from_category(self, category: str) -> Optional[str]:
        """Extract the primal cut from a category name.
        
        Args:
            category: The category name (e.g., 'Beef Chuck', 'Beef Rib')
            
        Returns:
            The primal cut name if found, None otherwise
        """
        # Remove 'beef' prefix if present and normalize
        category = category.lower().replace('beef', '').strip()
        
        # Check for exact primal matches first
        for primal in self.primals:
            if primal.lower() == category:
                return primal
                
        # Then check for contained primal names
        for primal in self.primals:
            if primal.lower() in category:
                return primal
                
        # No match found
        logger.warning(f"Could not infer primal from category: {category}")
        return None
    
    def set_primal(self, primal: str) -> bool:
        """Set the current primal cut for extraction.
        
        Args:
            primal: The primal cut name to set
            
        Returns:
            True if the primal was valid and set successfully, False otherwise
        """
        if primal in self.primals:
            self.current_primal = primal
            logger.info(f"Set current primal to: {primal}")
            return True
        else:
            logger.warning(f"Invalid primal cut: {primal}. Must be one of {self.primals}")
            return False
    
    # Implement required abstract methods from BaseLLMExtractor
    def get_subprimal_mapping(self) -> Dict[str, List[str]]:
        """Return mapping of standard subprimal names to their variations."""
        if not self.current_primal:
            logger.warning("No primal set, cannot get subprimal mapping")
            return {}
            
        # Convert reference data structure to the format expected by BaseLLMExtractor
        mapping = {}
        subprimals = self.reference_data.get_subprimals(self.current_primal)
        
        for subprimal in subprimals:
            synonyms = self.reference_data.get_synonyms('subprimal', subprimal, self.current_primal)
            mapping[subprimal] = list(synonyms)
            
        return mapping
    
    def get_category_name(self) -> str:
        """Return the category name (e.g., 'Beef Chuck', 'Beef Rib')."""
        if self.current_primal:
            return f"Beef {self.current_primal}"
        return "Beef"
    
    def extract(self, description: str) -> ExtractionResult:
        """Extract structured data from beef product description.
        
        Uses specialized dynamic prompting based on the current primal cut.
        
        Args:
            description: The product description to extract data from
            
        Returns:
            ExtractionResult: Structured data extracted from the description
        
        Raises:
            ValueError: If extraction fails
        """
        # Input validation 
        logger.info(f"Extracting data from description: '{description[:50]}...'")
        if not description or not str(description).strip():
            logger.warning(f"Empty description provided for extraction")
            return ExtractionResult(
                subprimal=None,
                grade=None,
                size=None,
                size_uom=None,
                brand=None,
                bone_in=False,
                confidence=0.0,
                needs_review=True
            )
        
        # If no current primal is set, try to infer from description
        if not self.current_primal:
            description_lower = description.lower()
            self.current_primal = next(
                (p for p in self.primals if p.lower() in description_lower),
                None
            )
        
        # Initialize result with default values
        result = ExtractionResult(
            subprimal=None,
            grade=None,
            size=None,
            size_uom=None,
            brand=None,
            bone_in=False,
            confidence=0.0,
            needs_review=True
        )
        
        try:
            # Use dynamic prompt generator if primal is known
            if not self.current_primal:
                logger.warning(f"No primal cut identified for description: {description[:50]}")
                
                # Set for review as we can't create a specialized prompt
                result.needs_review = True
                return result
                
            # Generate specialized prompts based on the primal cut
            system_prompt = self.prompt_generator.generate_system_prompt(self.current_primal)
            user_prompt = self.prompt_generator.generate_user_prompt(self.current_primal, description)
            
            logger.info(f"Using specialized prompt for {self.current_primal} primal cut, extracting: '{description[:50]}...'")
            
            # Make API call using prompt generator output
            response = self.api_manager.call_with_retry(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.0
            )
                
            if not response:
                logger.error(f"API call returned None or empty response")
                
                result.needs_review = True
                return result
                
            logger.info(f" \n\n\n\n\n\n API call successful, response {response} \n\n\n\n\n\n")
            
            # Parse JSON response
            try:
                # First attempt direct parsing
                extraction_data = json.loads(response)
                logger.info(f"Successfully parsed JSON response")

            except json.JSONDecodeError as json_error:
                logger.error(f"Failed to parse API response as JSON: {json_error}")
                logger.info(f"Raw API response: {response}")
                
                # Attempt to extract JSON from text response
                try:
                    # Look for JSON-like content between braces
                    json_match = re.search(r'\{.*\}', response, re.DOTALL)
                    if json_match:
                        potential_json = json_match.group(0)
                        extraction_data = json.loads(potential_json)
                        logger.info(f"Successfully parsed JSON from text response")
                    else:
                        logger.error("No JSON-like content found in response")
                        result.needs_review = True
                        return result
                except Exception as e:
                    logger.error(f"Failed to extract valid JSON from text: {e}")
                    result.needs_review = True
                    return result
                
            # Log the extracted data
            logger.info(f"Extracted data: {extraction_data}")
                
            # Update result with extracted data
            result.species = extraction_data.get("species", "Beef")
            
            # Use the base class methods to validate and score the extraction
            # First, populate an ExtractionResult from the JSON data
            raw_result = {
                'subprimal': extraction_data.get('subprimal'),
                'grade': extraction_data.get('grade'),
                'size': extraction_data.get('size'),
                'size_uom': extraction_data.get('size_uom'),
                'brand': extraction_data.get('brand'),
                'bone_in': extraction_data.get('bone_in', False)
            }
            
            # Use the base class's validation and scoring logic
            validated_result = self.validate_and_score(raw_result, description)
            
            # Note: We can't add beef-specific fields to ExtractionResult as it doesn't have species/primal fields
            # The calling code in run_pipeline.py handles species/primal mapping from category
            
            # Return the validated result
            return validated_result
            
        except Exception as e:
            logger.error(f"Extraction failed with error: {str(e)}")
            
            # If anything goes wrong, return a result flagged for review
            result.needs_review = True
            return result
    
    def validate_and_score(self, raw_result: Dict, description: str) -> ExtractionResult:
        """Override the base validation method to use beef-specific reference data properly."""
        result = ExtractionResult()
            
        # Extract fields
        result.subprimal = raw_result.get('subprimal')
        result.grade = raw_result.get('grade') 
        result.size = raw_result.get('size')
        result.size_uom = raw_result.get('size_uom')
        result.brand = raw_result.get('brand')
        result.bone_in = raw_result.get('bone_in', False)
        
        # Validation and confidence scoring
        confidence_score = 0.5  # Base confidence
        
        # Validate subprimal using our available reference data methods
        if result.subprimal and self.current_primal:
            subprimal_mapping = self.get_subprimal_mapping()
                
            # Direct match in the mapping keys
            found_match = False
            for standard_subprimal, synonyms in subprimal_mapping.items():
                # Check exact match
                if result.subprimal.lower() == standard_subprimal.lower():
                    result.subprimal = standard_subprimal  # Normalize capitalization
                    confidence_score += 0.3
                    found_match = True
                    break
                # Check synonyms
                if result.subprimal.lower() in [syn.lower() for syn in synonyms]:
                    result.subprimal = standard_subprimal  # Map to standard name
                    confidence_score += 0.2
                    found_match = True
                    break
                # Partial match (if input is contained in standard or vice versa)
                if result.subprimal.lower() in standard_subprimal.lower() or standard_subprimal.lower() in result.subprimal.lower():
                    result.subprimal = standard_subprimal
                    confidence_score += 0.1
                    found_match = True
                    break
                        
            if not found_match:
                logger.warning(f"Unknown subprimal '{result.subprimal}' for {self.current_primal}")
                result.needs_review = True
        
        # Validate grade using our available reference data methods
        if result.grade:
            # Get all grade terms (official names and synonyms)
            grade_terms = self.reference_data.get_all_grade_terms()
                
            # Check if grade matches any standard grade or synonym
            found_match = False
            for standard_grade, synonyms in self.reference_data.grade_mappings.items():
                # Check direct match with standard grade
                if result.grade.lower() == standard_grade.lower():
                    result.grade = standard_grade  # Normalize capitalization
                    confidence_score += 0.2
                    found_match = True
                    break
                # Check matches with synonyms
                if result.grade.lower() in [syn.lower() for syn in synonyms]:
                    result.grade = standard_grade  # Map to standard name
                    confidence_score += 0.1
                    found_match = True
                    break
                        
            if not found_match:
                logger.warning(f"Unknown grade: {result.grade}")
                result.needs_review = True
                
        # Validate size unit
        if result.size_uom and result.size_uom.lower() in [unit.lower() for unit in self.VALID_SIZE_UNITS]:
            confidence_score += 0.05
        elif result.size_uom:
            result.needs_review = True
            logger.warning(f"Unknown size unit: {result.size_uom}")
            
        # Check if we found any specific information
        if result.subprimal or result.grade or result.size:
            confidence_score += 0.05
            
        result.confidence = min(confidence_score, 1.0)
            
        # Flag for review if confidence is low
        if result.confidence < 0.6:
            result.needs_review = True
                
        return result
        
    def extract_batch(self, descriptions: List[str], primal: Optional[str] = None) -> List[ExtractionResult]:
        """Extract data from a batch of descriptions."""
        results = []
        
        # Set primal once for all extractions in batch if provided
        original_primal = self.current_primal
        if primal:
            self.current_primal = primal
        
        for description in descriptions:
            result = self.extract(description)
            results.append(result)
            
        # Reset primal to original value after batch processing
        self.current_primal = original_primal
        
        return results
    

