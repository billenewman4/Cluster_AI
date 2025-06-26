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
from ..base_extractor import BaseLLMExtractor
from ..dynamic_prompt_generator import DynamicPromptGenerator
from ...data_ingestion.utils.reference_data_loader import ReferenceDataLoader
from ...models.product_model import ProductData

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
    
    def get_subprimal_mapping(self) -> Dict[str, List[str]]:
        """Return mapping of standard subprimal names to their variations."""
        if not self.current_primal:
            # Default to Chuck if no primal is set
            return self._get_primal_variations("Chuck")
        return self._get_primal_variations(self.current_primal)
    
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
    
    def get_category_name(self) -> str:
        """Return the category name (e.g., 'Beef Chuck', 'Beef Rib')."""
        if self.current_primal:
            return f"Beef {self.current_primal}"
        return "Beef"
        
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
    
    def extract(self, product: ProductData) -> ProductData:
        """Extract structured data from beef product description and update the ProductData object.
        
        Uses specialized dynamic prompting based on the current primal cut.
        
        Args:
            product: ProductData object with at least product_description populated
            
        Returns:
            ProductData: The same ProductData object with extracted attributes populated
        
        Raises:
            ValueError: If extraction fails
        """
        # Input validation
        description = product.productdescription
        logger.info(f"Extracting data from description: '{description[:50] if description else ''}...'")
        if not description or not str(description).strip():
            logger.warning(f"Empty description provided for extraction")
            product.needs_review = True
            product.confidence = 0.0
            return product
        
        # If no current primal is set, try to infer from description
        if not self.current_primal:
            description_lower = description.lower()
            self.current_primal = next(
                (p for p in self.primals if p.lower() in description_lower),
                None
            )
        
        # Initialize product with default values if not already set
        product.confidence = product.confidence or 0.0
        product.needs_review = product.needs_review or True
        
        # Set product species to Beef
        product.species = product.species or 'Beef'
        
        try:
            # Use dynamic prompt generator if primal is known
            if not self.current_primal:
                logger.warning(f"No primal cut identified for description: {description[:50]}")
                
                # Set for review as we can't create a specialized prompt
                product.needs_review = True
                return product
                
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
                
                product.needs_review = True
                return product
                
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
                        product.needs_review = True
                        return product
                except Exception as e:
                    logger.error(f"Failed to extract valid JSON from text: {e}")
                    product.needs_review = True
                    return product
                
            # Log the extracted data
            logger.info(f"Extracted data: {extraction_data}")
                
            # Update product with extracted data
            product.species = extraction_data.get("species", "Beef")
            
            # Use the base class methods to validate and score the extraction
            # Create a raw result dictionary from the JSON data
            raw_result = {
                'subprimal': extraction_data.get('subprimal'),
                'grade': extraction_data.get('grade'),
                'size': extraction_data.get('size'),
                'size_uom': extraction_data.get('size_uom'),
                'brand': extraction_data.get('brand'),
                'bone_in': extraction_data.get('bone_in', False)
            }
            
            # Use the base class's validation and scoring logic
            # This updates the passed product object in place
            validated_product = self.validate_and_score(raw_result, product)
            
            # Add beef-specific data to the ProductData object
            if self.current_primal:
                product.primal = self.current_primal
            
            # Return the validated product
            return validated_product
            
        except Exception as e:
            logger.error(f"Extraction failed with error: {str(e)}")
            import traceback
            logger.debug(f"Traceback: {traceback.format_exc()}")
            
            # Ensure product is flagged for human review
            product.confidence = 0.0
            product.needs_review = True
            return product
        
    def validate_and_score(self, raw_result: Dict, product: ProductData) -> ProductData:
        """Override the base validation method to use beef-specific reference data properly.
        
        Args:
            raw_result: Dictionary containing the extracted fields
            product: ProductData object to update with validated fields
            
        Returns:
            The same ProductData object with validated fields and confidence score
        """
        # Extract fields and update the ProductData object
        product.subprimal = raw_result.get('subprimal')
        product.grade = raw_result.get('grade') 
        product.size = raw_result.get('size')
        product.size_uom = raw_result.get('size_uom')
        product.brand = raw_result.get('brand')
        product.bone_in = raw_result.get('bone_in', False)
        
        # Validation and confidence scoring
        confidence_score = 0.5  # Base confidence
        
        # Validate subprimal using our available reference data methods
        if product.subprimal and self.current_primal:
            subprimal_mapping = self.get_subprimal_mapping()
                
            # Direct match in the mapping keys
            found_match = False
            for standard_subprimal, synonyms in subprimal_mapping.items():
                # Check exact match
                if product.subprimal.lower() == standard_subprimal.lower():
                    product.subprimal = standard_subprimal  # Normalize capitalization
                    confidence_score += 0.3
                    found_match = True
                    break
                # Check synonyms
                if product.subprimal.lower() in [syn.lower() for syn in synonyms]:
                    product.subprimal = standard_subprimal  # Map to standard name
                    confidence_score += 0.2
                    found_match = True
                    break
                # Partial match (if input is contained in standard or vice versa)
                if product.subprimal.lower() in standard_subprimal.lower() or standard_subprimal.lower() in product.subprimal.lower():
                    product.subprimal = standard_subprimal
                    confidence_score += 0.1
                    found_match = True
                    break
                        
            if not found_match:
                logger.warning(f"Unknown subprimal '{product.subprimal}' for {self.current_primal}")
                product.needs_review = True
        
        # Validate grade using our available reference data methods
        if product.grade:
            # Get all grade terms (official names and synonyms)
            grade_terms = self.reference_data.get_all_grade_terms()
                
            # Check if grade matches any standard grade or synonym
            found_match = False
            for standard_grade, synonyms in self.reference_data.grade_mappings.items():
                # Check direct match with standard grade
                if product.grade.lower() == standard_grade.lower():
                    product.grade = standard_grade  # Normalize capitalization
                    confidence_score += 0.2
                    found_match = True
                    break
                # Check matches with synonyms
                if product.grade.lower() in [syn.lower() for syn in synonyms]:
                    product.grade = standard_grade  # Map to standard name
                    confidence_score += 0.1
                    found_match = True
                    break
                        
            if not found_match:
                logger.warning(f"Unknown grade: {product.grade}")
                product.needs_review = True
                
        # Validate size unit
        if product.size_uom and product.size_uom.lower() in [unit.lower() for unit in self.VALID_SIZE_UNITS]:
            confidence_score += 0.05
        elif product.size_uom:
            product.needs_review = True
            logger.warning(f"Unknown size unit: {product.size_uom}")
            
        # Check if we found any specific information
        if product.subprimal or product.grade or product.size:
            confidence_score += 0.05
            
        product.confidence = min(confidence_score, 1.0)
            
        # Flag for review if confidence is low
        if product.confidence < 0.6:
            product.needs_review = True
                
        return product
        
    def extract_batch(self, products: List[ProductData], primal: Optional[str] = None) -> List[ProductData]:
        """Extract data from a batch of ProductData objects.
        
        Args:
            products: List of ProductData objects to process
            primal: Optional primal cut to use for all extractions in the batch
            
        Returns:
            The same list of ProductData objects with extracted fields populated
        """
        # Set primal once for all extractions in batch if provided
        original_primal = self.current_primal
        if primal:
            self.current_primal = primal
        
        processed_products = []
        for product in products:
            # Only reset primal between descriptions if no batch-level primal was set
            if not primal:
                self.current_primal = original_primal
                
            updated_product = self.extract(product)
            processed_products.append(updated_product)
            
        # Reset primal to original value after batch processing
        self.current_primal = original_primal
        
        return processed_products
