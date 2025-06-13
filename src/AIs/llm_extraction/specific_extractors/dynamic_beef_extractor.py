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
from .dynamic_prompt_generator import DynamicPromptGenerator

# Import reference data loader with absolute import to avoid relative import issues
try:
    from data_ingestion.utils.reference_data_loader import ReferenceDataLoader
except ImportError:
    # Fallback for different import contexts
    from src.data_ingestion.utils.reference_data_loader import ReferenceDataLoader

from src.AIs.utils import APIManager, ResultParser

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
        logger.debug(f"Extracting from: '{description[:50]}...'")
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
            expanded_description = self.expand_abbreviations(description)
            user_prompt = self.prompt_generator.generate_user_prompt(self.current_primal, expanded_description)
            
            logger.info(f"Using specialized prompt for {self.current_primal} primal cut, extracting: '{description[:50]}...'")
            
            # Make API call using prompt generator output
            response = self.api_manager.call_with_retry(
                system_prompt=system_prompt,
                user_prompt=user_prompt
            )
                
            if not response:
                logger.error(f"API call returned None or empty response")
                
                result.needs_review = True
                return result
                
            logger.debug(f"API call successful for {self.current_primal} extraction")
            
            # Parse JSON response
            extraction_data = ResultParser.parse_json_response(response)
            
            if extraction_data:
                logger.debug(f"Successfully parsed extraction response")
                logger.debug(f"Extracted data: {extraction_data}")
            else:
                logger.error(f"Failed to parse JSON response from API")
                result.needs_review = True
                return result
            
            # Update result with extracted data
            result.species = extraction_data.get("species", "Beef")
            
            # First, populate an ExtractionResult from the JSON data
            raw_result = {
                'subprimal': extraction_data.get('subprimal'),
                'grade': extraction_data.get('grade'),
                'size': extraction_data.get('size'),
                'size_uom': extraction_data.get('size_uom'),
                'brand': extraction_data.get('brand'),
                'bone_in': extraction_data.get('bone_in', False),
                'confidence': extraction_data.get('confidence', 0.0),
                'needs_review': extraction_data.get('needs_review', True)
            }
            
            # Use the base class's validation and scoring logic
            validated_result = self.validate_and_score(raw_result, description)
            
            # Standardize subprimal to canonical name
            validated_result = self.standardize_to_canonical(validated_result)
            
            # Note: We can't add beef-specific fields to ExtractionResult as it doesn't have species/primal fields
            # The calling code in run_pipeline.py handles species/primal mapping from category
            
            # Return the validated result
            return validated_result
            
        except Exception as e:
            logger.error(f"Extraction failed with error: {str(e)}")
            
            # If anything goes wrong, return a result flagged for review
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
    
    def expand_abbreviations(self, text: str) -> str:
        """
        Expands common food-related abbreviations in the given text to their full descriptions,
        with special handling for meat industry terminology.
        
        Args:
            text (str): The text containing potential abbreviations.
            
        Returns:
            str: The text with abbreviations expanded to their full descriptions.
        """
        if not text or not isinstance(text, str):
            return text
        
        # Lowercase for better matching    
        result = text.lower()
        
        # Log original text for debugging
        logging.debug(f"Expanding abbreviations in: {text}")
        
        # Get abbreviation mapping
        abbrev_map = self.get_abbreviation_map()
        
        # Sort abbreviations by length (longest first) to prevent partial matches
        # For example, "Bone in" should be processed before "Bone"
        sorted_abbrevs = sorted(abbrev_map.keys(), key=len, reverse=True)
        
        # Special handling for multi-part meat industry terms that may be separated by punctuation
        # Example: "tip on" vs "tip-on" vs "tipon"
        meat_terms = [
            (r'tip[\-\s]*on', 'lip-on'),
            (r'lip[\-\s]*on', 'lip-on'),
            (r'bone[\-\s]*in', 'bone-in'),
            (r'rib[\-\s]*eye', 'ribeye'),
            (r'roll[\-\s]*off', 'roll off'),
            (r'neck[\-\s]*off', 'neck off'),
            (r'deckle[\-\s]*off', 'deckle off'),
            (r'flat[\-\s/]*nose[\-\s]*off', 'flat nose off'),
            (r'heavy[\-\s]*weight', 'heavy weight'),
            (r'(outside|outer)[\-\s]*skirt', 'outside skirt'),
            (r'chuck[\-\s]*roll', 'chuck roll'),
            (r'chuck[\-\s]*clod', 'shoulder clod'),
            (r'shoulder[\-\s]*clod', 'shoulder clod'),
            (r'clod[\-\s]*xt', 'shoulder clod'),
            (r'chuck[\-\s]*flat', 'chuck flat'),
            (r'brisket[\-\s]*flat', 'brisket flat'),
            (r'brisket[\-\s]*at[\-\s]*code', 'brisket'),
            (r'brisket[\-\s]*deckle[\-\s]*off', 'brisket deckle off'),
            (r'teres[\-\s]*major', 'teres major'),
            (r'usda[\-\s]*(\d+[a-z]*)', r'usda \1')
        ]
        
        # Apply meat-specific patterns first
        for pattern, replacement in meat_terms:
            result = re.sub(r'\b' + pattern + r'\b', replacement, result, flags=re.IGNORECASE)
        
        # First pass: Handle measurement abbreviations that often appear within terms (no word boundaries)
        measurement_abbrevs = ['oz', '#', 'lb', 'lbs', 'gal', 'qt', 'pt', 'ea', 'ct', 'cs', 'dz', 'pcs', 'pc']
        for abbrev in sorted_abbrevs:
            if abbrev in measurement_abbrevs:
                # For measurements, also match when they're attached to numbers (e.g., "10oz")
                # Use lookahead to ensure we don't replace within other words
                pattern = r'(?i)(\d+)' + re.escape(abbrev) + r'(?![a-zA-Z])'
                replacement = r'\1 ' + abbrev_map[abbrev]
                result = re.sub(pattern, replacement, result)
        
        # Second pass: Handle all other abbreviations using word boundaries
        for abbrev in sorted_abbrevs:
            if abbrev not in measurement_abbrevs:  # Skip those already processed
                # Use word boundaries to ensure we're replacing whole words/phrases
                pattern = r'(?i)\b' + re.escape(abbrev) + r'\b'
                result = re.sub(pattern, abbrev_map[abbrev], result)
        
        # Final pass: Special handling for codes in parentheses that often indicate grades
        # For example: (ch), (ui), (uj)
        result = re.sub(r'\(ch\)', '(choice)', result, flags=re.IGNORECASE)
        result = re.sub(r'\(ui\)', '(usda inspection)', result, flags=re.IGNORECASE)
        result = re.sub(r'\(uj\)', '(usda inspection)', result, flags=re.IGNORECASE)
        
        # Clean up any double spaces created during replacements
        result = re.sub(r'\s+', ' ', result).strip()
        
        # Log the result for debugging
        if result != text.lower():
            logging.debug(f"Expanded to: {result}")
        
        return result

    
    def get_abbreviation_map(self):
        """
        Returns a dictionary mapping common food-related abbreviations to their full descriptions.
        
        Returns:
            dict: A dictionary of abbreviation-to-description mappings.
        """
        return {
            # Meat cut abbreviations
            'Bn-in': 'Bone in',
            'Bnls': 'Boneless',
            'Bnl': 'Boneless',
            'Cntr Cut': 'Center Cut',
            'Cov': 'Cover',
            'Dkle': 'Deckle',
            'Dfatd': 'Defatted',
            'Dnd': 'Denuded',
            'Dia': 'Diamond',
            'Div': 'Divided',
            'Ex': 'Extra',
            'Fr': 'Fresh',
            'Frz': 'Frozen',
            'Grnd': 'Ground',
            'Inter': 'Intermediate',
            'IM': 'Individual Muscle',
            'Nk-off': 'Neck off',
            'NTE': 'Not to Exceed',
            'Oven-Prep': 'Oven-Prepared',
            'Part': 'Partially',
            'Pld': 'Peeled',
            'Prthse': 'Porterhouse',
            'Portn': 'Portion',
            'Reg': 'Regular',
            'Rst-Rdy': 'Roast-Ready',
            'Rst': 'Roast',
            'Rnd': 'Round',
            'Sh Cut': 'Short Cut',
            'Shld': 'Shoulder',
            'Sirln': 'Sirloin',
            'Sknd': 'Skinned',
            'Sp': 'Special',
            'Sq-Cut': 'Square Cut',
            'Stk': 'Steak',
            'Tender': 'Tenderloin',
            'Tri Tip': 'Triangle Tip',
            'Trmd': 'Trimmed',
            'Untrmd': 'Untrimmed',
            
            # Packaging and measurement abbreviations
            'oz': 'ounce',
            '#': 'pound',
            'lb': 'pound',
            'lbs': 'pounds',
            'gal': 'gallon',
            'qt': 'quart',
            'pt': 'pint',
            'fl oz': 'fluid ounce',
            'pkg': 'package',
            'pkgs': 'packages',
            'cnt': 'container',
            'ea': 'each',
            'pcs': 'pieces',
            'pc': 'piece',
            'ct': 'count',
            'cs': 'case',
            'dz': 'dozen',
            
            # Food preparation abbreviations
            'chk': 'chicken',
            'chx': 'chicken',
            'ckn': 'chicken',
            'ck': 'chicken',
            'tur': 'turkey',
            'bf': 'beef',
            'pk': 'pork',
            'vl': 'veal',
            'lmb': 'lamb',
            'veg': 'vegetable',
            'vegt': 'vegetable',
            'vgts': 'vegetables',
            'tom': 'tomato',
            'toms': 'tomatoes',
            'pot': 'potato',
            'pots': 'potatoes',
            'chs': 'cheese',
            'chdr': 'cheddar',
            'mozz': 'mozzarella',
            'org': 'organic',
            'nat': 'natural',
            'whl': 'whole',
            'slc': 'slice',
            
            # ===== MEAT INDUSTRY SPECIFIC ABBREVIATIONS =====
            
            # Meat Grade Terminology
            'ch': 'choice',
            'cho': 'choice',
            'chce': 'choice',
            'sel': 'select',
            'prm': 'prime',
            'pr': 'prime',
            
            # Bone-related Terminology
            'bny': 'bone-in',
            'bi': 'bone-in',
            'bn': 'bone',
            'bn-in': 'bone-in',
            'bnls': 'boneless',
            'bnlss': 'boneless',
            'bnl': 'boneless',
            'bonlss': 'boneless',
            
            # Cut Style Terminology
            'lip on': 'lip-on',
            'lip-on': 'lip-on',
            'tip on': 'lip-on',  # Standardizing 'tip on' to 'lip-on'
            'tipon': 'lip-on',  # Standardizing 'tipon' to 'lip-on'
            'roll-off': 'roll off',
            'roll off': 'roll off',
            'necked': 'neck off',
            'neckoff': 'neck off',
            'neck-off': 'neck off',
            'deckle off': 'deckle off',
            'dkle off': 'deckle off',
            
            # Cut Names and Variations
            'rib eye': 'ribeye',
            'rib-eye': 'ribeye',
            'rbeye': 'ribeye',
            'rbey': 'ribeye',
            'rby': 'ribeye',
            'ribeye': 'ribeye',
            'r eye': 'ribeye',
            'hvw': 'heavy weight',
            'hvw upon': 'heavy weight ribeye',
            'hvy': 'heavy weight',
            'hvy upon': 'heavy weight ribeye',
            'hw': 'heavy weight',
            
            'chuck roll': 'chuck roll',
            'chuck clod': 'shoulder clod',
            'shoulder clod': 'shoulder clod',
            'clod': 'shoulder clod',
            'clod xt': 'shoulder clod',
            'chuck flat': 'chuck flat',
            
            'brisket flat': 'brisket flat',
            'brisket at code': 'brisket',
            'brisket deckle off': 'brisket deckle off',
            
            'outside skirt': 'outside skirt',
            'outside skrt': 'outside skirt',
            'skrt': 'skirt',
            
            'teres major': 'teres major',
            
            # Regional/Source Indicators - Standardized
            'creekstone': 'creekstone',
            'angus': 'angus',
            'oma': 'omaha',
            'flat/nose off': 'flat nose off',
            'flat/nose-off': 'flat nose off',
            'nebraska': 'nebraska',
            'neb': 'nebraska',
            
            # Processing Codes - normalize to standardize
            '1/4': 'quarter',
            
            # State/Form Indicators
            'frzn': 'frozen',
            'frz': 'frozen',
            'fr': 'fresh',
            'slcs': 'slices',
            'slcd': 'sliced',
            'pud': 'peeled and deveined',
            't/off': 'tail off',
            'ez': 'easy',
            'wht': 'white',
            'grn': 'green',
            'blk': 'black',
            'brn': 'brown',
            'med': 'medium',
            'lg': 'large',
            'sm': 'small',
            'xl': 'extra large',
            'xsm': 'extra small',
            'kc': 'Kansas City',
            'ny': 'New York',
        }

    def standardize_to_canonical(self, result: ExtractionResult) -> ExtractionResult:
        """Standardize subprimal and grade names to canonical forms using reference data."""
        
        # Standardize subprimal
        if result.subprimal and self.current_primal:
            subprimal_lower = result.subprimal.lower().strip()
            
            # Find the canonical name for this subprimal
            for canonical in self.reference_data.get_subprimals(self.current_primal):
                # Check if it's already the canonical name
                if canonical.lower() == subprimal_lower:
                    result.subprimal = canonical  # Ensure exact case match
                    break
                    
                # Check synonyms for this canonical name
                synonyms = self.reference_data.get_subprimal_synonyms(self.current_primal, canonical)
                if synonyms:
                    for synonym in synonyms:
                        if synonym.lower().strip() == subprimal_lower:
                            logger.info(f"Standardized subprimal '{result.subprimal}' → '{canonical}' (canonical)")
                            result.subprimal = canonical
                            break
                    else:
                        continue
                    break
            else:
                # If no match found, log warning but keep original
                logger.warning(f"Could not standardize subprimal '{result.subprimal}' for {self.current_primal}")
        
        # Standardize grade using ReferenceDataLoader methods
        if result.grade:
            grade_lower = result.grade.lower().strip()
            
            # Get all official grades from reference data
            for canonical_grade in self.reference_data.get_grades():
                # Check if it's already the canonical name
                if canonical_grade.lower().strip() == grade_lower:
                    result.grade = canonical_grade  # Ensure exact case match
                    break
                    
                # Check synonyms for this canonical grade
                synonyms = self.reference_data.get_grade_synonyms(canonical_grade)
                if synonyms:
                    for synonym in synonyms:
                        if synonym.lower().strip() == grade_lower:
                            logger.info(f"Standardized grade '{result.grade}' → '{canonical_grade}' (canonical)")
                            result.grade = canonical_grade
                            break
                    else:
                        continue
                    break
            else:
                # If no match found, log warning but keep original
                logger.warning(f"Could not standardize grade '{result.grade}'")
        
        return result

