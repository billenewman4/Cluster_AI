"""
Base LLM Extractor
Provides common functionality for all LLM-based meat attribute extraction.
"""

import os
import json
import re
import logging
from typing import Dict, Optional, List
from dataclasses import dataclass
from abc import ABC, abstractmethod

from dotenv import load_dotenv

# Import utils from the same package
from ..utils.api_utils import APIManager
from ..utils.result_parser import ResultParser

load_dotenv()
logger = logging.getLogger(__name__)

@dataclass
class ExtractionResult:
    """Base result structure for LLM extraction."""
    primal: Optional[str] = None
    subprimal: Optional[str] = None
    grade: Optional[str] = None
    size: Optional[float] = None
    size_uom: Optional[str] = None
    brand: Optional[str] = None
    bone_in: bool = False
    confidence: float = 0.0
    needs_review: bool = False

class BaseLLMExtractor(ABC):
    """Base class for LLM-based meat attribute extraction."""
    
    # Common valid attributes across all meat types
    VALID_GRADES = {
        'prime', 'choice', 'select', 'utility', 'wagyu', 'angus', 'certified angus', 
       'Hereford', 'creekstone angus', 'no grade', "A", "AA", "AAA"
    }
    
    VALID_SIZE_UNITS = {'oz', 'lb', 'g', 'kg', 'in', 'inch', 'inches'}
    
    def __init__(self):
        # Use APIManager instead of directly creating client
        api_key = os.getenv("OPENAI_API_KEY")
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        
        # o3 and 4o-mini models use temperature=1.0, other models use 0.8
        temperature = 1.0 if self.model in ["o3", "gpt-4o-mini"] else 0.2
        
        self.api_manager = APIManager(api_key=api_key, model=self.model, temperature=temperature)
        self.result_parser = ResultParser()
    
    @abstractmethod
    def get_subprimal_mapping(self) -> Dict[str, List[str]]:
        """Return mapping of standard subprimal names to their variations."""
        pass
    
    @abstractmethod
    def get_category_name(self) -> str:
        """Return the category name (e.g., 'Beef Chuck', 'Beef Rib')."""
        pass
    
    def get_valid_grades(self) -> Dict[str, List[str]]:
        """Get valid grades. Override in subclasses for specific grade mappings."""
        return {grade: [] for grade in self.VALID_GRADES}
    
    def call_llm(self, description: str, user_prompt: str = None, system_prompt: str = None) -> Optional[str]:
        """Call LLM with the specialized prompt."""
        try:
            # o3 and 4o-mini models use temperature=1.0, other models use 0.6
            temperature = 1.0 if self.model in ["o3", "gpt-4o-mini"] else 0.6
            
            # Use APIManager for API calls - this handles rate limiting and retries
            return self.api_manager.call_with_retry(
                system_prompt=system_prompt,  
                user_prompt=user_prompt,
                temperature=temperature
            )
            
        except Exception as e:
            logger.error(f"LLM call failed: {str(e)}")
            return None
    
    def parse_response(self, response: str) -> Optional[Dict]:
        """Parse LLM JSON response."""
        # Use ResultParser for consistent JSON parsing
        return self.result_parser.parse_json_response(response)
    
    
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

            # USDA Item No.
            '116G': 'Chuck, Under Blade, Center Cut (IM)',
            '116H': 'Chuck, Chuck Eye (IM)',
            '116I': 'Chuck, Neck Roast',
            '116K': 'Chuck Roll, 3-Way',
            '117': 'Foreshank',
            '118': 'Brisket',
            '119': 'Brisket, Deckle-On, Boneless',
            '120': 'Brisket, Deckle-Off, Boneless',
            '120A': 'Brisket, Flat Cut, Boneless (IM)',
            '120B': 'Brisket, Point Cut, Boneless (IM)',
            '120C': 'Brisket, 2 Piece, Boneless',
            '121': 'Plate, Short Plate',
            '121A': 'Plate, Short Plate, Boneless',
            '121B': 'Plate, Short Plate, Trimmed, Boneless',
            '121C': 'Plate, Outside Skirt (IM)',
            '121D': 'Plate, Inside Skirt (IM)',
            '121E': 'Plate, Outside Skirt (IM), Skinned',
            '121F': 'Plate, Short Plate, Short Ribs Removed',
            '121G': 'Plate, Short Plate, Short Ribs Removed, Boneless',
            '122': 'Plate, Full',
            '122A': 'Plate, Full, Boneless',
            '123': 'Short Ribs',
            '123A': 'Short Plate, Short Ribs, Trimmed Amount as Specified',
            '123B': 'Rib, Short Ribs, Trimmed Amount as Specified',
            '123C': 'Rib, Short Ribs Amount as Specified',
            '123D': 'Short Ribs, Boneless',
            '124': 'Rib, Back Ribs',
            '124A': 'Rib, Back Rib, Rib Fingers',
            '124B': 'Plate, Rib Fingers',
            '125': 'Chuck, Armbone',
            '126': 'Chuck, Armbone, Boneless',
            '126A': 'Chuck, Armbone, Clod-Out, Boneless',
            '127': 'Chuck, Cross-Cut',
            '128': 'Chuck, Cross-Cut, Boneless',
            '130': 'Chuck, Short Ribs',
            '130A': 'Chuck, Short Ribs, Boneless',
            '132': 'Triangle',
            '133': 'Triangle, Boneless',
            '134': 'Beef Bones',
            '135': 'Diced Beef',
            '135A': 'Beef for Stewing',
            '135B': 'Beef for Kabobs',
            '135C': 'Beef for Stir Fry',
            '136': 'Ground Beef',
            '136A': 'Ground Beef and Soy Protein Product Patty Mix',
            '136C': 'Beef Patty Mix, NTE 10% Fat',
            '136D': 'Pure Beef',
            '137': 'Ground Beef, Special',
            '137A': 'Ground Beef and Soy Protein Product, Special',
            '138': 'Beef Trimmings',
            '139': 'Special Trim, Boneless',
            '140': 'Hanging Tender (IM)',
            '155': 'Hindquarter',
            '155A': 'Hindquarter, Boneless',
            '157': 'Hindshank',
            '158': 'Round, Primal',
            '158A': 'Round, Diamond-Cut',
            '158B': 'Round, NY Style',
            '159': 'Round, Primal, Boneless',
            '160': 'Round, Shank-Off, Partially Boneless',
            '160A': 'Round, Diamond Cut, Shank Off, Partially Boneless',
            '160B': 'Round, Heel and Shank Off, Without Sirloin Tip, Boneless',
            '163': 'Round, Shank Off, 3-Way, Boneless',
            '163A': 'Round, Shank Off, 3-Way, Untrimmed, Boneless',
            '164': 'Round, Rump and Shank Off',
            '165': 'Round, Rump and Shank Off, Boneless',
            '165A': 'Round, Rump and Shank Off, Boneless, Special',
            '165B': 'Round, Rump and Shank Off, Boneless, Special',
            '166': 'Round, Rump and Shank Off, Boneless',
            '166A': 'Round, Rump Partially Removed, Shank Off',
            '166B': 'Round, Rump and Shank Partially Off, Handle On',
            '167': 'Round, Sirloin Tip(Knuckle)',
            '167A': 'Round, Sirloin Tip (Knuckle), Peeled',
            '167B': 'Round, Full Sirloin Tip',
            '167C': 'Round, Sirloin (Full) Tip',
            '167F': 'Round, Sirloin Tip, Side Roast (IM)',
            '168': 'Round, Top (Inside), Untrimmed',
            '169': 'Round, Top (Inside)',
            '169A': 'Round, Top (Inside), Cap Off',
            '169B': 'Round, Top (Inside), Cap (IM)',
            '169C': 'Round, Top (Inside), Front Side (IM)',
            '169D': 'Round, Top (Inside) Soft Side Removed',
            '169E': 'Round, Top (Inside) (IM)',
            '170': 'Round, Bottom (Gooseneck)',
            '170A': 'Round, Bottom (Gooseneck), Heel Out',
            '171': 'Round, Bottom (Gooseneck), Untrimmed',
            '171A': 'Round, Bottom (Gooseneck), Untrimmed, Heel Out',
            '171B': 'Round, Outside Round (Flat)',
            '171C': 'Round, Eye of Round (IM)',
            '171D': 'Round, Outside Round, Side Muscle Removed (IM)',
            '171E': 'Round, Outside Round, Side Roast (IM)',
            '171F': 'Round, Outside Round, Heel',
            '171G': 'Round, Outside Round, Rump (IM)',
            '172': 'Loin, Full Loin, Trimmed',
            '172A': 'Loin, Full Loin, Diamond Cut, Trimmed',
            '173': 'Loin, Short Loin',
            '174': 'Loin, Short Loin, Short-Cut',
            '175': 'Loin, Strip Loin',
            '176': 'Loin, Steak Tails',
            '180': 'Loin, Strip Loin, Boneless',
            '181': 'Loin, Sirloin',
            '181A': 'Loin, Top Sirloin',
            '182': 'Loin, Sirloin Butt, Boneless',
            '183': 'Loin, Sirloin Butt, Trimmed, Boneless',
            '184': 'Loin, Top Sirloin Butt, Boneless',
            '184A': 'Loin, Top Sirloin Butt, Semi Center-Cut, Boneless',
            '184B': 'Loin, Top Sirloin Butt, Center-Cut, Cap Off (IM), Boneless',
            '184C': 'Loin, Top Sirloin Butt, Untrimmed, Boneless',
            '184D': 'Loin, Top Sirloin Butt, Cap (IM)',
            '184E': 'Loin, Top Sirloin Butt, 2-Piece',
            '184F': 'Loin, Top Sirloin Butt, Center-Cut, Seamed, Dorsal Side (IM)',
            '185': 'Loin, Bottom Sirloin Butt, Boneless',
            '185A': 'Loin, Bottom Sirloin Butt, Flap, Boneless (IM)',
            '185B': 'Loin, Bottom Sirloin Butt, Ball Tip, Boneless',
            '185C': 'Loin, Bottom Sirloin Butt, Tri-Tip, Boneless (IM)',
            '185D': 'Loin, Bottom Sirloin Butt, Tri-Tip, Defatted, Boneless',
            '186': 'Loin, Bottom Sirloin Butt, Trimmed, Boneless',
            '188': 'Loin, Tenderloin, Bone-in',
            '189': 'Loin, Tenderloin, Full',
            '189A': 'Loin, Tenderloin, Full, Side Muscle On, Defatted',
            '189B': 'Loin, Tenderloin, Full, Side Muscle On, Partially Defatted',
            '190': 'Loin, Tenderloin, Full, Side Muscle Off, Defatted',
            '190A': 'Loin, Tenderloin, Full, Side Muscle Off, Skinned',
            '190B': 'Loin, Tenderloin, Full, Side Muscle Off, Center-Cut, Skinned (IM)',
            '191': 'Loin, Tenderloin, Butt',
            '191A': 'Loin, Tenderloin Butt, Defatted',
            '191B': 'Loin, Tenderloin Butt, Skinned',
            '192': 'Loin, Tenderloin, Short',
            '192A': 'Loin, Tenderloin Tails',
            '193': 'Flank, Flank Steak (IM)',
            '194': 'Flank, Rose Meat (IM)',
        }
        
    def expand_abbreviations(self, text: str) -> str:
        """
        Expands common food-related abbreviations in the given text to their full descriptions,
        with special handling for meat industry terminology.
        
        Args:
            text (str): The text containing potential abbreviations.
            
        Returns:
            str: The text with abbreviations expanded to their full descriptions.
        """
        if not text:
            return text
            
        # Get abbreviation dictionary
        abbrev_dict = self.get_abbreviation_map()
        
        # First check if the text is a valid USDA code by itself
        if text in abbrev_dict and text.isdigit() or (len(text) > 3 and text[:3].isdigit() and text[3:].isalpha()):
            # This might be a USDA code, so we'll return the expanded version
            return abbrev_dict.get(text, text)
        
        # Otherwise, process the text word by word
        words = text.split()
        result_words = []
        
        for word in words:
            # Check if the word is an abbreviation we know
            if word.lower() in map(str.lower, abbrev_dict.keys()):
                # Find the exact key with case insensitive match
                key = next((k for k in abbrev_dict.keys() if k.lower() == word.lower()), None)
                if key:
                    result_words.append(abbrev_dict[key])
                else:
                    result_words.append(word)  # Should not happen given the check above
            else:
                result_words.append(word)
        
        return ' '.join(result_words)
        
    def extract_usda_code(self, description: str) -> Optional[str]:
        """
        Extract USDA code from a product description.
        First checks for direct USDA code matches, then tries to find codes in the description.
        
        Args:
            description (str): The product description to extract from
            
        Returns:
            str: The extracted USDA code or None if not found
        """
        if not description:
            return None
            
        # Get USDA code mapping dictionary
        usda_code_map = {k: v for k, v in self.get_abbreviation_map().items() 
                       if k.isdigit() or (len(k) > 3 and k[:3].isdigit() and k[3:].isalpha())}
        
        # First search for direct USDA code matches
        words = description.split()
        for word in words:
            # Clean the word (remove non-alphanumeric chars except for letters after digits like 120A)
            clean_word = re.sub(r'[^0-9A-Za-z]', '', word)
            
            # Check if the cleaned word is a USDA code
            if clean_word in usda_code_map:
                return clean_word
        
        # If no direct match, try to find description matches
        description_lower = description.lower()
        for code, desc in usda_code_map.items():
            # Check if the description contains this USDA code description
            if desc.lower() in description_lower:
                return code
        
        # No match found
        return None

    def validate_and_score(self, raw_result: Dict, description: str) -> ExtractionResult:
        """Validate results and assign confidence score."""
        result = ExtractionResult()
        
        # Extract fields
        result.primal = raw_result.get('primal')
        result.subprimal = raw_result.get('subprimal')
        result.grade = raw_result.get('grade') 
        result.size = raw_result.get('size')
        result.size_uom = raw_result.get('size_uom')
        result.brand = raw_result.get('brand')
        result.bone_in = raw_result.get('bone_in', False)
        
        # Use AI's confidence and needs_review if provided, otherwise calculate
        ai_confidence = raw_result.get('confidence')
        ai_needs_review = raw_result.get('needs_review')
        
        # Validation and confidence scoring (only as fallback)
        confidence_score = 0.5  # Base confidence
        validation_needs_review = False

        #validate primal
        if result.primal:
            confidence_score += 0.0
        else:
            validation_needs_review = True
            logger.warning(f"Unknown primal for {self.get_category_name()}: {result.primal}")
        
        # Validate subprimal (case-insensitive)
        subprimal_mapping = self.get_subprimal_mapping()
        if result.subprimal:
            # Check if subprimal matches any key (case-insensitive)
            subprimal_lower = result.subprimal.lower()
            
            # Create a case-insensitive mapping lookup
            mapping_lower = {k.lower(): k for k in subprimal_mapping.keys()}
            
            if subprimal_lower in mapping_lower:
                confidence_score += 0.3
                # Normalize to the standard case from mapping
                result.subprimal = mapping_lower[subprimal_lower]
            else:
                validation_needs_review = True
                logger.warning(f"Unknown subprimal for {self.get_category_name()}: {result.subprimal}")
        
        # Validate grade (use specific grade mappings if available)
        if result.grade:
            # Get all valid grades using the overridable method
            grade_mapping = self.get_valid_grades()
            if grade_mapping:
                valid_grades = []
                for standard_grade, variations in grade_mapping.items():
                    valid_grades.extend([standard_grade] + variations)
            else:
                valid_grades = [g.lower() for g in self.VALID_GRADES]
            
            # Check if grade matches any valid grade (case-insensitive)
            grade_lower = result.grade.lower()
            if grade_lower in [g.lower() for g in valid_grades]:
                confidence_score += 0.1
                # Normalize to standard format if found in specific grade mappings
                if grade_mapping:
                    for standard_grade, variations in grade_mapping.items():
                        if grade_lower in [v.lower() for v in [standard_grade] + variations]:
                            print("Changing grade from", grade_lower, "to", standard_grade)
                            result.grade = standard_grade
                            break
            else:
                validation_needs_review = True
                logger.warning(f"Unknown grade: {result.grade}")
        
        # Validate size unit
        if result.size_uom and result.size_uom in self.VALID_SIZE_UNITS:
            confidence_score += 0.05
        elif result.size_uom:
            validation_needs_review = True
            logger.warning(f"Unknown size unit: {result.size_uom}")
        
        # Check if we found any specific information
        if result.subprimal or result.grade or result.size:
            confidence_score += 0.05
        
        calculated_confidence = min(confidence_score, 1.0)
        calculated_needs_review = validation_needs_review or calculated_confidence < 0.6
        
        # Use AI's assessment if provided, otherwise use calculated values
        if ai_confidence is not None:
            result.confidence = float(ai_confidence)
            logger.debug(f"Using AI confidence: {ai_confidence} (calculated: {calculated_confidence})")
        else:
            result.confidence = calculated_confidence
            logger.debug(f"Using calculated confidence: {calculated_confidence}")
        
        if ai_needs_review is not None:
            result.needs_review = bool(ai_needs_review)
            logger.debug(f"Using AI needs_review: {ai_needs_review} (calculated: {calculated_needs_review})")
        else:
            result.needs_review = calculated_needs_review
            logger.debug(f"Using calculated needs_review: {calculated_needs_review}")
        
        # Override to True if validation found critical issues (safety check)
        if validation_needs_review and not result.needs_review:
            result.needs_review = True
            logger.warning("Overriding AI needs_review=False due to validation issues")
        
        return result
    
    def extract(self, description: str) -> ExtractionResult:
        """Extract meat information from description."""
        
        # First try LLM
        llm_response = self.call_llm(description)
        parsed_result = self.parse_response(llm_response) if llm_response else print("LLM response is None")
        
        if not parsed_result:
            raise Exception("LLM extraction failed")
        
        # Validate and score
        result = self.validate_and_score(parsed_result, description)
        
        return result 