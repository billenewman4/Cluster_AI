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
        'creekstone angus', 'no grade'
    }
    
    VALID_SIZE_UNITS = {'oz', 'lb', 'g', 'kg', 'in', 'inch', 'inches'}
    
    def __init__(self):
        # Use APIManager instead of directly creating client
        api_key = os.getenv("OPENAI_API_KEY")
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.api_manager = APIManager(api_key=api_key, model=self.model)
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
            
            # Use APIManager for API calls - this handles rate limiting and retries
            return self.api_manager.call_with_retry(
                system_prompt=system_prompt,  
                user_prompt=user_prompt,
                temperature=0.6
            )
            
        except Exception as e:
            logger.error(f"LLM call failed: {str(e)}")
            return None
    
    def parse_response(self, response: str) -> Optional[Dict]:
        """Parse LLM JSON response."""
        # Use ResultParser for consistent JSON parsing
        return self.result_parser.parse_json_response(response)
    
    def apply_regex_fallbacks(self, description: str) -> Dict:
        """Apply regex patterns as fallback for extraction."""
        result = {}
        description_lower = description.lower()
        
        # Subprimal detection with regex
        subprimal_mapping = self.get_subprimal_mapping()
        for standard_name, variations in subprimal_mapping.items():
            for variation in variations:
                if re.search(r'\b' + re.escape(variation.lower()) + r'\b', description_lower):
                    result['subprimal'] = standard_name
                    break
            if result.get('subprimal'):
                break
        
        # Grade detection
        for grade in self.VALID_GRADES:
            if re.search(r'\b' + re.escape(grade.lower()) + r'\b', description_lower):
                result['grade'] = grade.title()
                break
        
        # Size detection
        size_match = re.search(r'(\d+(?:\.\d+)?)\s*(oz|lb|#|g|kg)\b', description, re.IGNORECASE)
        if size_match:
            result['size'] = float(size_match.group(1))
            result['size_uom'] = size_match.group(2).lower()
        
        # Bone-in detection
        result['bone_in'] = bool(re.search(r'\bbone.?in\b', description_lower))
        
        return result
    
    def validate_and_score(self, raw_result: Dict, description: str) -> ExtractionResult:
        """Validate results and assign confidence score."""
        result = ExtractionResult()
        
        # Extract fields
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