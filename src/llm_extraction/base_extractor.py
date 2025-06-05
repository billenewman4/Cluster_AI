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

from openai import OpenAI
from dotenv import load_dotenv

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
    
    VALID_SIZE_UNITS = {'oz', 'lb', '#', 'g', 'kg', 'in', 'inch', 'inches'}
    
    def __init__(self):
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        # Use GPT-4o-mini for optimal balance of speed, cost, and accuracy
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    
    @abstractmethod
    def get_subprimal_mapping(self) -> Dict[str, List[str]]:
        """Return mapping of standard subprimal names to their variations."""
        pass
    
    @abstractmethod
    def get_category_name(self) -> str:
        """Return the category name (e.g., 'Beef Chuck', 'Beef Rib')."""
        pass
    
    def create_prompt(self, description: str) -> str:
        """Create specialized prompt for extraction."""
        
        # Get subprimal mapping for this category
        subprimal_mapping = self.get_subprimal_mapping()
        category_name = self.get_category_name()
        
        # Build subprimal list with all variations for better acronym handling
        subprimal_text = []
        for standard_name, variations in subprimal_mapping.items():
            all_forms = [standard_name] + variations
            subprimal_text.append(f"- {standard_name}: {', '.join(all_forms)}")
        
        cuts_list = '\n'.join(subprimal_text)
        
        # Get beef-specific grades if available
        if hasattr(self, 'get_beef_grades'):
            beef_grades = self.get_beef_grades()
            grades_text = []
            for standard_grade, variations in beef_grades.items():
                grades_text.append(f"- {standard_grade.title()}: {', '.join(variations)}")
            grades_list = '\n'.join(grades_text)
        else:
            # Fallback to general grades
            grades_list = "Prime, Choice, Select, Utility, Wagyu, Angus, Certified Angus, Creekstone Angus, No Grade"
        
        system_prompt = f"""You are a meat industry expert specializing in {category_name}. Extract information from abbreviated product descriptions.

IMPORTANT: These descriptions use heavy abbreviations (e.g., "Bf" = Beef, "Ch" = Choice, "Shl" = Shoulder).

SUBPRIMAL CUTS TO IDENTIFY:
{cuts_list}

BEEF GRADES TO LOOK FOR (or any variation):
{grades_list}

COMMON ABBREVIATIONS:
- Bf = Beef
- Ch = Choice, Pr = Prime, Se = Select, Ute = Utility  
- Shl = Shoulder, Clod = Clod
- N/Off = Natural/Off, Bi = Bone-In
- Ang = Angus, Aaa = AAA Grade (Choice), AA = Select
- # = pounds, oz = ounces
- NR = No Roll, A = Utility grade
-- please use common sense for any other abbreviations you detect 

Extract and return ONLY JSON:
{{"subprimal": "standard_name_from_list_above", "grade": "grade_if_found", "size": numeric_value, "size_uom": "unit", "brand": "brand_if_found", "bone_in": true_or_false}}

Input: "{description}"
JSON:"""
        
        return system_prompt
    
    def call_llm(self, description: str) -> Optional[str]:
        """Call LLM with the specialized prompt."""
        try:
            prompt = self.create_prompt(description)
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0,  # Deterministic for speed
                max_tokens=150,   # Reduced for speed
                timeout=30        # Add timeout for speed
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.error(f"LLM call failed: {str(e)}")
            return None
    
    def parse_response(self, response: str) -> Optional[Dict]:
        """Parse LLM JSON response."""
        if not response:
            return None
            
        try:
            # Extract JSON from response
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                json_str = json_match.group()
                return json.loads(json_str)
            else:
                return json.loads(response)
                
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON: {response[:100]}...")
            return None
    
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
        
        # Brand detection (simple approach)
        brand_keywords = ['certified', 'angus', 'creekstone', 'wagyu']
        for keyword in brand_keywords:
            if keyword in description_lower:
                # Extract surrounding context as potential brand
                brand_match = re.search(rf'\b\w*{keyword}\w*(?:\s+\w+)*', description, re.IGNORECASE)
                if brand_match:
                    result['brand'] = brand_match.group().strip()
                break
        
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
        
        # Validation and confidence scoring
        confidence_score = 0.5  # Base confidence
        
        # Validate subprimal (case-insensitive)
        subprimal_mapping = self.get_subprimal_mapping()
        if result.subprimal:
            # Check if subprimal matches any key (case-insensitive)
            subprimal_lower = result.subprimal.lower()
            if subprimal_lower in subprimal_mapping:
                confidence_score += 0.3
                # Normalize to the standard lowercase key
                result.subprimal = subprimal_lower
            else:
                result.needs_review = True
                logger.warning(f"Unknown subprimal for {self.get_category_name()}: {result.subprimal}")
        
        # Validate grade (use beef-specific grades if available)
        if result.grade:
            # Get all valid grades (beef-specific or general)
            if hasattr(self, 'get_beef_grades'):
                beef_grades = self.get_beef_grades()
                valid_grades = []
                for standard_grade, variations in beef_grades.items():
                    valid_grades.extend([standard_grade] + variations)
            else:
                valid_grades = [g.lower() for g in self.VALID_GRADES]
            
            # Check if grade matches any valid grade (case-insensitive)
            grade_lower = result.grade.lower()
            if grade_lower in [g.lower() for g in valid_grades]:
                confidence_score += 0.1
                # Normalize to standard format if found in beef-specific grades
                if hasattr(self, 'get_beef_grades'):
                    for standard_grade, variations in beef_grades.items():
                        if grade_lower in [v.lower() for v in [standard_grade] + variations]:
                            result.grade = standard_grade
                            break
            else:
                result.needs_review = True
                logger.warning(f"Unknown grade: {result.grade}")
        
        # Validate size unit
        if result.size_uom and result.size_uom in self.VALID_SIZE_UNITS:
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
    
    def extract(self, description: str) -> ExtractionResult:
        """Extract meat information from description."""
        
        # First try LLM
        llm_response = self.call_llm(description)
        parsed_result = self.parse_response(llm_response) if llm_response else None
        
        if not parsed_result:
            # Fall back to regex
            logger.debug("LLM extraction failed, using regex fallback")
            parsed_result = self.apply_regex_fallbacks(description)
        
        # Validate and score
        result = self.validate_and_score(parsed_result, description)
        
        return result 