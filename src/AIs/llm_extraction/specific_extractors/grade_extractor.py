"""
Grade Extractor
Specialized extractor for extracting grade information from product descriptions.
"""

import os
import logging
from typing import Dict, List, Optional, Set, Any
from pydantic import BaseModel, Field

# Import the base extractor class
from ..base_extractor import BaseLLMExtractor

# Import reference data loader with absolute import to avoid relative import issues
try:
    from data_ingestion.utils.reference_data_loader import ReferenceDataLoader
except ImportError:
    # Fallback for different import contexts
    from src.data_ingestion.utils.reference_data_loader import ReferenceDataLoader

from dotenv import load_dotenv

# Define a simple Pydantic model for grade extraction output
class GradeExtractionResult(BaseModel):
    """Simple model for grade extraction output."""
    grade: Optional[str] = Field(None, description="The identified grade or null if not found")
    confidence: float = Field(0.0, description="Confidence level in the extraction")
    needs_review: bool = Field(True, description="Whether human review is needed")

# Ensure environment variables are loaded
load_dotenv()

logger = logging.getLogger(__name__)

class GradeExtractor(BaseLLMExtractor):
    """Specialized extractor for grade information.
    
    Focuses only on extracting and validating grade information from product descriptions.
    """
    
    def __init__(self, reference_data_path: str = "data/incoming/beef_cuts.xlsx"):
        """Initialize with reference data."""
        # Initialize base extractor first
        super().__init__()
        
        # Load reference data
        try:
            self.reference_data = ReferenceDataLoader(reference_data_path)
            self.grade_mapping = self.reference_data.build_grade_mapping()
            logger.info(f"Loaded reference data for beef grades")
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
                        self.grade_mapping = self.reference_data.build_grade_mapping()
                        logger.info(f"Successfully loaded reference data from path: {path}")
                        return
                        
                raise FileNotFoundError("Could not find beef_cuts.xlsx in any expected location")
                        
            except Exception as inner_e:
                logger.error(f"All attempts to load reference data failed: {inner_e}")
                raise RuntimeError(f"Cannot initialize grade extractor without valid reference data: {inner_e}")
    
    # Implement abstract methods from BaseLLMExtractor
    def get_subprimal_mapping(self) -> Dict[str, List[str]]:
        """Return mapping of standard subprimal names to their variations."""
        # Not relevant for grade extractor, but required by base class
        return {}
    
    def get_category_name(self) -> str:
        """Return the category name."""
        # Not specific to a category
        return "Beef"
    
    def get_valid_grades(self) -> Dict[str, List[str]]:
        """Get valid beef grades with synonyms from reference data."""
        grade_mapping = {}
        
        # Get all official grades from reference data
        for grade in self.reference_data.get_grades():
            synonyms = self.reference_data.get_grade_synonyms(grade)
            grade_mapping[grade] = synonyms
            
        return grade_mapping
    
    def generate_system_prompt(self) -> str:
        """Generate a system prompt specialized for grade extraction."""
        # Build comma-separated list of recognized grades from reference data
        grade_list = ', '.join(self.reference_data.get_grades())
        
        return f"""You are a meat industry expert specialized in identifying beef grades.
Extract ONLY the grade information from product descriptions with high accuracy.

GRADE DETERMINATION PROCESS ─────────────────────────────────────────
Follow these steps IN ORDER. As soon as a step yields a grade, STOP and use it; ignore all lower-priority clues.

GRADE DETECTION TABLE (case insensitive) - EXACT PRIORITY ORDER:
┌─────────────────────┬─────────────┬───────────┐
│ Input Description   │ Output Grade│ Reason    │
├─────────────────────┼─────────────┼───────────┤
│ "AAA USDA Choice"   │ "AAA"       │ AAA>Choice│
│ "AA USDA Select"    │ "AA"        │ AA>Select │
│ "USDA Prime A"      │ "A"         │ A>Prime   │
│ "Hereford Choice"   │ "Hereford"  │ Hereford  │
│ "USDA CH+"          │ "Choice"    │ CH+       │
│ "No Roll"           │ "NR"        │ NR        │
└─────────────────────┴─────────────┴───────────┘
1. Search for the explicit Canadian grades "AAA", "AA", or "A" (case-insensitive, may appear with the word "Canadian").
   • If found, output EXACTLY the uppercase version of the grade ("AAA", "AA", or "A")
2. Search for the keyword "Hereford" (anywhere in the text).
   • If found, output grade "Hereford".
3. Search for the plus grades:
   • "CH+", "Choice+"  → output "Choice"
   • "PR+", "Prime+"   → output "Prime"
4. Search for NR tokens: "N/R", "NR", or "No-Roll"  → output "NR".
5. Search for the recognized USDA grades ({grade_list}) and use the first one found.
6. If no grade tokens are detected, output null for grade and set needs_review = true.

IMPORTANT CLARIFICATION:
• When A / AA / AAA appear, you MUST output exactly "A", "AA", or "AAA" for the grade and DISREGARD any token such as "USDA Choice", "Prime", "CH+", etc. If the item says Canadian, it must be A, AA, or AAA
• When "Hereford" appears, you MUST output grade "Hereford" and ignore every other grade token.
• Tokens like "CH+ to Choice", "PR+ to Prime" only apply if none of the higher-priority tokens appear.

Do NOT include any other attributes beyond the grade. Focus only on extracting the grade with high confidence.

Return valid JSON with the following schema:
{{
  "grade": "string or null",
  "confidence": float between 0.0 and 1.0,
  "needs_review": boolean
}}
"""

    def generate_user_prompt(self, description: str) -> str:
        """Generate a user prompt for grade extraction."""
        return f"""Extract ONLY the grade from this product description:

Description: "{description}"

Grade abbreviations and their canonical mappings:
- (CH), CH+, Choice+ = Choice
- (PR), PR+, Prime+ = Prime
- (SEL) = Select
- (UT) = Utility
- A, Canadian A = A
- AA, Canadian AA = AA (overrides any other grade; ignore USDA labels if both present)
- AAA, Canadian AAA = AAA (overrides any other grade)
- N/R, NR, No-Roll = NR
- (WAG) = Wagyu
- (ANG), CAngus, Choice Angus = (Other) Angus Brands
- (CAB) = Creekstone Angus
- Hereford (anywhere) = Hereford

BRAND-SPECIFIC GRADE EQUIVALENCIES:
- St. Helen's is a Canadian brand that uses Canadian grading system:
  - St. Helen's A = n/r (no rating/standard)
  - St. Helen's AA = Select
  - St. Helen's AAA = Choice
- When St. Helen's brand is detected with A, AA, or AAA grades, use the above equivalencies

Return a JSON object with:
- grade: The identified grade or null if not found
- confidence: Value between 0 and 1 indicating extraction confidence
- needs_review: Boolean indicating if human review is needed

Example:
Input: "Beef Chuck Ribeye AAA USDA Choice 10lb"
Output: {{"grade": "AAA", "confidence": 0.95, "needs_review": false}}

Input: "St. Helen's AA Ribeye 12oz"
Output: {{"grade": "AA", "confidence": 0.9, "needs_review": false}}

Input: "Beef Ribeye 8oz"
Output: {{"grade": null, "confidence": 0.3, "needs_review": true}}
"""

    def extract(self, description: str) -> GradeExtractionResult:
        """Extract grade information from a product description.
        
        Args:
            description: The product description to extract from
            
        Returns:
            GradeExtractionResult with grade information
        """
        # Initialize with default values
        result = GradeExtractionResult()
        
        if not description or not str(description).strip():
            logger.warning("Empty description provided for grade extraction")
            return result
            
        try:
            # Generate specialized prompts for grade extraction
            system_prompt = self.generate_system_prompt()
            user_prompt = self.generate_user_prompt(description)
            
            # Call the LLM
            response = self.call_llm(
                description, 
                user_prompt=user_prompt,
                system_prompt=system_prompt
            )
            
            if not response:
                logger.error("API call returned None or empty response")
                return result
                
            # Parse JSON response
            extraction_data = self.parse_response(response)
            
            if not extraction_data:
                logger.error("Failed to parse JSON response from API")
                return result
                
            # Update the result with the extracted grade
            result.grade = extraction_data.get('grade')
            result.confidence = extraction_data.get('confidence', 0.0)
            result.needs_review = extraction_data.get('needs_review', True)
            
            # Validate the grade
            if result.grade:
                # Get valid grades
                grade_mapping = self.get_valid_grades()
                valid_grades = []
                for standard_grade, variations in grade_mapping.items():
                    valid_grades.extend([standard_grade] + variations)
                
                # Check if grade matches any valid grade (case-insensitive)
                grade_lower = result.grade.lower()
                if grade_lower in [g.lower() for g in valid_grades]:
                    # Normalize to standard format if found in specific grade mappings
                    for standard_grade, variations in grade_mapping.items():
                        if grade_lower in [v.lower() for v in [standard_grade] + variations]:
                            result.grade = standard_grade
                            break
                else:
                    logger.warning(f"Unknown grade: {result.grade}")
                    result.needs_review = True
            
            return result
            
        except Exception as e:
            logger.error(f"Grade extraction failed with error: {str(e)}")
            return GradeExtractionResult(needs_review=True)

# Convenience function for direct extraction
def extract_grade(description: str) -> Optional[str]:
    """Extract grade from a product description.
    
    Args:
        description: The product description to extract from
        
    Returns:
        The extracted grade or None if not found
    """
    extractor = GradeExtractor()
    result = extractor.extract(description)
    return result.grade
