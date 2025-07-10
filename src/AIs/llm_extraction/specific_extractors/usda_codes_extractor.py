"""
USDA Codes Extractor
Specialized extractor for USDA codes from product descriptions.
This is a placeholder implementation for future development.
"""

import os
import re
import logging
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field

# Import the base extractor class
from ..base_extractor import BaseLLMExtractor

from dotenv import load_dotenv

# Define a simple Pydantic model for USDA codes extraction output
class USDACodeExtractionResult(BaseModel):
    """Simple model for USDA code extraction output."""
    usda_code: Optional[str] = Field(None, description="The identified USDA code or null if not found")
    confidence: float = Field(1.0, description="Confidence level in the extraction - defaults to 1.0 since we only return high confidence results")
    needs_review: bool = Field(False, description="Whether human review is needed - defaults to False since we only return high confidence results")

# Ensure environment variables are loaded
load_dotenv()

logger = logging.getLogger(__name__)

class USDACodesExtractor(BaseLLMExtractor):
    """Specialized extractor for USDA codes information.
    
    Placeholder implementation - will be enhanced in the future.
    """
    
    def __init__(self):
        """Initialize the USDA codes extractor."""
        # Initialize base extractor first
        super().__init__()
        
        # USDA codes specific initialization can be added here
        logger.info("Initialized USDA Codes Extractor (placeholder)")
    
    # Implement abstract methods from BaseLLMExtractor
    def get_subprimal_mapping(self) -> Dict[str, List[str]]:
        """Return mapping of standard subprimal names to their variations."""
        # Not relevant for USDA codes extractor, but required by base class
        return {}
    
    def get_category_name(self) -> str:
        """Return the category name."""
        return "Beef"
    
    def get_valid_grades(self) -> Dict[str, List[str]]:
        """Get valid beef grades with synonyms."""
        # Not directly relevant for USDA codes extractor
        return {grade: [] for grade in self.VALID_GRADES}
    
    def generate_system_prompt(self) -> str:
        """Generate a system prompt specialized for USDA codes extraction."""
        # Get the USDA codes and descriptions from our mapping
        usda_codes_map = {k: v for k, v in self.get_abbreviation_map().items() 
                         if k.isdigit() or (len(k) > 3 and k[:3].isdigit() and k[3:].isalpha())}
        
        # Format the codes as a full reference for the prompt
        codes_list = "\n".join([f"'{code}': '{desc}'" for code, desc in usda_codes_map.items()])
        
        # Ensure we have a non-empty codes list
        if not codes_list:
            codes_list = "'112A': 'Ribeye Roll'"
            logger.warning("No USDA codes found in abbreviation map, using fallback codes list")
        
        return f"""You are a USDA codes extraction specialist.
Extract USDA code information from product descriptions with high accuracy.
Focus ONLY on identifying USDA codes and related institutional identifiers.

Here is the COMPLETE mapping of USDA codes to their descriptions:
{codes_list}

Return valid JSON with the following schema:
{{
  "usda_code": "string or null"
}}

RETURN NULL UNLESS YOU ARE ABSOLUTELY CERTAIN.

If you find an exact USDA code match in the description (like '120A', '167F', etc.), return that code.
If the description clearly matches one of the USDA code descriptions, return the corresponding code.
Otherwise, return null for usda_code.
"""

    def generate_user_prompt(self, description: str) -> str:
        """Generate a simple user prompt for USDA codes extraction."""
        return f"""Extract ONLY the USDA code from this product description, if one exists.

Description: "{description}"

1. First, look for an EXACT USDA code in the description (like 112A, 120, 193, etc.)
2. If no exact code is found, try to match the description text to a standard USDA cut description
3. If you're not COMPLETELY confident (<95% sure), return null

Return a JSON object with ONLY:
- usda_code: The identified USDA code or null if not found or uncertain

Examples:
Input: "Beef Ribeye Roll 112A Choice 10lb"
Output: {{"usda_code": "112A"}}

Input: "Beef Tenderloin, Full, Side Muscle Off, Defatted"
Output: {{"usda_code": "190"}}

Input: "Generic Meat Product"
Output: {{"usda_code": null}}

Input: "Beef Chuck, might be 116B but not sure"
Output: {{"usda_code": null}}
"""
        

    def parse_llm_output(self, llm_output: str) -> Dict[str, Any]:
        """Parse the LLM output to extract USDA code.
        
        Args:
            llm_output: The raw text response from the LLM
            
        Returns:
            Dict with usda_code key or empty dict
        """
        try:
            # Try to parse as JSON
            # Remove backticks if LLM returns code blocks
            cleaned_output = llm_output.strip('`')
            if cleaned_output.startswith('json'):
                cleaned_output = cleaned_output[4:].strip()
                
            import json
            result = json.loads(cleaned_output)
            return result
        except Exception as e:
            raise ValueError(f"Failed to parse LLM output: {e}")
    
    def extract(self, description: str) -> USDACodeExtractionResult:
        """Extract USDA codes from a product description.
        
        First tries to find a direct match with a USDA code in the description.
        If that fails, it tries to match based on the description text using the LLM.
        Only returns codes with high confidence, otherwise returns null.
        
        Args:
            description: The product description to extract from
            
        Returns:
            USDACodeExtractionResult with extracted USDA code or null
        """
        logger.info(f"USDA Codes extractor received: {description[:50]}...")
        
        # Return empty result for empty description
        if not description or not str(description).strip():
            logger.warning("Empty description provided for USDA code extraction")
            return USDACodeExtractionResult()
        
        # First try the rule-based method from base extractor
        extracted_code = self.extract_usda_code(description)
        
        # If we found a code through the rule-based method, return it with high confidence
        if extracted_code:
            # Direct matches have highest confidence
            # Check if this was a direct USDA code match (numbers only or numbers + letter format)
            words = description.split()
            for word in words:
                clean_word = re.sub(r'[^0-9A-Za-z]', '', word)
                if clean_word == extracted_code:
                    # Direct match in the text has highest confidence
                    logger.info(f"Found direct USDA code match: {extracted_code}")
                    return USDACodeExtractionResult(usda_code=extracted_code)
            
            # If we get here, it was found through description matching
            # We'll consider this high confidence too
            logger.info(f"Found USDA code through description match: {extracted_code}")
            return USDACodeExtractionResult(usda_code=extracted_code)
            
        # If rule-based method failed, try the LLM approach
        # This falls back to LLM reasoning for more complex cases
        try:
            system_prompt = self.generate_system_prompt()
            user_prompt = self.generate_user_prompt(description)
            
            # Call the LLM through the base extractor's method
            result = self.call_llm(system_prompt, user_prompt)
            
            # Parse the LLM response and extract the USDA code
            parsed_result = self.parse_llm_output(result)
            
            if parsed_result and parsed_result.get("usda_code"):
                logger.info(f"Found USDA code through LLM: {parsed_result['usda_code']}")
                return USDACodeExtractionResult(usda_code=parsed_result["usda_code"])
            else:
                logger.info("LLM returned null or invalid USDA code")
        except Exception as e:
            logger.error(f"Error in LLM extraction: {e}")
        
        # If we get here, no code was found with high confidence
        logger.info("No USDA code found with high confidence")
        return USDACodeExtractionResult(usda_code=None)

# Convenience function for direct extraction
def extract_usda_code(description: str) -> Optional[str]:
    """Extract USDA code from a product description.
    
    Uses the USDACodesExtractor to extract a USDA code from a product description.
    
    Args:
        description: The product description to extract from
        
    Returns:
        The extracted USDA code or None if not found
    """
    extractor = USDACodesExtractor()
    result = extractor.extract(description)
    return result.usda_code
