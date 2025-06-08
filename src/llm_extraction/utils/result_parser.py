"""
Result Parser Module
Handles parsing and validation of LLM API responses.
"""

import re
import json
import logging
from typing import Dict, Optional, Any

# Configure logging
logger = logging.getLogger(__name__)

class ResultParser:
    """Parses and validates LLM API responses."""
    
    @staticmethod
    def parse_json_response(response: str) -> Optional[Dict[str, Any]]:
        """Parse JSON from an LLM response.
        
        Handles cases where JSON might be embedded in markdown or 
        surrounded by other text.
        
        Args:
            response: Raw text response from LLM
            
        Returns:
            Optional[Dict[str, Any]]: Parsed JSON dict or None if parsing failed
        """
        if not response:
            return None
            
        try:
            # First try: direct JSON parsing
            return json.loads(response)
        except json.JSONDecodeError:
            pass
        
        try:
            # Second try: extract JSON block using regex
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                json_str = json_match.group()
                return json.loads(json_str)
        except (json.JSONDecodeError, AttributeError):
            pass
            
        try:
            # Third try: look for code block markdown
            code_block_match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', response)
            if code_block_match:
                json_str = code_block_match.group(1)
                return json.loads(json_str)
        except (json.JSONDecodeError, AttributeError):
            pass
        
        # All parsing attempts failed
        logger.warning(f"Failed to parse JSON from response: {response[:100]}...")
        return None
    
    @staticmethod
    def validate_extraction_fields(parsed_json: Dict[str, Any], required_fields: list) -> bool:
        """Validate that extraction result contains all required fields.
        
        Args:
            parsed_json: Parsed extraction result
            required_fields: List of field names that must be present
            
        Returns:
            bool: True if all required fields are present
        """
        if not parsed_json:
            return False
            
        return all(field in parsed_json for field in required_fields)
