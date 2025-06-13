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
        """Parse JSON from an LLM response with robust fence handling.
        
        Handles cases where JSON might be embedded in markdown code fences,
        surrounded by other text, or have extra whitespace.
        
        Args:
            response: Raw text response from LLM
            
        Returns:
            Optional[Dict[str, Any]]: Parsed JSON dict or None if parsing failed
        """
        if not response:
            return None
        
        # Step 1: Clean the response - strip common code fence patterns
        clean = response.strip()
        
        # Remove markdown code fences (```json, ```, etc.)
        if clean.startswith('```'):
            # Find the end of the opening fence
            lines = clean.split('\n')
            if len(lines) > 1:
                # Remove first line (```json or ```)
                lines = lines[1:]
                # Remove last line if it's just ```
                if lines and lines[-1].strip() == '```':
                    lines = lines[:-1]
                clean = '\n'.join(lines)
        
        # Strip backticks, whitespace, and newlines
        clean = clean.strip('` \n\r\t')
        
        # Step 2: Validate it looks like JSON before parsing
        if clean.startswith('{') and clean.endswith('}'):
            try:
                return json.loads(clean)
            except json.JSONDecodeError as e:
                logger.debug(f"JSON parsing failed after cleaning: {e}")
        
        # Step 3: Fallback to original regex-based extraction
        try:
            # Try to extract JSON block using regex
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                json_str = json_match.group().strip()
                return json.loads(json_str)
        except (json.JSONDecodeError, AttributeError):
            pass
            
        try:
            # Try to extract from code block markdown (fallback)
            code_block_match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', response)
            if code_block_match:
                json_str = code_block_match.group(1).strip()
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
