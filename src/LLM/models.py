"""
LLM Extraction Models

Defines data models used in the LLM extraction pipeline.
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional, List


@dataclass
class ExtractionResult:
    """
    Represents the result of an extraction operation.
    
    Contains both the extracted data and metadata about the extraction process.
    """
    
    description: str
    extracted_data: Dict[str, Any]
    primal: Optional[str] = None
    successful: bool = True
    error: Optional[str] = None
