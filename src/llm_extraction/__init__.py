"""
LLM Extraction Package
Handles LLM-based extraction of meat attributes using a modular architecture.
"""

from .base_extractor import BaseLLMExtractor, ExtractionResult
from .beef_chuck_extractor import BeefChuckExtractor  
from .batch_processor import BatchProcessor

__all__ = ['BaseLLMExtractor', 'ExtractionResult', 'BeefChuckExtractor', 'BatchProcessor'] 