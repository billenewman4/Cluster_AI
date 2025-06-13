"""
LLM Extraction Package
Handles LLM-based extraction of meat attributes using a modular architecture.
"""

from .base_extractor import BaseLLMExtractor, ExtractionResult
from .batch_processor import BatchProcessor

__all__ = ['BaseLLMExtractor', 'ExtractionResult', 'BatchProcessor']