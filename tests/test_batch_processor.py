"""
Unit tests for BatchProcessor class.
Tests batch processing functionality, caching, and error handling for product descriptions.
"""
import os
import sys
import unittest
import tempfile
import json
from unittest.mock import patch, MagicMock
import pandas as pd

# Add src to sys.path for proper imports
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src'))

from src.llm_extraction.batch_processor import BatchProcessor
from src.llm_extraction.base_extractor import BaseLLMExtractor, ExtractionResult

class MockExtractor(BaseLLMExtractor):
    """Mock extractor for testing."""
    
    def __init__(self, category_name="Beef"):
        self.category_name = category_name
        self.extraction_calls = []
        
    def get_subprimal_mapping(self):
        return {"Test": ["test", "sample"]}
        
    def get_category_name(self):
        return self.category_name
        
    def extract(self, description, **kwargs):
        """Record calls and return mock results."""
        self.extraction_calls.append(description)
        
        if description is None or not description.strip():
            return ExtractionResult(
                needs_review=True,
                confidence=0.0
            )
            
        # Simple mock extraction logic
        return ExtractionResult(
            subprimal="Test Subprimal",
            grade="Choice" if "choice" in description.lower() else "No Grade",
            size=12.0 if "12" in description else None,
            size_uom="oz" if "oz" in description.lower() else None,
            brand="Test Brand" if "brand" in description.lower() else None,
            bone_in="bone-in" in description.lower(),
            confidence=0.9,
            needs_review=False
        )


class TestBatchProcessor(unittest.TestCase):
    """Test cases for BatchProcessor."""

    def setUp(self):
        """Set up test resources."""
        # Create temp file for cache
        self.temp_cache = tempfile.NamedTemporaryFile(delete=False)
        self.temp_cache.close()
        
        # Create mock extractors
        self.mock_beef_extractor = MockExtractor("Beef")
        self.mock_pork_extractor = MockExtractor("Pork")
        
        # Create batch processor
        self.extractors = {
            "Beef": self.mock_beef_extractor,
            "Pork": self.mock_pork_extractor
        }
        self.processor = BatchProcessor(self.extractors, cache_file=self.temp_cache.name)
        
        # Sample data
        self.test_data = pd.DataFrame({
            'product_code': ['001', '002', '003', '004', '005'],
            'product_description': ['Valid beef choice', 'Another valid', None, '', 'Brand beef 12 oz'],
            'category_description': ['Beef', 'Beef', 'Beef', 'Beef', 'Beef']
        })
        
    def tearDown(self):
        """Clean up resources."""
        os.unlink(self.temp_cache.name)

    def test_process_single_record_handles_null_description(self):
        """Test that _process_single_record handles null descriptions correctly."""
        # Test with null description
        record = {'product_code': '003', 'product_description': None, 'category_description': 'Beef'}
        result = self.processor._process_single_record(record, 'Beef')
        
        # Should return default values without calling extractor
        self.assertIsNone(result['subprimal'])
        self.assertTrue(result['needs_review'])
        self.assertEqual(result['confidence'], 0.0)
        self.assertEqual(len(self.mock_beef_extractor.extraction_calls), 0)
        
        # Test with empty string
        record = {'product_code': '004', 'product_description': '', 'category_description': 'Beef'}
        result = self.processor._process_single_record(record, 'Beef')
        
        # Should return default values without calling extractor
        self.assertIsNone(result['subprimal'])
        self.assertTrue(result['needs_review'])
        self.assertEqual(result['confidence'], 0.0)
        self.assertEqual(len(self.mock_beef_extractor.extraction_calls), 0)

    def test_process_single_record_with_valid_description(self):
        """Test that _process_single_record processes valid descriptions correctly."""
        record = {'product_code': '001', 'product_description': 'Valid beef choice', 'category_description': 'Beef'}
        result = self.processor._process_single_record(record, 'Beef')
        
        # Should call extractor and return valid results
        self.assertEqual(result['subprimal'], 'Test Subprimal')
        self.assertEqual(result['grade'], 'Choice')
        self.assertFalse(result['needs_review'])
        self.assertEqual(len(self.mock_beef_extractor.extraction_calls), 1)
        self.assertEqual(self.mock_beef_extractor.extraction_calls[0], 'Valid beef choice')

    def test_batch_process_with_mixed_descriptions(self):
        """Test batch processing with mixed valid and null descriptions."""
        # Process sample dataset
        result_df = self.processor.process_batch(self.test_data, 'category_description')
        
        # Should have same number of rows as input
        self.assertEqual(len(result_df), len(self.test_data))
        
        # Check individual results
        self.assertEqual(result_df.iloc[0]['subprimal'], 'Test Subprimal')  # Valid
        self.assertEqual(result_df.iloc[1]['subprimal'], 'Test Subprimal')  # Valid
        self.assertIsNone(result_df.iloc[2]['subprimal'])  # Null
        self.assertIsNone(result_df.iloc[3]['subprimal'])  # Empty
        self.assertEqual(result_df.iloc[4]['subprimal'], 'Test Subprimal')  # Valid
        
        # Check needs_review flag is set correctly
        self.assertFalse(result_df.iloc[0]['needs_review'])  # Valid
        self.assertFalse(result_df.iloc[1]['needs_review'])  # Valid
        self.assertTrue(result_df.iloc[2]['needs_review'])   # Null
        self.assertTrue(result_df.iloc[3]['needs_review'])   # Empty
        self.assertFalse(result_df.iloc[4]['needs_review'])  # Valid
        
        # Should only call extractor for valid descriptions (3 valid out of 5)
        self.assertEqual(len(self.mock_beef_extractor.extraction_calls), 3)

    def test_cache_functionality(self):
        """Test that caching works correctly."""
        # Process once
        self.processor.process(self.test_data, 'category_description')
        extraction_calls_first = len(self.mock_beef_extractor.extraction_calls)
        
        # Reset call count
        self.mock_beef_extractor.extraction_calls = []
        
        # Process again - should use cache
        self.processor.process(self.test_data, 'category_description')
        
        # Should not make any new extraction calls
        self.assertEqual(len(self.mock_beef_extractor.extraction_calls), 0)
        
        # Add new record
        new_data = pd.DataFrame({
            'product_code': ['006'],
            'product_description': ['New description'],
            'category_description': ['Beef']
        })
        
        # Process with new record
        combined_data = pd.concat([self.test_data, new_data], ignore_index=True)
        self.processor.process_batch(combined_data, 'category_description')
        
        # Should only make one new call for the new record
        self.assertEqual(len(self.mock_beef_extractor.extraction_calls), 1)
        self.assertEqual(self.mock_beef_extractor.extraction_calls[0], 'New description')


if __name__ == '__main__':
    unittest.main()
