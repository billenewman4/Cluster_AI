"""
Unit tests for BeefExtractor class.
Tests proper extraction logic, dataclass handling, and serialization of extraction results.
"""
import os
import sys
import unittest
import json
from unittest.mock import patch, MagicMock
from typing import Dict, Any

# Add src to sys.path for proper imports
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src'))

from src.extractors.beef_extractor import BeefExtractor
from src.extractors.base_extractor import ExtractionResult

class TestBeefExtractor(unittest.TestCase):
    """Test cases for BeefExtractor."""

    def setUp(self):
        """Set up test resources."""
        # Use patch to avoid loading actual reference data
        patcher = patch('src.extractors.beef_extractor.ReferenceDataLoader')
        mock_loader_class = patcher.start()
        self.mock_loader = mock_loader_class.return_value
        self.mock_loader.get_primals.return_value = ['Chuck', 'Rib', 'Loin', 'Round', 'Brisket']
        
        self.addCleanup(patcher.stop)
        
        # Create extractor with mock reference data
        self.extractor = BeefExtractor(reference_data_path="mocked_path")
        self.extractor.set_primal("Chuck")
        
        # Mock OpenAI client
        patcher_client = patch('src.extractors.beef_extractor.OpenAI')
        self.mock_client_class = patcher_client.start()
        self.mock_client = self.mock_client_class.return_value
        self.extractor.client = self.mock_client
        
        self.addCleanup(patcher_client.stop)
        
    def test_null_description_handling(self):
        """Test that null descriptions are handled correctly with default values."""
        # Test None description
        result = self.extractor.extract(None)
        self.assertTrue(result.needs_review)
        self.assertEqual(result.confidence, 0.0)
        self.assertIsNone(result.subprimal)
        
        # Test empty string
        result = self.extractor.extract("")
        self.assertTrue(result.needs_review)
        self.assertEqual(result.confidence, 0.0)
        self.assertIsNone(result.subprimal)
        
        # Test whitespace
        result = self.extractor.extract("   ")
        self.assertTrue(result.needs_review)
        self.assertEqual(result.confidence, 0.0)
        self.assertIsNone(result.subprimal)

    @patch('src.extractors.beef_extractor.json')
    def test_extraction_result_serialization(self, mock_json):
        """Test that extraction results are properly serialized."""
        # Mock successful API response
        mock_response = MagicMock()
        mock_response.choices[0].message.content = '{"subprimal": "Chuck Eye", "grade": "Choice", "weight": 12, "unit": "oz", "bone_in": false, "brand": "Test Brand", "confidence": 0.95, "needs_review": false}'
        self.mock_client.chat.completions.create.return_value = mock_response
        
        # Mock json.loads to return the actual parsed JSON
        mock_json.loads.return_value = {
            "subprimal": "Chuck Eye",
            "grade": "Choice",
            "weight": 12,
            "unit": "oz",
            "bone_in": False,
            "brand": "Test Brand",
            "confidence": 0.95,
            "needs_review": False
        }
        
        # Call extract with valid description
        result = self.extractor.extract("BEEF CHUCK EYE STEAK CHOICE 12 OZ")
        
        # Verify result is created properly
        self.assertIsInstance(result, ExtractionResult)
        self.assertEqual(result.subprimal, "Chuck Eye")
        self.assertEqual(result.grade, "Choice")
        self.assertEqual(result.size, 12)
        self.assertEqual(result.size_uom, "oz")
        self.assertEqual(result.brand, "Test Brand")
        self.assertEqual(result.confidence, 0.95)
        self.assertFalse(result.needs_review)
        
        # Test serialization to dict
        result_dict = result.to_dict()
        self.assertIsInstance(result_dict, dict)
        self.assertEqual(result_dict["subprimal"], "Chuck Eye")

    def test_extraction_result_creation_from_dict(self):
        """Test creating ExtractionResult from dictionary."""
        test_dict = {
            "subprimal": "Chuck Eye", 
            "grade": "Choice",
            "size": 12.0,
            "size_uom": "oz",
            "bone_in": False,
            "brand": "Test Brand",
            "confidence": 0.95,
            "needs_review": False
        }
        
        result = ExtractionResult.from_dict(test_dict)
        
        self.assertEqual(result.subprimal, "Chuck Eye")
        self.assertEqual(result.grade, "Choice")
        self.assertEqual(result.size, 12.0)
        self.assertEqual(result.size_uom, "oz")
        self.assertEqual(result.brand, "Test Brand")
        self.assertEqual(result.confidence, 0.95)
        self.assertFalse(result.needs_review)

    @patch('src.extractors.beef_extractor.json')
    def test_json_decode_error_handling(self, mock_json):
        """Test handling of JSON decode errors from API response."""
        # Mock API response with invalid JSON
        mock_response = MagicMock()
        mock_response.choices[0].message.content = 'Not valid JSON'
        self.mock_client.chat.completions.create.return_value = mock_response
        
        # Mock json.loads to raise JSONDecodeError
        mock_json.loads.side_effect = json.JSONDecodeError("Invalid JSON", "Not valid JSON", 0)
        
        # Call extract with valid description
        result = self.extractor.extract("BEEF CHUCK EYE STEAK CHOICE 12 OZ")
        
        # Verify error handling
        self.assertTrue(result.needs_review)
        self.assertEqual(result.confidence, 0.0)

    def test_api_exception_handling(self):
        """Test handling of API exceptions."""
        # Mock API call to raise exception
        self.mock_client.chat.completions.create.side_effect = Exception("API Error")
        
        # Call extract with valid description
        result = self.extractor.extract("BEEF CHUCK EYE STEAK CHOICE 12 OZ")
        
        # Verify error handling
        self.assertTrue(result.needs_review)
        self.assertEqual(result.confidence, 0.0)


if __name__ == '__main__':
    unittest.main()
