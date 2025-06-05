"""
Tests for the DynamicBeefExtractor module.

Validates the beef extraction functionality across different primal cuts
using the dynamic prompt approach.
"""

import json
import os
import unittest
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest

from src.LLM.extractors.dynamic_beef_extractor import DynamicBeefExtractor
from src.LLM.models import ExtractionResult


class TestDynamicBeefExtractor(unittest.TestCase):
    """Test suite for DynamicBeefExtractor."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Mock OpenAI and related components
        self.openai_patcher = patch('src.LLM.extractors.dynamic_beef_extractor.OpenAI')
        self.mock_openai = self.openai_patcher.start()
        
        # Mock reference data loader
        self.ref_data_patcher = patch('src.LLM.extractors.dynamic_beef_extractor.ReferenceDataLoader')
        self.mock_ref_data_class = self.ref_data_patcher.start()
        self.mock_ref_data = MagicMock()
        self.mock_ref_data_class.return_value = self.mock_ref_data
        
        # Configure mock reference data
        self.mock_ref_data.get_primals.return_value = ["Chuck", "Loin", "Rib"]
        self.mock_ref_data.get_subprimals.return_value = ["Chuck Roll", "Chuck Blade"]
        self.mock_ref_data.get_all_subprimal_terms.return_value = {
            "Chuck Roll", "Chuck Blade", "CR", "CB", "Blade"
        }
        
        # Mock prompt generator
        self.prompt_gen_patcher = patch('src.LLM.extractors.dynamic_beef_extractor.DynamicPromptGenerator')
        self.mock_prompt_gen_class = self.prompt_gen_patcher.start()
        self.mock_prompt_gen = MagicMock()
        self.mock_prompt_gen_class.return_value = self.mock_prompt_gen
        
        # Configure mock prompt generator
        self.mock_prompt_gen.generate_system_prompt.return_value = "System prompt for {primal}"
        self.mock_prompt_gen.generate_user_prompt.return_value = "User prompt for {primal} with {description}"
        self.mock_prompt_gen.get_post_processing_rules.return_value = {
            "grade_regex_patterns": [
                (r'\bprime\b', 'Prime'),
                (r'\bchoice\b', 'Choice')
            ],
            "size_regex_pattern": r'(\d+(?:\.\d+)?)\s*(oz|lb|#|g|kg)\b',
            "brand_keywords": ["angus", "certified"]
        }
        
        # Mock OpenAI client and response
        self.mock_client = MagicMock()
        self.mock_openai.return_value = self.mock_client
        
        self.mock_response = MagicMock()
        self.mock_choice = MagicMock()
        self.mock_message = MagicMock()
        self.mock_message.content = json.dumps({
            "species": "Beef",
            "primal": "Chuck",
            "subprimal": "Chuck Roll",
            "grade": "Choice",
            "size": 10,
            "size_uom": "#",
            "brand": "Certified Angus"
        })
        self.mock_choice.message = self.mock_message
        self.mock_response.choices = [self.mock_choice]
        
        self.mock_client.chat.completions.create.return_value = self.mock_response
        
        # Create extractor with mocks
        with patch.dict(os.environ, {"OPENAI_API_KEY": "mock-api-key", "OPENAI_MODEL": "gpt-3.5-turbo"}):
            self.extractor = DynamicBeefExtractor("mock_path.xlsx", "mock_processed_dir")
    
    def tearDown(self):
        """Clean up after each test method."""
        self.openai_patcher.stop()
        self.ref_data_patcher.stop()
        self.prompt_gen_patcher.stop()
    
    def test_initialization(self):
        """Test extractor initialization."""
        # Verify reference data loader was initialized
        self.mock_ref_data_class.assert_called_once_with("mock_path.xlsx")
        
        # Verify prompt generator was initialized
        self.mock_prompt_gen_class.assert_called_once_with(self.mock_ref_data)
        
        # Verify supported primals were loaded
        self.assertEqual(self.extractor.supported_primals, ["Chuck", "Loin", "Rib"])
        
    def test_extract_with_explicit_primal(self):
        """Test extraction with explicitly provided primal."""
        # Set up test data
        description = "Beef Chuck Roll 10# Choice"
        primal = "Chuck"
        
        # Call extract method
        result = self.extractor.extract(description, primal)
        
        # Verify prompt generation calls
        self.mock_prompt_gen.generate_system_prompt.assert_called_once_with(primal)
        self.mock_prompt_gen.generate_user_prompt.assert_called_once_with(primal, description)
        
        # Verify OpenAI API call
        self.mock_client.chat.completions.create.assert_called_once()
        call_args = self.mock_client.chat.completions.create.call_args[1]
        self.assertEqual(call_args["model"], "gpt-3.5-turbo")
        self.assertEqual(len(call_args["messages"]), 2)
        
        # Verify successful extraction result
        self.assertTrue(result.successful)
        self.assertEqual(result.description, description)
        self.assertEqual(result.primal, primal)
        self.assertEqual(result.extracted_data["subprimal"], "Chuck Roll")
        self.assertEqual(result.extracted_data["grade"], "Choice")
        self.assertEqual(result.extracted_data["size"], 10)
        self.assertEqual(result.extracted_data["size_uom"], "#")
        self.assertEqual(result.extracted_data["brand"], "Certified Angus")
    
    def test_extract_with_inferred_primal(self):
        """Test extraction with primal inference."""
        # Configure mock for _infer_primal_cut
        with patch.object(self.extractor, '_infer_primal_cut', return_value="Chuck") as mock_infer:
            # Set up test data
            description = "Beef Chuck Roll 10# Choice"
            
            # Call extract method without primal
            result = self.extractor.extract(description)
            
            # Verify primal inference was called
            mock_infer.assert_called_once_with(description)
            
            # Verify prompt generation calls with inferred primal
            self.mock_prompt_gen.generate_system_prompt.assert_called_once_with("Chuck")
            self.mock_prompt_gen.generate_user_prompt.assert_called_once_with("Chuck", description)
            
            # Verify successful extraction result with inferred primal
            self.assertTrue(result.successful)
            self.assertEqual(result.primal, "Chuck")
    
    def test_extract_with_failed_primal_inference(self):
        """Test extraction when primal inference fails."""
        # Configure mock for _infer_primal_cut
        with patch.object(self.extractor, '_infer_primal_cut', return_value=None) as mock_infer:
            # Set up test data
            description = "Beef some unknown cut 10#"
            
            # Call extract method without primal
            result = self.extractor.extract(description)
            
            # Verify primal inference was called
            mock_infer.assert_called_once_with(description)
            
            # Verify prompt generation calls with Generic primal
            self.mock_prompt_gen.generate_system_prompt.assert_called_once_with("Generic")
            self.mock_prompt_gen.generate_user_prompt.assert_called_once_with("Generic", description)
            
            # Verify successful extraction result with Generic primal
            self.assertTrue(result.successful)
            self.assertEqual(result.primal, "Generic")
    
    def test_infer_primal_cut(self):
        """Test primal cut inference from description."""
        # Test cases with expected results
        test_cases = [
            ("Beef Chuck Roll 10#", "Chuck"),
            ("Prime Loin Steak", "Loin"),
            ("Rib Eye Choice Cut", "Rib"),
            ("Some unknown beef", None),
        ]
        
        for description, expected_primal in test_cases:
            inferred = self.extractor._infer_primal_cut(description)
            self.assertEqual(inferred, expected_primal, f"Failed to infer primal from '{description}'")
    
    def test_extract_batch(self):
        """Test batch extraction."""
        # Set up test data
        descriptions = [
            "Beef Chuck Roll 10# Choice",
            "Beef Loin Strip 8oz Prime"
        ]
        primal = "Chuck"  # Only used for first description
        
        # Mock extract method to return controlled results
        with patch.object(self.extractor, 'extract') as mock_extract:
            # Configure mock to return different results for different inputs
            def side_effect(desc, prim=None, **kwargs):
                if "Chuck" in desc:
                    return ExtractionResult(
                        description=desc,
                        extracted_data={"primal": "Chuck", "subprimal": "Chuck Roll"},
                        primal="Chuck",
                        successful=True,
                        error=None
                    )
                else:
                    return ExtractionResult(
                        description=desc,
                        extracted_data={"primal": "Loin", "subprimal": "Strip"},
                        primal="Loin",
                        successful=True,
                        error=None
                    )
            
            mock_extract.side_effect = side_effect
            
            # Call extract_batch
            results = self.extractor.extract_batch(descriptions, primal)
            
            # Verify extract was called for each description
            self.assertEqual(mock_extract.call_count, 2)
            mock_extract.assert_any_call(descriptions[0], primal, **{})
            mock_extract.assert_any_call(descriptions[1], primal, **{})
            
            # Verify results
            self.assertEqual(len(results), 2)
            self.assertEqual(results[0].description, descriptions[0])
            self.assertEqual(results[1].description, descriptions[1])
            
    def test_api_error_handling(self):
        """Test handling of API errors."""
        # Configure mock to raise an exception
        self.mock_client.chat.completions.create.side_effect = Exception("API error")
        
        # Set up test data
        description = "Beef Chuck Roll 10# Choice"
        primal = "Chuck"
        
        # Call extract method
        result = self.extractor.extract(description, primal)
        
        # Verify error handling
        self.assertFalse(result.successful)
        self.assertEqual(result.error, "API error")
        self.assertEqual(result.extracted_data, {})
        
    def test_json_parse_error_handling(self):
        """Test handling of JSON parsing errors."""
        # Configure mock to return invalid JSON
        mock_message = MagicMock()
        mock_message.content = "This is not valid JSON"
        mock_choice = MagicMock()
        mock_choice.message = mock_message
        mock_response = MagicMock()
        mock_response.choices = [mock_choice]
        self.mock_client.chat.completions.create.return_value = mock_response
        
        # Set up test data
        description = "Beef Chuck Roll 10# Choice"
        primal = "Chuck"
        
        # Call extract method
        result = self.extractor.extract(description, primal)
        
        # Verify error handling
        self.assertFalse(result.successful)
        self.assertIn("No JSON found in response", result.error)
        self.assertEqual(result.extracted_data, {})

    def test_post_process_result(self):
        """Test post-processing of extraction results."""
        # Set up test data
        result = {
            "species": "Beef",
            "primal": "Chuck",
            "subprimal": "Chuck Roll",
            "grade": None,
            "size": None,
            "size_uom": None,
            "brand": None
        }
        description = "Beef Chuck Roll 10# Choice Certified Angus"
        rules = {
            "grade_regex_patterns": [
                (r'\bchoice\b', 'Choice'),
                (r'\bprime\b', 'Prime')
            ],
            "size_regex_pattern": r'(\d+(?:\.\d+)?)\s*(oz|lb|#|g|kg)\b',
            "brand_keywords": ["certified", "angus"]
        }
        
        # Call post-processing method
        processed = self.extractor._post_process_result(result, description, rules)
        
        # Verify post-processing
        self.assertEqual(processed["grade"], "Choice")
        self.assertEqual(processed["size"], 10.0)
        self.assertEqual(processed["size_uom"], "#")
        
    def test_get_supported_primals(self):
        """Test retrieval of supported primals."""
        primals = self.extractor.get_supported_primals()
        self.assertEqual(primals, ["Chuck", "Loin", "Rib"])
        self.mock_ref_data.get_primals.assert_called_once()
        
    def test_generate_cache_key(self):
        """Test cache key generation."""
        # Test with description only
        key1 = self.extractor._generate_cache_key("Beef Chuck Roll")
        self.assertEqual(key1, "Beef Chuck Roll")
        
        # Test with description and primal
        key2 = self.extractor._generate_cache_key("Beef Chuck Roll", "Chuck")
        self.assertEqual(key2, "Beef Chuck Roll_Chuck")
        
        # Verify different keys for different inputs
        self.assertNotEqual(key1, key2)


if __name__ == "__main__":
    unittest.main()
