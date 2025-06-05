"""
Tests for the ExtractionController module.

Validates the controller's ability to orchestrate extraction across
various primal cuts using the dynamic beef extractor.
"""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.LLM.extraction_controller import ExtractionController
from src.LLM.models import ExtractionResult


class TestExtractionController(unittest.TestCase):
    """Test suite for ExtractionController."""

    def setUp(self):
        """Set up test fixtures before each test."""
        # Mock dependencies
        self.reference_data_patcher = patch('src.LLM.extraction_controller.ReferenceDataLoader')
        self.mock_ref_data_class = self.reference_data_patcher.start()
        self.mock_ref_data = MagicMock()
        self.mock_ref_data_class.return_value = self.mock_ref_data
        
        # Configure mock reference data
        self.mock_ref_data.get_primals.return_value = ["Chuck", "Loin", "Rib"]
        
        # Mock DynamicBeefExtractor
        self.dynamic_extractor_patcher = patch('src.LLM.extraction_controller.DynamicBeefExtractor')
        self.mock_dynamic_extractor_class = self.dynamic_extractor_patcher.start()
        self.mock_dynamic_extractor = MagicMock()
        self.mock_dynamic_extractor_class.return_value = self.mock_dynamic_extractor
        
        # Configure mock extractor
        self.mock_dynamic_extractor.get_supported_primals.return_value = ["Chuck", "Loin", "Rib"]
        
        # Create controller with mocks
        self.controller = ExtractionController("mock/processed/dir", "mock/reference/data.xlsx")
        
    def tearDown(self):
        """Clean up after each test."""
        self.reference_data_patcher.stop()
        self.dynamic_extractor_patcher.stop()
        
    def test_initialization(self):
        """Test controller initialization."""
        # Verify reference data was loaded
        self.mock_ref_data_class.assert_called_once_with("mock/reference/data.xlsx")
        
        # Verify dynamic extractor was created
        self.mock_dynamic_extractor_class.assert_called_once_with(
            "mock/reference/data.xlsx", "mock/processed/dir")
        
        # Verify category extractors were mapped
        self.assertEqual(len(self.controller.category_extractors), 3)  # Chuck, Loin, Rib
        self.assertIn("Beef Chuck", self.controller.category_extractors)
        self.assertIn("Beef Loin", self.controller.category_extractors)
        self.assertIn("Beef Rib", self.controller.category_extractors)
        
        # Verify all categories map to the dynamic extractor
        for category, extractor in self.controller.category_extractors.items():
            self.assertEqual(extractor, self.mock_dynamic_extractor)
            
    def test_extract_single_known_category(self):
        """Test extraction for a single item with known category."""
        # Configure mock extractor response
        mock_result = MagicMock()
        mock_result.successful = True
        mock_result.extracted_data = {"primal": "Chuck", "subprimal": "Chuck Roll"}
        self.mock_dynamic_extractor.extract.return_value = mock_result
        
        # Call extract_single with a known category
        result = self.controller.extract_single("Beef Chuck Roll 10#", "Beef Chuck")
        
        # Verify extractor was called with the right parameters
        self.mock_dynamic_extractor.extract.assert_called_once_with(
            "Beef Chuck Roll 10#", primal="Chuck")
            
        # Verify result
        self.assertEqual(result, {"primal": "Chuck", "subprimal": "Chuck Roll"})
        
    def test_extract_single_unknown_category(self):
        """Test extraction for a single item with unknown category."""
        # Configure mock extractor response
        mock_result = MagicMock()
        mock_result.successful = True
        mock_result.extracted_data = {"primal": "Unknown", "subprimal": "Unknown Cut"}
        self.mock_dynamic_extractor.extract.return_value = mock_result
        
        # Call extract_single with an unknown category
        result = self.controller.extract_single("Some beef product", "Unknown Category")
        
        # Verify extractor was called without a primal hint
        self.mock_dynamic_extractor.extract.assert_called_once_with("Some beef product")
            
        # Verify result
        self.assertEqual(result, {"primal": "Unknown", "subprimal": "Unknown Cut"})
        
    def test_extract_single_failure(self):
        """Test extraction failure handling."""
        # Configure mock extractor response for failure
        mock_result = MagicMock()
        mock_result.successful = False
        mock_result.error = "Extraction failed"
        self.mock_dynamic_extractor.extract.return_value = mock_result
        
        # Call extract_single
        result = self.controller.extract_single("Beef Chuck Roll 10#", "Beef Chuck")
        
        # Verify empty result on failure
        self.assertEqual(result, {})
            
    def test_extract_batch(self):
        """Test batch extraction."""
        # Create test DataFrame
        data = {
            'Description': ['Beef Chuck Roll 10#', 'Beef Loin Strip 8oz', 'Unknown Product'],
            'Category': ['Beef Chuck', 'Beef Loin', 'Unknown Category']
        }
        test_df = pd.DataFrame(data)
        
        # Configure mock for extract_batch results
        def mock_extract_batch(descriptions, primal=None):
            results = []
            for desc in descriptions:
                if 'Chuck' in desc:
                    result = ExtractionResult(
                        description=desc,
                        extracted_data={"primal": "Chuck", "subprimal": "Chuck Roll"},
                        primal="Chuck",
                        successful=True
                    )
                elif 'Loin' in desc:
                    result = ExtractionResult(
                        description=desc,
                        extracted_data={"primal": "Loin", "subprimal": "Strip"},
                        primal="Loin",
                        successful=True
                    )
                else:
                    result = ExtractionResult(
                        description=desc,
                        extracted_data={},
                        primal=None,
                        successful=False,
                        error="Unknown product"
                    )
                results.append(result)
            return results
            
        self.mock_dynamic_extractor.extract_batch.side_effect = mock_extract_batch
        
        # Call extract_batch
        result_df = self.controller.extract_batch(test_df)
        
        # Verify result DataFrame
        self.assertEqual(len(result_df), 3)
        
        # Check successful extractions
        chuck_row = result_df[result_df['Description'] == 'Beef Chuck Roll 10#'].iloc[0]
        self.assertTrue(chuck_row['Success'])
        self.assertEqual(chuck_row['Category'], 'Beef Chuck')
        self.assertEqual(chuck_row['Primal'], 'Chuck')
        
        loin_row = result_df[result_df['Description'] == 'Beef Loin Strip 8oz'].iloc[0]
        self.assertTrue(loin_row['Success'])
        self.assertEqual(loin_row['Category'], 'Beef Loin')
        self.assertEqual(loin_row['Primal'], 'Loin')
        
        # Check failed extraction
        unknown_row = result_df[result_df['Description'] == 'Unknown Product'].iloc[0]
        self.assertFalse(unknown_row['Success'])
        self.assertEqual(unknown_row['Category'], 'Unknown Category')
        self.assertIsNone(unknown_row['Primal'])
        self.assertEqual(unknown_row['Error'], 'Unknown product')
    
    def test_run_extraction(self):
        """Test run_extraction method."""
        # Mock process_category method
        with patch.object(self.controller.dynamic_beef_extractor, 'process_category') as mock_process:
            # Configure mock to return a DataFrame for each category
            chuck_df = pd.DataFrame({'description': ['Beef Chuck Test']})
            loin_df = pd.DataFrame({'description': ['Beef Loin Test']})
            
            mock_process.side_effect = lambda category: chuck_df if 'Chuck' in category else loin_df
            
            # Call run_extraction with specific categories
            results = self.controller.run_extraction(['Beef Chuck', 'Beef Loin'])
            
            # Verify process_category was called for each category
            self.assertEqual(mock_process.call_count, 2)
            mock_process.assert_any_call('Beef Chuck')
            mock_process.assert_any_call('Beef Loin')
            
            # Verify results
            self.assertEqual(len(results), 2)
            self.assertIn('Beef Chuck', results)
            self.assertIn('Beef Loin', results)
            self.assertEqual(len(results['Beef Chuck']), 1)
            self.assertEqual(len(results['Beef Loin']), 1)


if __name__ == "__main__":
    unittest.main()
