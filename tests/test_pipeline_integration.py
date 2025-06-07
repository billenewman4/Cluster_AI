"""
Integration tests for the beef extraction pipeline.
Tests end-to-end flow from data loading through transformation to extraction.
"""
import os
import sys
import unittest
import pandas as pd
from unittest.mock import patch, MagicMock
import tempfile
import shutil
from pathlib import Path

# Add src to sys.path for proper imports
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src'))

from src.data_ingestion.core.product_transformer import ProductTransformer
from src.llm_extraction.batch_processor import BatchProcessor
from src.extractors.beef_extractor import BeefExtractor


class TestPipelineIntegration(unittest.TestCase):
    """Integration tests for the complete extraction pipeline."""

    def setUp(self):
        """Set up test environment with test data and directories."""
        # Create temp directories
        self.test_dir = tempfile.mkdtemp()
        self.data_dir = os.path.join(self.test_dir, "data")
        self.incoming_dir = os.path.join(self.data_dir, "incoming")
        self.processed_dir = os.path.join(self.data_dir, "processed")
        
        # Create directory structure
        os.makedirs(self.incoming_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        
        # Create test product query CSV file
        self.product_query_file = os.path.join(self.incoming_dir, "Product_Query_2025_06_06.csv")
        self.test_data = pd.DataFrame({
            'product_code': ['001', '002', '003', '004', '005'],
            'product_description': ['Beef Chuck Roast Choice', 'Beef Rib Eye Steak', None, '', 'Beef Round Steak'],
            'description': ['Bone-in 2lb', 'Prime 12oz', 'Missing description', 'Empty desc', 'No additional info'],
            'category_description': ['Beef Chuck', 'Beef Rib', 'Beef Loin', 'Beef Brisket', 'Beef Round']
        })
        self.test_data.to_csv(self.product_query_file, index=False)
        
        # Create mock beef_cuts.xlsx
        self.beef_cuts_file = os.path.join(self.incoming_dir, "beef_cuts.xlsx")
        beef_cuts_data = pd.DataFrame({
            'category': ['Beef Chuck', 'Beef Rib', 'Beef Loin', 'Beef Brisket', 'Beef Round'],
            'subprimal': ['Chuck Eye', 'Ribeye', 'Strip Loin', 'Brisket Flat', 'Top Round'],
        })
        beef_cuts_data.to_excel(self.beef_cuts_file, index=False)
        
        # Set up transformer
        self.transformer = ProductTransformer()
        
        # Mock extractors and processor to avoid actual API calls
        patcher = patch('src.extractors.beef_extractor.OpenAI')
        self.mock_openai = patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.test_dir)

    def mock_api_response(self, mock_content):
        """Helper to create a mock OpenAI API response."""
        mock_response = MagicMock()
        mock_choice = MagicMock()
        mock_message = MagicMock()
        mock_message.content = mock_content
        mock_choice.message = mock_message
        mock_response.choices = [mock_choice]
        return mock_response

    @patch('src.llm_extraction.batch_processor.ThreadPoolExecutor')
    def test_end_to_end_pipeline(self, mock_executor_class):
        """Test the complete data pipeline from loading to extraction."""
        # Set up mock ThreadPoolExecutor to use synchronous execution for testing
        mock_executor = MagicMock()
        mock_executor_class.return_value.__enter__.return_value = mock_executor
        
        def submit_side_effect(fn, *args, **kwargs):
            future = MagicMock()
            future.result.return_value = fn(*args, **kwargs)
            return future
            
        mock_executor.submit.side_effect = submit_side_effect
        
        # Set up mock API responses
        client_mock = self.mock_openai.return_value
        chat_mock = MagicMock()
        client_mock.chat.completions.create = chat_mock
        
        # Configure mock responses for different descriptions
        response_map = {
            'Beef Chuck Roast Choice Bone-in 2lb': self.mock_api_response(
                '{"subprimal": "Chuck Roast", "grade": "Choice", "weight": 2.0, "unit": "lb", "bone_in": true, "brand": null, "confidence": 0.95, "needs_review": false}'
            ),
            'Beef Rib Eye Steak Prime 12oz': self.mock_api_response(
                '{"subprimal": "Ribeye", "grade": "Prime", "weight": 12.0, "unit": "oz", "bone_in": false, "brand": null, "confidence": 0.98, "needs_review": false}'
            ),
            'Beef Round Steak No additional info': self.mock_api_response(
                '{"subprimal": "Top Round", "grade": "No Grade", "weight": null, "unit": null, "bone_in": false, "brand": null, "confidence": 0.85, "needs_review": true}'
            )
        }
        
        chat_mock.side_effect = lambda model, messages, response_format: response_map.get(
            messages[0]['content'].split('Description:')[-1].strip().split('\n')[0], 
            self.mock_api_response('{"subprimal": null, "grade": null, "weight": null, "unit": null, "bone_in": false, "brand": null, "confidence": 0.0, "needs_review": true}')
        )
        
        # STEP 1: Load and transform product data
        transformed_data = self.transformer.transform(self.test_data)
        
        # Verify transformation step
        self.assertEqual(len(transformed_data), 5)  # All rows should be preserved
        self.assertTrue(all(pd.notna(transformed_data['product_description'])))  # No NaNs after merge
        
        # Verify merged descriptions
        self.assertEqual(transformed_data.iloc[0]['product_description'], 'Beef Chuck Roast Choice Bone-in 2lb')
        self.assertEqual(transformed_data.iloc[1]['product_description'], 'Beef Rib Eye Steak Prime 12oz')
        
        # STEP 2: Initialize extractor with test reference data
        beef_extractor = BeefExtractor(reference_data_path=self.beef_cuts_file, processed_dir=self.processed_dir)
        extractors = {
            "Beef Chuck": beef_extractor,
            "Beef Rib": beef_extractor,
            "Beef Loin": beef_extractor,
            "Beef Brisket": beef_extractor,
            "Beef Round": beef_extractor
        }
        
        # STEP 3: Set up batch processor
        batch_processor = BatchProcessor(
            extractors=extractors,
            cache_file=os.path.join(self.processed_dir, ".extraction_cache.json")
        )
        
        # STEP 4: Process the transformed data
        processed_data = batch_processor.process(transformed_data, 'category_description')
        
        # STEP 5: Verify extraction results
        self.assertEqual(len(processed_data), 5)  # All rows processed
        
        # Check valid descriptions were processed correctly
        self.assertEqual(processed_data.iloc[0]['subprimal'], 'Chuck Roast')  # Row with valid description
        self.assertEqual(processed_data.iloc[1]['subprimal'], 'Ribeye')        # Row with valid description
        
        # Check null/empty descriptions were properly handled
        self.assertTrue(processed_data.iloc[2]['needs_review'])  # Row with null description
        self.assertTrue(processed_data.iloc[3]['needs_review'])  # Row with empty description
        
        # Verify that confidence scores are set correctly
        self.assertEqual(processed_data.iloc[0]['confidence'], 0.95)  # High confidence
        self.assertEqual(processed_data.iloc[2]['confidence'], 0.0)   # Null description = zero confidence

    @patch('sys.exit')
    @patch('builtins.print')
    def test_pipeline_fails_with_invalid_data(self, mock_print, mock_exit):
        """Test that pipeline fails appropriately with invalid data."""
        # Create test data with only invalid descriptions
        invalid_data = pd.DataFrame({
            'product_code': ['001', '002'],
            'product_description': [None, ''],
            'description': [None, ''],
            'category_description': ['Beef Chuck', 'Beef Rib']
        })
        
        # Save invalid data to a test file
        invalid_file = os.path.join(self.incoming_dir, "Invalid_Product_Query.csv")
        invalid_data.to_csv(invalid_file, index=False)
        
        # Try to process the invalid data - should raise ValueError
        with self.assertRaises(ValueError):
            self.transformer.transform(invalid_data)
        
        # Alternatively, if we're testing a script that calls sys.exit on error:
        # process_product_data(invalid_data)  # This would call sys.exit in production code
        # mock_exit.assert_called()  # Verify that sys.exit was called


if __name__ == '__main__':
    unittest.main()
