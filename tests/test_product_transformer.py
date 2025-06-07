"""
Unit tests for ProductTransformer class and related functionality.
Tests proper handling of product descriptions, column validation, and data transformation.
"""
import os
import sys
import unittest
import pandas as pd
from unittest.mock import patch, MagicMock
import numpy as np

# Add src to sys.path for proper imports
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src'))

from data_ingestion.core.product_transformer import ProductTransformer

class TestProductTransformer(unittest.TestCase):
    """Test cases for ProductTransformer and related functions."""

    def setUp(self):
        """Set up test data."""
        # Sample test data with various description scenarios
        self.test_data = pd.DataFrame({
            'product_code': ['001', '002', '003', '004', '005'],
            'product_description': ['Valid description', 'Another valid', np.nan, '', 'Final valid'],
            'description': ['Extra info', np.nan, 'Only in description col', 'Only in description col', 'More detail'],
            'category_description': ['Beef Chuck', 'Beef Rib', 'Beef Loin', 'Beef Brisket', 'Beef Round']
        })
        
        self.transformer = ProductTransformer()
    
    def test_merge_product_descriptions_handles_null_values(self):
        """Test that merge_product_descriptions properly handles null values."""
        result = self.transformer.merge_product_descriptions(self.test_data, primary_col='product_description', secondary_col='description', output_col='product_description')
        
        # Verify row counts match
        self.assertEqual(len(result), len(self.test_data), "Row count should be preserved")
        
        # Check merged descriptions
        self.assertEqual(result.iloc[0]['product_description'], 'Valid description Extra info')  # Both columns have values
        self.assertEqual(result.iloc[1]['product_description'], 'Another valid')  # description is null
        self.assertEqual(result.iloc[2]['product_description'], 'Only in description col')  # product_description is null
        self.assertEqual(result.iloc[3]['product_description'], 'Only in description col')  # product_description is empty
        self.assertEqual(result.iloc[4]['product_description'], 'Final valid More detail')  # Both columns have values
        
        # Verify no null values in result
        self.assertEqual(result['product_description'].isnull().sum(), 0, "Should have no null values after merge")
        self.assertTrue(all(result['product_description'].str.strip() != ''), "Should have no empty values after merge")

    def test_process_product_data_validates_required_columns(self):
        """Test that process_product_data validates the presence of required columns."""
        # Missing required column
        incomplete_data = self.test_data.drop('product_code', axis=1)
        
        with self.assertRaises(ValueError):
            transformed_data = self.transformer.transform(incomplete_data)

    def test_process_product_data_removes_invalid_descriptions(self):
        """Test that process_product_data removes rows with invalid descriptions after merging."""
        # Create data with only empty descriptions after merge would occur
        bad_data = pd.DataFrame({
            'product_code': ['001', '002'],
            'product_description': ['', np.nan],
            'description': [np.nan, ''],
            'category_description': ['Beef Chuck', 'Beef Rib']
        })
        
        with self.assertRaises(ValueError):
            transformed_data = self.transformer.transform(bad_data)

    def test_process_product_data_maps_columns(self):
        """Test that column mapping works correctly."""
        # Test with different column name casing
        mixed_case_data = pd.DataFrame({
            'Product_Code': ['001', '002'],
            'PRODUCT_DESCRIPTION': ['Valid desc', 'Another desc'],
            'Category_Description': ['Beef Chuck', 'Beef Rib']
        })
        
        result = self.transformer.transform(mixed_case_data)
        
        # Check columns were properly mapped
        self.assertIn('product_code', result.columns)
        self.assertIn('product_description', result.columns)
        self.assertIn('category_description', result.columns)
        
        # Verify values
        self.assertEqual(result.iloc[0]['product_description'], 'Valid desc')
        self.assertEqual(result.iloc[1]['product_description'], 'Another desc')

    def test_transformer_product_categories(self):
        """Test product category mapping in transformer."""
        # Mock reference data
        categories = ['Beef Chuck', 'Beef Rib', 'Beef Loin', 'Beef Brisket', 'Beef Round']
        self.transformer.get_categories = MagicMock(return_value=categories)
        
        # Transform data
        result = self.transformer.transform(self.test_data)
        
        # All category_description values should still be in the result
        for cat in categories:
            self.assertTrue(any(result['category_description'] == cat), f"{cat} should be preserved")

    @patch('data_ingestion.core.product_transformer.logger')
    def test_logs_description_stats(self, mock_logger):
        """Test that proper logging occurs for description stats."""
        process_product_data(self.test_data)
        
        # Verify that the logger was called with description stats
        mock_logger.info.assert_any_call("Description statistics after merge:")


if __name__ == '__main__':
    unittest.main()
