"""
Tests for the ReferenceDataLoader module.

Validates that the ReferenceDataLoader correctly loads and parses 
reference data from the beef_cuts.xlsx spreadsheet.
"""

import os
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open

import pandas as pd
import pytest

from src.data_ingestion.utils.reference_data_loader import ReferenceDataLoader


class TestReferenceDataLoader(unittest.TestCase):
    """Test suite for ReferenceDataLoader."""

    def setUp(self):
        """Set up test fixtures before each test."""
        # Create mock data for testing
        self.mock_primal_data = {
            "Chuck": {
                "Chuck Roll": ["CR", "Chuck Rst"],
                "Chuck Blade": ["CB", "Blade"]
            },
            "Loin": {
                "Strip Loin": ["SL", "NY Strip"],
                "Tenderloin": ["Filet", "Filet Mignon"]
            }
        }
        
        self.mock_grade_mappings = {
            "Prime": ["PR", "P"],
            "Choice": ["CH", "C"],
            "Select": ["SL", "S"]
        }
        
        # Path to test data
        self.test_data_path = Path("tests/test_data/beef_cuts_test.xlsx")
        
        # Create test Excel file with mock data
        os.makedirs(os.path.dirname(self.test_data_path), exist_ok=True)
        
        # Create Excel writer with mock data
        with pd.ExcelWriter(self.test_data_path) as writer:
            # Write Chuck sheet
            chuck_data = pd.DataFrame({
                'Sub-primal': ['Chuck Roll', 'Chuck Blade'],
                'Known Synonyms': ['CR, Chuck Rst', 'CB, Blade']
            })
            chuck_data.to_excel(writer, sheet_name='Beef Chuck', index=False)
            
            # Write Loin sheet
            loin_data = pd.DataFrame({
                'Sub-primal': ['Strip Loin', 'Tenderloin'],
                'Known Synonyms': ['SL, NY Strip', 'Filet, Filet Mignon']
            })
            loin_data.to_excel(writer, sheet_name='Beef Loin', index=False)
            
            # Write Grades sheet
            grades_data = pd.DataFrame({
                'Official / Commercial Grade Name': ['Prime', 'Choice', 'Select'],
                'Common Synonyms & Acronyms': ['PR, P', 'CH, C', 'SL, S']
            })
            grades_data.to_excel(writer, sheet_name='Grades', index=False)
    
    def tearDown(self):
        """Clean up after each test."""
        # Remove test Excel file
        if self.test_data_path.exists():
            os.remove(self.test_data_path)
            
        # Remove test directory if empty
        test_dir = self.test_data_path.parent
        if test_dir.exists() and not any(test_dir.iterdir()):
            os.rmdir(test_dir)
    
    def test_initialize_with_valid_path(self):
        """Test initialization with a valid data path."""
        loader = ReferenceDataLoader(str(self.test_data_path))
        self.assertIsNotNone(loader)
        self.assertEqual(loader.data_path, self.test_data_path)
        
    def test_initialize_with_invalid_path(self):
        """Test initialization with an invalid data path."""
        invalid_path = "invalid/path/beef_cuts.xlsx"
        with self.assertRaises(FileNotFoundError):
            ReferenceDataLoader(invalid_path)
    
    def test_load_data_success(self):
        """Test successful data loading."""
        loader = ReferenceDataLoader(str(self.test_data_path))
        
        # Check primal data
        self.assertEqual(len(loader.primal_data), 2)
        self.assertIn("Chuck", loader.primal_data)
        self.assertIn("Loin", loader.primal_data)
        
        # Check subprimal data for Chuck
        chuck_subprimals = loader.primal_data["Chuck"]
        self.assertEqual(len(chuck_subprimals), 2)
        self.assertIn("Chuck Roll", chuck_subprimals)
        self.assertEqual(chuck_subprimals["Chuck Roll"], ["CR", "Chuck Rst"])
        
        # Check grade mappings
        self.assertEqual(len(loader.grade_mappings), 3)
        self.assertIn("Prime", loader.grade_mappings)
        self.assertEqual(loader.grade_mappings["Prime"], ["PR", "P"])
    
    def test_get_primals(self):
        """Test get_primals method."""
        loader = ReferenceDataLoader(str(self.test_data_path))
        primals = loader.get_primals()
        
        self.assertEqual(len(primals), 2)
        self.assertIn("Chuck", primals)
        self.assertIn("Loin", primals)
    
    def test_get_subprimals(self):
        """Test get_subprimals method."""
        loader = ReferenceDataLoader(str(self.test_data_path))
        
        # Test for Chuck
        chuck_subprimals = loader.get_subprimals("Chuck")
        self.assertEqual(len(chuck_subprimals), 2)
        self.assertIn("Chuck Roll", chuck_subprimals)
        self.assertIn("Chuck Blade", chuck_subprimals)
        
        # Test for nonexistent primal
        nonexistent_subprimals = loader.get_subprimals("Nonexistent")
        self.assertEqual(len(nonexistent_subprimals), 0)
    
    def test_get_subprimal_synonyms(self):
        """Test get_subprimal_synonyms method."""
        loader = ReferenceDataLoader(str(self.test_data_path))
        
        # Test for Chuck Roll
        synonyms = loader.get_subprimal_synonyms("Chuck", "Chuck Roll")
        self.assertEqual(len(synonyms), 2)
        self.assertIn("CR", synonyms)
        self.assertIn("Chuck Rst", synonyms)
        
        # Test for nonexistent subprimal
        nonexistent_synonyms = loader.get_subprimal_synonyms("Chuck", "Nonexistent")
        self.assertEqual(len(nonexistent_synonyms), 0)
        
        # Test for nonexistent primal
        nonexistent_primal_synonyms = loader.get_subprimal_synonyms("Nonexistent", "Subprimal")
        self.assertEqual(len(nonexistent_primal_synonyms), 0)
    
    def test_get_all_subprimal_terms(self):
        """Test get_all_subprimal_terms method."""
        loader = ReferenceDataLoader(str(self.test_data_path))
        
        # Test for Chuck
        chuck_terms = loader.get_all_subprimal_terms("Chuck")
        self.assertEqual(len(chuck_terms), 6)  # 2 subprimals + 4 synonyms
        self.assertIn("Chuck Roll", chuck_terms)
        self.assertIn("Chuck Blade", chuck_terms)
        self.assertIn("CR", chuck_terms)
        self.assertIn("Chuck Rst", chuck_terms)
        self.assertIn("CB", chuck_terms)
        self.assertIn("Blade", chuck_terms)
        
        # Test for nonexistent primal
        nonexistent_terms = loader.get_all_subprimal_terms("Nonexistent")
        self.assertEqual(len(nonexistent_terms), 0)
    
    def test_get_grades(self):
        """Test get_grades method."""
        loader = ReferenceDataLoader(str(self.test_data_path))
        grades = loader.get_grades()
        
        self.assertEqual(len(grades), 3)
        self.assertIn("Prime", grades)
        self.assertIn("Choice", grades)
        self.assertIn("Select", grades)
    
    def test_get_grade_synonyms(self):
        """Test get_grade_synonyms method."""
        loader = ReferenceDataLoader(str(self.test_data_path))
        
        # Test for Prime
        prime_synonyms = loader.get_grade_synonyms("Prime")
        self.assertEqual(len(prime_synonyms), 2)
        self.assertIn("PR", prime_synonyms)
        self.assertIn("P", prime_synonyms)
        
        # Test for nonexistent grade
        nonexistent_synonyms = loader.get_grade_synonyms("Nonexistent")
        self.assertEqual(len(nonexistent_synonyms), 0)
    
    def test_get_all_grade_terms(self):
        """Test get_all_grade_terms method."""
        loader = ReferenceDataLoader(str(self.test_data_path))
        
        grade_terms = loader.get_all_grade_terms()
        self.assertEqual(len(grade_terms), 9)  # 3 grades + 6 synonyms
        
        # Official names
        self.assertIn("Prime", grade_terms)
        self.assertIn("Choice", grade_terms)
        self.assertIn("Select", grade_terms)
        
        # Synonyms
        self.assertIn("PR", grade_terms)
        self.assertIn("P", grade_terms)
        self.assertIn("CH", grade_terms)
        self.assertIn("C", grade_terms)
        self.assertIn("SL", grade_terms)
        self.assertIn("S", grade_terms)


if __name__ == "__main__":
    unittest.main()
