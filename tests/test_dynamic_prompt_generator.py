"""
Tests for the DynamicPromptGenerator module.

Validates that the prompt generator creates appropriate prompts
for different primal cuts based on reference data.
"""

import unittest
from unittest.mock import MagicMock, patch

import pytest

from src.LLM.prompts.dynamic_prompt_generator import DynamicPromptGenerator


class TestDynamicPromptGenerator(unittest.TestCase):
    """Test suite for DynamicPromptGenerator."""

    def setUp(self):
        """Set up test fixtures before each test."""
        # Create mock ReferenceDataLoader
        self.mock_reference_data = MagicMock()
        
        # Configure mock for Chuck primal
        self.mock_reference_data.get_subprimals.return_value = ["Chuck Roll", "Chuck Blade", "Chuck Eye"]
        self.mock_reference_data.get_all_subprimal_terms.return_value = {
            "Chuck Roll", "Chuck Blade", "Chuck Eye",
            "CR", "Chuck Rst", "CB", "Blade", "CE"
        }
        
        # Create prompt generator with mock reference data
        self.prompt_generator = DynamicPromptGenerator(self.mock_reference_data)
        
        # Test description
        self.test_description = "Beef Chuck Roll 10# Choice"

    def test_generate_system_prompt(self):
        """Test generation of system prompt for a specific primal."""
        # Get system prompt for Chuck
        system_prompt = self.prompt_generator.generate_system_prompt("Chuck")
        
        # Verify prompt content
        self.assertIn("You are a butchery-domain extraction assistant", system_prompt)
        self.assertIn("beef chuck", system_prompt)
        self.assertIn("Chuck Roll", system_prompt)
        self.assertIn("Chuck Blade", system_prompt)
        self.assertIn("Chuck Eye", system_prompt)
        self.assertIn("CR", system_prompt)
        self.assertIn("Chuck Rst", system_prompt)
        
        # Verify reference data calls
        self.mock_reference_data.get_subprimals.assert_called_once_with("Chuck")
        self.mock_reference_data.get_all_subprimal_terms.assert_called_once_with("Chuck")

    def test_generate_user_prompt(self):
        """Test generation of user prompt with a product description."""
        # Get user prompt for Chuck
        user_prompt = self.prompt_generator.generate_user_prompt("Chuck", self.test_description)
        
        # Verify prompt content
        self.assertIn(f'Description: "{self.test_description}"', user_prompt)
        self.assertIn("Chuck Roll", user_prompt)
        self.assertIn("species", user_prompt)
        self.assertIn("primal", user_prompt)
        self.assertIn("subprimal", user_prompt)
        self.assertIn("grade", user_prompt)
        self.assertIn("size", user_prompt)
        self.assertIn("size_uom", user_prompt)
        self.assertIn("brand", user_prompt)
        
        # Verify examples are included
        self.assertIn("Input:", user_prompt)
        self.assertIn("Output:", user_prompt)
        self.assertIn("Chuck Roll", user_prompt)
        
        # Verify JSON format in examples
        self.assertIn('{"species": "Beef", "primal": "Chuck"', user_prompt)

    def test_generate_user_prompt_with_limited_subprimals(self):
        """Test user prompt generation with limited subprimals."""
        # Configure mock to return fewer subprimals
        self.mock_reference_data.get_subprimals.return_value = ["Chuck Roll"]
        
        # Get user prompt
        user_prompt = self.prompt_generator.generate_user_prompt("Chuck", self.test_description)
        
        # Verify prompt has at least one example
        self.assertIn("Chuck Roll", user_prompt)
        self.assertIn("Input:", user_prompt)
        self.assertIn("Output:", user_prompt)

    def test_get_post_processing_rules_generic(self):
        """Test getting generic post-processing rules."""
        # Get generic rules
        rules = self.prompt_generator.get_post_processing_rules()
        
        # Verify basic rules are included
        self.assertIn("grade_regex_patterns", rules)
        self.assertIn("size_regex_pattern", rules)
        self.assertIn("brand_keywords", rules)
        
        # Verify specific patterns
        self.assertIn((r'\bprime\b', 'Prime'), rules["grade_regex_patterns"])
        self.assertIn((r'\bchoice\b', 'Choice'), rules["grade_regex_patterns"])
        
        # Verify size regex pattern
        import re
        self.assertTrue(re.search(rules["size_regex_pattern"], "10 lb"))
        self.assertTrue(re.search(rules["size_regex_pattern"], "8 oz"))
        self.assertTrue(re.search(rules["size_regex_pattern"], "15#"))
        
        # Verify brand keywords
        self.assertIn("angus", rules["brand_keywords"])
        self.assertIn("certified", rules["brand_keywords"])

    def test_get_post_processing_rules_primal_specific(self):
        """Test getting primal-specific post-processing rules."""
        # Configure mock for subprimal terms
        subprimal_terms = {"Chuck Roll", "CR", "Chuck Blade", "CB"}
        self.mock_reference_data.get_all_subprimal_terms.return_value = subprimal_terms
        
        # Get primal-specific rules
        rules = self.prompt_generator.get_post_processing_rules("Chuck")
        
        # Verify subprimal terms are included
        self.assertIn("subprimal_terms", rules)
        self.assertEqual(set(rules["subprimal_terms"]), subprimal_terms)
        
        # Verify generic rules are still included
        self.assertIn("grade_regex_patterns", rules)
        self.assertIn("size_regex_pattern", rules)
        self.assertIn("brand_keywords", rules)
        
        # Verify reference data call
        self.mock_reference_data.get_all_subprimal_terms.assert_called_with("Chuck")


if __name__ == "__main__":
    unittest.main()
