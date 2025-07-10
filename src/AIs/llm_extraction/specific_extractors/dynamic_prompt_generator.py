"""
Dynamic Prompt Generator Module for Subprimal Extraction

Generates specialized prompts for subprimal extraction based on reference data.
Focuses exclusively on subprimal identification, not grade or other attributes.
"""

import re
from typing import Dict, List, Any, Set

class DynamicPromptGenerator:
    """
    Generates dynamic prompts for LLM extraction based on reference data.
    
    Creates specialized system and user prompts for each primal cut
    by incorporating reference data from the beef_cuts.xlsx file.
    """
    
    def __init__(self, reference_data_loader):
        """
        Initialize the prompt generator.
        
        Args:
            reference_data_loader: Instance of ReferenceDataLoader with loaded reference data
        """
        self.reference_data = reference_data_loader
        
    def generate_system_prompt(self, primal: str) -> str:
        """
        Generate a system prompt specialized for subprimal extraction for a specific primal cut.
        
        Args:
            primal: The primal cut name
            
        Returns:
            System prompt string focused solely on subprimal extraction
        """
        subprimals = self.reference_data.get_subprimals(primal)
        
        # Build canonical to synonym mappings for clear instruction
        canonical_mappings = []
        for subprimal in subprimals:
            synonyms = self.reference_data.get_subprimal_synonyms(primal, subprimal)
            if synonyms:
                synonym_list = ', '.join(synonyms)
                canonical_mappings.append(f"• {subprimal} (synonyms: {synonym_list})")
            else:
                canonical_mappings.append(f"• {subprimal}")
        
        canonical_text = '\n'.join(canonical_mappings)
        
        # Build a specialized system prompt focused solely on subprimal extraction
        system_prompt = f"""You are a butchery-domain extraction assistant specialized in identifying beef {primal.lower()} subprimal cuts.
Use the Meat Buyer's Guide as ground truth for cut names and hierarchy.

Extract ONLY the subprimal information from product descriptions with high accuracy.
Do not attempt to extract grade, size, or other attributes - focus EXCLUSIVELY on identifying the subprimal.

IMPORTANT: Always use the CANONICAL subprimal names, not synonyms. When you encounter synonyms, map them to their canonical form. If you do not see a clear mapping, YOU MUST leave the subprimal information null. It will be common for mappings to be unclear.

For beef {primal.lower()}, the canonical subprimals and their synonyms are:
{canonical_text}

Your response should include:
1. The identified subprimal (or null if unclear)
2. Your confidence in the extraction (0.0 to 1.0)
3. Whether human review is needed (true if uncertain)

Return valid JSON only with this exact schema:
{{
  "subprimal": string or null,
  "confidence": number between 0.0 and 1.0,
  "needs_review": boolean
}}
"""

        return system_prompt
        
    def generate_user_prompt(self, primal: str, description: str) -> str:
        """
        Generate a user prompt focused solely on subprimal extraction.
        
        Args:
            primal: The primal cut name
            description: The product description to extract from
            
        Returns:
            User prompt string focused only on subprimal extraction
        """
        # Get example subprimals for this primal (up to 3)
        subprimals = self.reference_data.get_subprimals(primal)[:3]
        example_subprimals = subprimals if subprimals else ["Unknown"]
        
        # Build simplified examples focused only on subprimal extraction
        examples = []
        
        # First example
        examples.append(f"""Input: "Beef {primal} {example_subprimals[0]} 15# (CH) CAB"
Output: {{"subprimal": "{example_subprimals[0]}", "confidence": 0.95, "needs_review": false}}""")
        
        # Second example
        if len(example_subprimals) > 1:
            examples.append(f"""Input: "{primal} {example_subprimals[1]} Prime 8oz"  
Output: {{"subprimal": "{example_subprimals[1]}", "confidence": 0.9, "needs_review": false}}""")
        
        # Third example - unclear subprimal
        examples.append(f"""Input: "Beef {primal} special cut 12#"
Output: {{"subprimal": null, "confidence": 0.2, "needs_review": true}}""")

        # Fourth example with complex description
        examples.append(f"""Input: "Beef {primal} {example_subprimals[0]} Choice AA blade tenderized 25#"
Output: {{"subprimal": "{example_subprimals[0]}", "confidence": 0.85, "needs_review": false}}""")
        
        # Build the user prompt focused solely on subprimal extraction
        user_prompt = f"""Extract ONLY the subprimal information from this product description:

Description: "{description}"

Focus EXCLUSIVELY on identifying the subprimal cut from the beef {primal} category.
DO NOT extract grade, size, brand, or any other attributes - focus ONLY on the subprimal.

Return a JSON object with exactly these keys:
- subprimal: The identified subprimal cut or null if unclear
- confidence: Float between 0 and 1 indicating your confidence level
- needs_review: Boolean indicating if human review is needed

If the subprimal cannot be clearly determined from the known subprimals for {primal}, set it to null and mark needs_review as true.

Examples:

{examples[0]}

{examples[1] if len(examples) > 1 else ''}

{examples[2]}

{examples[3]}"""

        return user_prompt
    
    def get_post_processing_rules(self, primal: str = None) -> Dict[str, Any]:
        """
        Get post-processing rules for a specific primal cut.
        
        Args:
            primal: Optional primal cut name for specialized rules
            
        Returns:
            Dictionary of post-processing rules
        """
        # Basic rules that apply to all primals
        rules = {
            "grade_regex_patterns": [
                (r'\bprime\b', 'Prime'),
                (r'\bchoice\b', 'Choice'), 
                (r'\bselect\b', 'Select'),
                (r'\bwagyu\b', 'Wagyu'),
                (r'\bangus\b', 'Angus'),
                (r'\bcreekstone\s+angus\b', 'Creekstone Angus'),
                (r'\butility\b', 'Utility'),
                (r'\bnr\b', 'NR')
            ],
            "size_regex_pattern": r'(\d+(?:\.\d+)?)\s*(oz|lb|#|g|kg)\b',
            "brand_keywords": ["certified", "angus", "creekstone", "prime", "wagyu"]
        }
        
        # Add primal-specific rules if needed
        if primal:
            subprimal_terms = self.reference_data.get_all_subprimal_terms(primal)
            rules["subprimal_terms"] = list(subprimal_terms)
            
        return rules
