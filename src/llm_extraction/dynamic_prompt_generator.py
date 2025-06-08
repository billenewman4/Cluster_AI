"""
Dynamic Prompt Generator Module

Generates specialized prompts for each primal cut based on reference data.
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
        Generate a system prompt specialized for a specific primal cut.
        
        Args:
            primal: The primal cut name
            
        Returns:
            System prompt string
        """
        subprimals = self.reference_data.get_subprimals(primal)
        subprimal_terms = self.reference_data.get_all_subprimal_terms(primal)
        
        # Build a specialized system prompt
        system_prompt = f"""You are a butchery-domain extraction assistant specialized in beef {primal.lower()} cuts.
Use the Meat Buyer's Guide as ground truth for cut names and hierarchy.

Extract structured data from product descriptions with high accuracy.
Focus on identifying: species, primal, subprimal, grade, size, size unit, and brand.

For beef {primal.lower()}, valid subprimals include: {', '.join(subprimals)}
Common synonyms and alternative terms for these subprimals include: {', '.join(sorted(subprimal_terms))}

Return valid JSON only."""

        return system_prompt
        
    def generate_user_prompt(self, primal: str, description: str) -> str:
        """
        Generate a user prompt for a specific primal cut and product description.
        
        Args:
            primal: The primal cut name
            description: The product description to extract from
            
        Returns:
            User prompt string
        """
        # Get example subprimals for this primal (up to 3)
        subprimals = self.reference_data.get_subprimals(primal)[:3]
        example_subprimals = subprimals if subprimals else ["Unknown"]
        
        # Build example strings based on the primal
        examples = []
        
        # First example
        examples.append(f"""Input: "Beef {primal} {example_subprimals[0]} 15# Choice Certified Angus"
Output: {{"species": "Beef", "primal": "{primal}", "subprimal": "{example_subprimals[0]}", "grade": "Choice", "size": 15, "size_uom": "#", "brand": "Certified Angus"}}""")
        
        # Second example with different size unit
        if len(example_subprimals) > 1:
            examples.append(f"""Input: "{primal} {example_subprimals[1]} Prime 8oz"  
Output: {{"species": "Beef", "primal": "{primal}", "subprimal": "{example_subprimals[1]}", "grade": "Prime", "size": 8, "size_uom": "oz", "brand": null}}""")
        
        # Third example with a different grade
        if len(example_subprimals) > 2:
            examples.append(f"""Input: "Beef {primal} {example_subprimals[2]} Wagyu 12lb"
Output: {{"species": "Beef", "primal": "{primal}", "subprimal": "{example_subprimals[2]}", "grade": "Wagyu", "size": 12, "size_uom": "lb", "brand": null}}""")
        
        # Build the user prompt
        user_prompt = f"""Extract structured data from this product description:

Description: "{description}"

Return a JSON object with exactly these keys:
- species (Beef, Pork, etc.)
- primal (e.g. {primal}, Loin) 
- subprimal (e.g. {', '.join(example_subprimals)})
- grade (one of: No Grade, Prime, Choice, Select, NR, Utility, Wagyu, Angus, Creekstone Angus)
- size (numeric value only, null if not found)
- size_uom (oz | lb | # | g | kg, null if not found)
- brand (free text or null)

If any value cannot be determined, use null.

Examples:

{examples[0]}

{examples[1] if len(examples) > 1 else ''}

{examples[2] if len(examples) > 2 else ''}"""

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
