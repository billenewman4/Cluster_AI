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
        
        # Build a specialized system prompt
        system_prompt = f"""You are a butchery-domain extraction assistant specialized in beef {primal.lower()} cuts.
Use the Meat Buyer's Guide as ground truth for cut names and hierarchy.

Extract structured data from product descriptions with high accuracy.
Focus on identifying: subprimal, grade, size, size unit, and bone-in for extraction.

Also list your confidence in the extraction and if you think the extraction needs to be reviewed by a human or if it is accurate.

IMPORTANT: Always use the CANONICAL subprimal names, not synonyms. When you encounter synonyms, map them to their canonical form. If you do not see a clear mapping, YOU MUST leave the information null. It will be common for mappings to be unclear.

For beef {primal.lower()}, the canonical subprimals and their synonyms are:
{canonical_text}

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

        # Get example subprimals for this primal (up to 3)
        subprimals = self.reference_data.get_subprimals(primal)[:3]
        example_subprimals = subprimals if subprimals else ["Unknown"]
        
        # Build example strings based on the primal
        examples = []
        
        # First example
        examples.append(f"""Input: "Beef {primal} {example_subprimals[0]} 15# (CH) CAB"
Output: {{"species": "Beef", "subprimal": "{example_subprimals[0]}", "grade": "Creekstone Angus", "size": 15, "size_uom": "#"}}""")
        
        # Second example with different size unit
        if len(example_subprimals) > 1:
            examples.append(f"""Input: "{primal} {example_subprimals[1]} Pri 8oz"  
Output: {{"species": "Beef", "subprimal": "{example_subprimals[1]}", "grade": "Prime", "size": 8, "size_uom": "oz"}}""")
        
        # Third example with a different grade
        if len(example_subprimals) > 2:
            examples.append(f"""Input: "Beef {primal} {example_subprimals[2]} Wagyu 12#"
Output: {{"species": "Beef", "subprimal": "{example_subprimals[2]}", "grade": "Wagyu", "size": 12, "size_uom": "lb"}}""")
        
        # Build the user prompt
        user_prompt = f"""Extract structured data from this product description:

Description: "{description}"

Return a JSON object with exactly these keys:
- species (Beef, Pork, etc.)
- subprimal (e.g. {canonical_text})
- grade (one of: No Grade, Prime, Choice, Select, NR, Utility, Wagyu, Angus, Creekstone Angus)
- size (numeric value only, null if not found)
- size_uom (oz | lb | g | kg, null if not found)
- bone_in (boolean, true if the product is bone-in, false if not)
- confidence (float between 0 and 1, 0 if the extraction is not confident, 1 if the extraction is confident but please be conservative if you do not specificy a subprimal review is required if you do not specify a grade review is reccomended if you specify a subprimal and grade not listed in known grades/subprimals review is required)
- needs_review (boolean, true if the extraction needs to be reviewed by a human, false if not)

NOTE: Do NOT include 'primal' in the output - primal is determined from the product category, not extracted from the description.

- note the following abbreviations are used in the descriptions:
    - (CH) Choice
    - (PR) Prime
    - (SEL) Select
    - (UT) Utility
    - (WAG) Wagyu
    - (ANG) Angus
    - (CAB) Creekstone Angus
    - USDA Choice = Choice
    - USDA Prime = Prime
    - USDA Select = Select
    - USDA Utility = Utility
    - USDA Wagyu = Wagyu
    - USDA Angus = Angus
    - USDA Creekstone Angus = Creekstone Angus
    - Choice Angus = Choice
    - AAA or Canadian AAA = Choice
    - AA or Canadian AA = Select
    - A or Canadian A = Standard

If any value including the subprimal cannot be determined, use null. Please only used the listed values for mapping DO NOT USE ANYTHING ELSE!

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
