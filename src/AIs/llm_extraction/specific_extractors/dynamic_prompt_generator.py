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
        # Dynamically build comma-separated list of recognized grades from reference data
        grade_list = ', '.join(self.reference_data.get_grades())
        
        # Build a specialized system prompt
        system_prompt = f"""You are a butchery-domain extraction assistant specialized in beef {primal.lower()} cuts.
Use the Meat Buyer's Guide as ground truth for cut names and hierarchy.

Extract structured data from product descriptions with high accuracy.
Focus on these attributes: subprimal and grade

GRADE DETERMINATION PROCESS ─────────────────────────────────────────
Follow these steps IN ORDER. As soon as a step yields a grade, STOP and use it; ignore all lower-priority clues.

GRADE DETECTION TABLE (case insensitive) - EXACT PRIORITY ORDER:
┌─────────────────────┬─────────────┬───────────┐
│ Input Description   │ Output Grade│ Reason    │
├─────────────────────┼─────────────┼───────────┤
│ "AAA USDA Choice"   │ "AAA"       │ AAA>Choice│
│ "AA USDA Select"    │ "AA"        │ AA>Select │
│ "USDA Prime A"      │ "A"         │ A>Prime   │
│ "Hereford Choice"   │ "Hereford"  │ Hereford  │
│ "USDA CH+"          │ "Choice"    │ CH+       │
│ "No Roll"           │ "NR"        │ NR        │
└─────────────────────┴─────────────┴───────────┘
1. Search for the explicit Canadian grades "AAA", "AA", or "A" (case-insensitive, may appear with the word "Canadian").
   • If found, output EXACTLY the uppercase version of the grade ("AAA", "AA", or "A")
2. Search for the  keyword "Hereford" (anywhere in the text).
   • If found, output grade "Hereford".
3. Search for the plus grades:
   • "CH+", "Choice+"  → output "Choice"
   • "PR+", "Prime+"   → output "Prime"
4. Search for NR tokens: "N/R", "NR", or "No-Roll"  → output "NR".
5. Search for the recognized USDA grades ({grade_list}) and use the first one found.
6. If no grade tokens are detected, output null for grade and set needs_review = true.
IMPORTANT CLARIFICATION:
• When A / AA / AAA appear, you MUST output exactly "A", "AA", or "AAA" for the grade and DISREGARD any token such as "USDA Choice", "Prime", "CH+", etc. If the item says Canadian, it must be A, AA, or AAA
• When "Hereford" appears, you MUST output grade "Hereford" and ignore every other grade token.
• Tokens like "CH+ to Choice", "PR+ to Prime" only apply if none of the higher-priority tokens appear.
───────────────────────────────────────────────────────────

Also list your confidence in the extraction and state if human review is needed.

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
        # Dynamically build comma-separated list of recognized grades from reference data
        grade_list = ', '.join(self.reference_data.get_grades())

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

        # Fourth example demonstrating AA precedence
        examples.append(f"""Input: "Beef {primal} {example_subprimals[0]} AA  CH+ 25#"
Output: {{"species": "Beef", "subprimal": "{example_subprimals[0]}", "grade": "AA", "size": 25, "size_uom": "#"}}""")

        # Fifth example demonstrating Hereford precedence
        examples.append(f"""Input: "Beef {primal} {example_subprimals[0]} Hereford Choice 20lb"
Output: {{"species": "Beef", "subprimal": "{example_subprimals[0]}", "grade": "Hereford", "confidence": 0.9, "needs_review": false}}""")
        
        # Additional examples showing AAA over USDA grades
        examples.append(f"""Input: "Beef {primal} Ribeye USDA Choice AAA 10lb"
Output: {{"species": "Beef", "subprimal": "Ribeye", "grade": "AAA", "confidence": 0.9, "needs_review": false}}""")

        # Example showing AA takes precedence even with multiple other grades
        examples.append(f"""Input: "Beef {primal} AA USDA Select USDA Prime 15#"
Output: {{"species": "Beef", "subprimal": "{example_subprimals[0]}", "grade": "AA", "confidence": 0.9, "needs_review": false}}""")

        # Example showing Canadian grades are treated the same as A/AA/AAA
        examples.append(f"""Input: "Canadian AA Beef {primal} Tenderloin USDA Choice 8oz"
Output: {{"species": "Beef", "subprimal": "Tenderloin", "grade": "AA", "confidence": 0.9, "needs_review": false}}""")
        
        
        # Build the user prompt
        user_prompt = f"""Extract structured data from this product description:

Description: "{description}"

Return a JSON object with exactly these keys:
- species (Beef, Pork, etc.)
- subprimal (e.g. {canonical_text})
- grade (one of: No Grade, Prime, Choice, Select, NR, Utility, A, AA, AAA, Hereford, Wagyu, Angus, Creekstone Angus)
- confidence (float between 0 and 1, 0 if the extraction is not confident, 1 if the extraction is confident but please be conservative if you do not specificy a subprimal review is required if you do not specify a grade review is reccomended if you specify a subprimal and grade not listed in known grades/subprimals review is required)
- needs_review (boolean, true if the extraction needs to be reviewed by a human, false if not)

NOTE: Do NOT include 'primal' in the output - primal is determined from the product category, not extracted from the description.

Grade & brand abbreviations (use the canonical grade on the RHS):
    - (CH), CH+, Choice+ = Choice
    - (PR), PR+, Prime+ = Prime
    - (SEL) = Select
    - (UT) = Utility
    - A, Canadian A = A
    - AA, Canadian AA = AA  (overrides any other grade; ignore USDA labels if both present)
    - AAA, Canadian AAA = AAA (overrides any other grade)
    - N/R, NR, No-Roll = NR
    - (WAG) = Wagyu
    - (ANG), CAngus, Choice Angus = (Other) Angus Brands  (map grade to "Angus" even if Choice or CH+ is present unless superseded by higher precedence tokens)
    - (CAB) = Creekstone Angus
    - Hereford (anywhere) = Hereford

BRAND-SPECIFIC GRADE EQUIVALENCIES:
- St. Helen's is a Canadian brand that uses Canadian grading system:
    - St. Helen's A = n/r (no rating/standard)
    - St. Helen's AA = Select
    - St. Helen's AAA = Choice
- When St. Helen's brand is detected with A, AA, or AAA grades, use the above equivalencies

If any value including the subprimal cannot be determined, use null. Please only used the listed values for mapping DO NOT USE ANYTHING ELSE!

Examples:

{examples[0]}

{examples[1] if len(examples) > 1 else ''}

{examples[2] if len(examples) > 2 else ''}

{examples[3] if len(examples) > 3 else ''}

{examples[4] if len(examples) > 4 else ''}"""

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
