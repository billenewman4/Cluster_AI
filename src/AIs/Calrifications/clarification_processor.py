"""
Clarification Processor: Generates questions about unclear product descriptions
"""

import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from dotenv import load_dotenv
from src.AIs.utils.api_utils import APIManager
from src.AIs.utils.result_parser import ResultParser

load_dotenv()
logger = logging.getLogger(__name__)

@dataclass
class ClarificationResult:
    """Simple structure for clarification results."""
    product_code: str
    product_description: str
    questions: List[str]
    extraction_results: Optional[Dict[str, Any]] = None  # Add extraction results

class ClarificationProcessor:
    """Generates clarification questions about unclear product descriptions."""
    
    def __init__(self, provider: str = "openai", model: str = "gpt-4o-mini", env_key: str = "OPENAI_API_KEY"):
        """Initialize the clarification processor."""
        api_key = os.getenv(env_key)
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")
        
        self.api_manager = APIManager(
            provider=provider,
            model=model,
            api_key=api_key,
            max_rpm=30,
            temperature=0.4
        )
        
        logger.info(f"Initialized clarification processor: {self.api_manager.get_provider_info()}")
    
    def create_system_prompt(self) -> str:
        """Create system prompt for clarification analysis."""
        return """You are a meat industry data reviewer. Your job is to identify ONLY the most critical unclear elements.

STRICT RULES:
1. Ask 0-2 questions maximum per product
2. ONLY ask if there's genuinely unclear terminology that the AI couldn't handle
3. Focus on specific abbreviations, codes, or terms that are undefined
4. Do NOT ask about things the AI extracted correctly
5. Do NOT ask general questions - be laser-focused

GOOD EXAMPLES:
- "What does '*__' indicate in this product code?" (undefined symbol)
- What does '120A-3' signify in the product description?

BAD EXAMPLES:
- "What is the significance of Choice grade?" (AI extracted this correctly)
- "What does 12oz refer to?" (obvious weight measurement)
- "What is a ribeye?" (common cut name)
- "What does 'bf' stand for?" (common abbreviation for beef)
- "What does bnl stand for?" (common abbreviation for boneless)

MOST PRODUCTS SHOULD GET 0 QUESTIONS. Only ask when there are genuinely confusing elements that prevented proper extraction.

RETURN FORMAT: JSON only
{
  "questions": [
    "What does 'NR' stand for in beef grading?"
  ]
}"""

    def create_user_prompt(self, description: str, previous_extraction: Dict[str, Any]) -> str:
        """Create user prompt for clarification analysis."""
        extraction_text = json.dumps(previous_extraction, indent=2)
        
        return f"""Review this product for ONLY genuinely unclear elements that caused extraction problems:

PRODUCT DESCRIPTION: "{description}"

AI EXTRACTION RESULT:
{extraction_text}

ONLY ask questions if:
- There are abbreviations/codes the AI couldn't identify
- There are terms that caused extraction errors or null values
- There are symbols or patterns that are undefined

Do NOT ask about:
- Things the AI extracted successfully 
- Common meat industry terms
- Standard measurements or grades

Ask 0-2 questions maximum. Most products should get 0 questions.

Return JSON with questions array:"""

    def analyze_product(self, description: str, previous_extraction: Dict[str, Any], product_code: str) -> ClarificationResult:
        """Analyze a single product and generate clarification questions."""
        try:
            system_prompt = self.create_system_prompt()
            user_prompt = self.create_user_prompt(description, previous_extraction)
            
            # Call AI
            response = self.api_manager.call_with_retry(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                max_retries=3
            )
            
            if not response:
                logger.error(f"No response from AI for product {product_code}")
                return ClarificationResult(
                    product_code=product_code,
                    product_description=description,
                    questions=["ERROR: No AI response received"],
                    extraction_results=previous_extraction
                )
            
            # Parse response with robust JSON parsing
            try:
                ai_response = ResultParser.parse_json_response(response)
                if ai_response:
                    questions = ai_response.get("questions", [])
                    
                    if not isinstance(questions, list):
                        questions = ["ERROR: Invalid AI response format"]
                else:
                    logger.error(f"Failed to parse JSON response for {product_code}")
                    questions = ["ERROR: Could not parse AI response - invalid JSON format"]
                    
            except Exception as e:
                logger.error(f"Error parsing response for {product_code}: {e}")
                questions = [f"ERROR: Could not parse AI response - {str(e)}"]
            
            result = ClarificationResult(
                product_code=product_code,
                product_description=description,
                questions=questions,
                extraction_results=previous_extraction
            )
            
            logger.info(f"Generated {len(questions)} questions for {product_code}")
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing {product_code}: {str(e)}")
            return ClarificationResult(
                product_code=product_code,
                product_description=description,
                questions=[f"ERROR: {str(e)}"],
                extraction_results=previous_extraction
            )

def process_products_for_clarification(
    products_data: List[Dict[str, Any]], 
    provider: str = "openai"
) -> List[ClarificationResult]:
    """
    Main function: Process multiple products and generate clarification questions.
    
    Args:
        products_data: List of dicts with 'product_code', 'description', 'previous_extraction'
        provider: AI provider to use
    
    Returns:
        List of ClarificationResult objects
    """
    logger.info(f"Processing {len(products_data)} products for clarification")
    
    processor = ClarificationProcessor(provider=provider)
    results = []
    
    for i, product in enumerate(products_data, 1):
        logger.info(f"Processing {i}/{len(products_data)}: {product['product_code']}")
        
        result = processor.analyze_product(
            description=product['description'],
            previous_extraction=product['previous_extraction'],
            product_code=product['product_code']
        )
        results.append(result)
    
    total_questions = sum(len(r.questions) for r in results)
    logger.info(f"Generated {total_questions} total questions across {len(results)} products")
    
    return results
