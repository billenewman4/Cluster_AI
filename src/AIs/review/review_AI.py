"""
Review Processor: Reviews product extractions to validate completeness and accuracy
"""

import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path
import re

from dotenv import load_dotenv
from src.AIs.utils import APIManager, ResultParser
from src.data_ingestion.utils.reference_data_loader import ReferenceDataLoader

load_dotenv()
logger = logging.getLogger(__name__)

@dataclass
class ReviewResults:
    """Result structure for review processing."""
    subprimal: Optional[str] = None
    grade: Optional[str] = None
    size: Optional[float] = None
    size_uom: Optional[str] = None
    brand: Optional[str] = None
    bone_in: bool = False
    confidence: float = 0.0
    needs_review: bool = False
    miss_categorized: bool = False

class ReviewProcessor:
    """Reviews product extractions to catch missed information and validate accuracy."""
    
    def __init__(self, provider: str = "openai", model: str = "gpt-4o-mini"):
        """Initialize the review processor."""
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")
        
        self.api_manager = APIManager(
            provider=provider,
            model=model,
            api_key=api_key,
            max_rpm=30,
            temperature=0.1  # Lower temperature for more consistent review
        )
        
        # Load reference data for validation - make this required, not optional
        self.reference_data = self._load_reference_data()
        
        # Load all grade terms for validation
        self.valid_grades = self.reference_data.get_all_grade_terms()
        
        logger.info(f"Loaded {len(self.valid_grades)} grade terms from reference data")
        logger.info(f"Initialized review processor: {self.api_manager.get_provider_info()}")
    
    def _load_reference_data(self) -> ReferenceDataLoader:
        """Load reference data with multiple fallback paths."""
        # Try multiple possible paths for the reference data
        possible_paths = [
            "data/incoming/beef_cuts.xlsx",
            "../data/incoming/beef_cuts.xlsx",
            "../../data/incoming/beef_cuts.xlsx",
            "./data/incoming/beef_cuts.xlsx"
        ]
        
        for path in possible_paths:
            try:
                if Path(path).exists():
                    logger.info(f"Loading reference data from: {path}")
                    return ReferenceDataLoader(path)
            except Exception as e:
                logger.warning(f"Failed to load reference data from {path}: {e}")
                continue
        
        # If no path works, raise an error - we require reference data
        raise RuntimeError("Could not load reference data from any expected path. Review AI requires valid reference data to function properly.")
    
    def create_system_prompt(self, primal: Optional[str] = None) -> str:
        """Create system prompt for review analysis using reference data."""
        
        if primal:
            # Get primal-specific subprimals and grades
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
            
            # For grades, we show all grades since they apply across all primals
            grade_list = sorted(list(self.valid_grades))
            
            primal_context = f" for beef {primal.lower()} cuts"
            subprimal_instruction = f"For beef {primal.lower()}, use these canonical subprimal names (map synonyms to canonical):\n{canonical_text}"
        else:
            raise ValueError("Primal is required for review")
        
        valid_grades_text = ', '.join(grade_list)
        
        # Get primal-specific examples for better guidance
        if primal == "Chuck":
            primal_examples = """
SUBPRIMAL IDENTIFICATION ISSUES:
- "SHL CLOD" or "SHOULDER CLOD" = Clod Shoulder (NOT just "Clod")
- "SCOTCH TENDER" or "Scotty Tenders" = Scotty Tender (NOT Mock Tender)
- "CHUCK FLAP" = Chuck Flap (specific subprimal)
- "CHUCK ROLL" terms should extract Chuck Roll subprimal

CRITICAL CHUCK MAPPING RULES:
- SHL CLOD → Clod Shoulder (use the specific "Clod Shoulder" subprimal)
- SCOTCH TENDER → Scotty Tender (preferred over Mock Tender)
- CLOD without SHL qualifier → may be just "Clod" (use context)"""
        elif primal == "Rib":
            primal_examples = """
SUBPRIMAL IDENTIFICATION ISSUES:
- "RIB EYE" or "RIBEYE" = Ribeye Roll (Lip-On) (canonical name)
- "BONELESS RIBEYE" = Ribeye Roll (Lip-On) (synonym)
- "PRIME RIB" = Prime Rib (synonym for Bone-In Rib)
- "BACK RIBS" = Back Ribs (specific subprimal)"""
        elif primal == "Loin":
            primal_examples = """
SUBPRIMAL IDENTIFICATION ISSUES:
- "STRIP LOIN" or "NY STRIP" = Strip Loin (canonical name)
- "TENDERLOIN" or "FILET" = Tenderloin (canonical name)
- "T-BONE" = T-Bone (specific subprimal)
- "PORTERHOUSE" = Porterhouse (specific subprimal)"""
        else:
            # Generic examples for other primals
            primal_examples = """
SUBPRIMAL IDENTIFICATION ISSUES:
- Use canonical names from the reference data
- Map synonyms to their canonical equivalents
- Be specific about the exact subprimal cut"""

        return f"""You are a meat industry expert reviewing product extractions{primal_context} for accuracy and completeness.

Your job is to:
1. Review the previous extraction against the product description
2. Identify any information that was missed or incorrectly extracted
3. Provide corrected/complete extraction data
4. Assign a confidence score based on the clarity of the product description

Focus on extracting:
- {subprimal_instruction}
- Grade ({valid_grades_text})
- Size (numeric value)
- Size unit (lb, #, oz, kg, etc.)
- Brand (specific brand names)
- Bone-in status (true/false)

IMPORTANT: When identifying subprimals, always use the canonical names from the meat industry standard reference guides, not synonyms. For example:
- Use "Teres Major" not "Petite Tender"
- Use "Scotty Tender" not "Scotch Tender"  
- Use "Mock Tender" not "Chuck Tender"

CRITICAL PATTERNS TO WATCH FOR:

BRAND/ABBREVIATION RECOGNITION:
- CAB = Creekstone Angus (grade)
- CH_Angus = Angus (grade)
- NR_Angus = Angus (grade) 
- ANG = Angus (grade abbreviation)
- (A) = Grade A (should extract as grade)
- (Ch) = Choice (grade)
- Chairman's Reserve ≠ Choice (it's a brand, not a grade)
- Hereford is NOT a valid grade
- AAA (Canadian) = Choice equivalent
- SEC = Select (grade abbreviation)
{primal_examples}

GRADE EXTRACTION RULES:
- "USDA CHOICE" = Choice
- "USDA PRIME" = Prime  
- "USDA SELECT" = Select
- "NO-ROLL" or "NR" = No Roll (grade)
- "CH" = Choice (common abbreviation)
- "Ch_Ang" = Choice (Ch prefix indicates Choice grade)
- Missing grade info = null (don't guess)
- If description lacks clear grade indicators, leave null
- Products without USDA or clear grade terms should have blank/null grade

CRITICAL GRADE PATTERNS:
- "N/OFF" without grade indicators = null (no grade extraction)
- "NR" or "NO-ROLL" = "No Roll" grade
- "CH" abbreviation = "Choice" grade  
- Descriptive products without USDA markings = null grade

COMMON MISCLASSIFICATION PATTERNS:
- Bone-in status should be extracted when mentioned
- Brand names (like "Harmony", "GreaterOmaha", "Nebraska Gold") are brands, not grades

VALID SUBPRIMAL CUTS:
{canonical_text}

VALID GRADES:
{valid_grades_text}

Return valid JSON only with the following structure:
{{
    "subprimal": "specific cut name or null",
    "grade": "grade name or null", 
    "size": numeric_value_or_null,
    "size_uom": "unit or null",
    "brand": "brand name or null",
    "bone_in": boolean,
    "confidence": float_between_0_and_1,
    "needs_review": boolean,
    "miss_categorized": boolean
}}

Set miss_categorized to true ONLY if you are very confident (>0.8) that the subprimal identified does not exist in standard beef cutting guides."""
    
    def _extract_primal_from_data(self, previous_extraction: Dict[str, Any], category: str = '') -> str:
        """Extract primal from source data (category) first, not from extraction results."""
        
        # FIRST, try to infer from category (source data) using the extractor's method
        if category:
            # Import and use the existing extractor method
            from src.AIs.llm_extraction.specific_extractors.dynamic_beef_extractor import DynamicBeefExtractor
            extractor = DynamicBeefExtractor()
            inferred_primal = extractor.infer_primal_from_category(category)
            if inferred_primal:
                logger.debug(f"✓ REVIEW: Using primal from category '{category}': {inferred_primal}")
                return inferred_primal
        
        # Only as a LAST RESORT, try to get primal from previous extraction
        primal = previous_extraction.get('primal')
        if primal and primal in self.reference_data.get_primals():
            logger.warning(f"⚠️  REVIEW: Falling back to extracted primal (not ideal): {primal}")
            return primal
        
        # If both fail, raise error
        raise ValueError(f"Valid primal is required for review. Got category='{category}', extracted primal='{primal}'")

    def validate_extraction(self, extraction: Dict[str, Any], primal: str) -> Dict[str, Any]:
        """Validate extraction against reference data for the specific primal."""
        if primal not in self.reference_data.get_primals():
            raise ValueError(f"Invalid primal: {primal}")
            
        validation = {'subprimal_valid': False, 'grade_valid': False}
        
        # Validate subprimal against primal-specific terms (case-insensitive)
        subprimal = extraction.get('subprimal')
        if subprimal:
            primal_subprimal_terms = self.reference_data.get_all_subprimal_terms(primal)
            # Case-insensitive validation
            subprimal_lower = subprimal.lower().strip()
            validation['subprimal_valid'] = any(
                term.lower().strip() == subprimal_lower for term in primal_subprimal_terms
            )
        
        # Validate grade against all grades (case-insensitive)
        grade = extraction.get('grade')
        if grade:
            grade_lower = grade.lower().strip()
            validation['grade_valid'] = any(
                valid_grade.lower().strip() == grade_lower for valid_grade in self.valid_grades
            )
            
        return validation

    def create_user_prompt(self, description: str, previous_extraction: Dict[str, Any], primal: str) -> str:
        """Create user prompt for review analysis."""
        if not primal:
            raise ValueError("Primal is required for review")
            
        extraction_text = json.dumps(previous_extraction, indent=2)
        
        # Simple validation using reference data
        validation = self.validate_extraction(previous_extraction, primal)
        
        # Build guidance with primal-specific options
        primal_subprimals = sorted(list(self.reference_data.get_all_subprimal_terms(primal)))
        subprimal_guidance = f"Look for {primal.lower()} cut names like: {', '.join(primal_subprimals)}"
        
        grade_examples = sorted(list(self.valid_grades))
        grade_guidance = f"Look for grades like: {', '.join(grade_examples)}"

        return f"""Product Description: "{description}"

Previous Extraction:
{extraction_text}

Please review this extraction carefully:

1. Is the subprimal correctly identified? {subprimal_guidance}
2. Is the grade correctly identified? {grade_guidance}
3. Is the size and unit correctly extracted?
4. Is the brand correctly identified?
5. Is bone-in status correctly determined?

Provide the corrected/complete extraction as JSON."""

    def analyze_product(self, description: str, previous_extraction: Dict[str, Any], product_code: str = None, category: str = '') -> ReviewResults:
        """Analyze a single product and review the previous extraction."""
        try:
            # Extract primal (will raise ValueError if not found)
            primal = self._extract_primal_from_data(previous_extraction, category)
            
            # Create primal-specific prompts
            system_prompt = self.create_system_prompt(primal)
            user_prompt = self.create_user_prompt(description, previous_extraction, primal)
            
            logger.info(f"Using {primal}-specific review prompts for product {product_code}")

            # Call AI
            response = self.api_manager.call_with_retry(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                max_retries=3
            )
            
            if not response:
                logger.error(f"❌ REVIEW ERROR: No AI response for {product_code}")
                # Return original extraction with review flag
                return ReviewResults(
                    subprimal=previous_extraction.get('subprimal'),
                    grade=previous_extraction.get('grade'),
                    size=previous_extraction.get('size'),
                    size_uom=previous_extraction.get('size_uom'),
                    brand=previous_extraction.get('brand'),
                    bone_in=previous_extraction.get('bone_in', False),
                    confidence=max(previous_extraction.get('confidence', 0.0) - 0.2, 0.0),
                    needs_review=True,
                    miss_categorized=False
                )
            
            # Parse response using robust parser
            ai_response = ResultParser.parse_json_response(response)
            if not ai_response:
                logger.error(f"❌ REVIEW ERROR: Failed to parse AI response for {product_code}")
                # Return original with reduced confidence
                return ReviewResults(
                    subprimal=previous_extraction.get('subprimal'),
                    grade=previous_extraction.get('grade'),
                    size=previous_extraction.get('size'),
                    size_uom=previous_extraction.get('size_uom'),
                    brand=previous_extraction.get('brand'),
                    bone_in=previous_extraction.get('bone_in', False),
                    confidence=max(previous_extraction.get('confidence', 0.0) - 0.3, 0.0),
                    needs_review=True,
                    miss_categorized=False
                )
            
            # Extract fields from parsed response
            subprimal = ai_response.get("subprimal", None)
            grade = ai_response.get("grade", None)
            size = ai_response.get("size", None)
            size_uom = ai_response.get("size_uom", None)
            brand = ai_response.get("brand", None)
            bone_in = ai_response.get("bone_in", False)
            confidence = float(ai_response.get("confidence", 0.0))
            needs_review = ai_response.get("needs_review", False)
            miss_categorized = ai_response.get("miss_categorized", False)
            
            # Simple validation using reference data
            validation = self.validate_extraction({'subprimal': subprimal, 'grade': grade}, primal)
            
            # Fix Issue #1: Flag missing subprimals as miss_categorized
            if not subprimal or subprimal.strip() == "":
                miss_categorized = True
                needs_review = True
                logger.warning(f"🚨 REVIEW ALERT: Missing subprimal flagged as miss_categorized for {product_code}")
            # Check for misclassification based on validation
            elif not validation['subprimal_valid'] and confidence > 0.8:
                miss_categorized = True
                logger.warning(f"🚨 REVIEW ALERT: High confidence ({confidence:.2f}) but invalid {primal} subprimal: '{subprimal}' for {product_code}")
            # Override AI decision: if subprimal is valid, never flag as miss_categorized
            elif validation['subprimal_valid']:
                miss_categorized = False
            
            # Standardize subprimal to canonical name
            subprimal = self.standardize_to_canonical(subprimal, primal)
            
            # Standardize grade to canonical name
            grade = self.standardize_grade_to_canonical(grade)
            
            # Flag for review based on validation issues
            if not validation['subprimal_valid'] or not validation['grade_valid']:
                needs_review = True
            
            # Create result
            result = ReviewResults(
                subprimal=subprimal,
                grade=grade,
                size=size,
                size_uom=size_uom,
                brand=brand,
                bone_in=bone_in,
                confidence=confidence,
                needs_review=needs_review,
                miss_categorized=miss_categorized
            )
            
            # Log key review conclusions
            original_subprimal = previous_extraction.get('subprimal')
            changes = []
            if original_subprimal != subprimal:
                changes.append(f"subprimal: '{original_subprimal}' → '{subprimal}'")
            
            original_grade = previous_extraction.get('grade')
            if original_grade != grade:
                changes.append(f"grade: '{original_grade}' → '{grade}'")
            
            flags = []
            if miss_categorized:
                flags.append("MISS_CATEGORIZED")
            if needs_review:
                flags.append("NEEDS_REVIEW")
            
            if changes or flags:
                change_text = f" | Changes: {', '.join(changes)}" if changes else ""
                flag_text = f" | Flags: {', '.join(flags)}" if flags else ""
                logger.info(f"📝 REVIEW RESULT [{product_code}]: confidence={confidence:.2f}{change_text}{flag_text}")
            else:
                logger.info(f"✅ REVIEW PASSED [{product_code}]: No changes needed, confidence={confidence:.2f}")
            
            return result
                    
        except Exception as e:
            logger.error(f"Error analyzing {product_code}: {str(e)}")
            raise

    def standardize_to_canonical(self, subprimal: str, primal: str) -> str:
        """Standardize subprimal name to canonical form using reference data."""
        if not subprimal or not primal:
            return subprimal
            
        subprimal_lower = subprimal.lower().strip()
        
        # Find the canonical name for this subprimal
        for canonical in self.reference_data.get_subprimals(primal):
            # Check if it's already the canonical name
            if canonical.lower() == subprimal_lower:
                return canonical  # Ensure exact case match
                
            # Check synonyms for this canonical name
            synonyms = self.reference_data.get_subprimal_synonyms(primal, canonical)
            if synonyms:
                for synonym in synonyms:
                    if synonym.lower().strip() == subprimal_lower:
                        logger.info(f"🔄 STANDARDIZED: '{subprimal}' → '{canonical}' (canonical)")
                        return canonical
        
        # If no match found, keep original
        return subprimal

    def standardize_grade_to_canonical(self, grade: str) -> str:
        """Standardize grade name to canonical form using reference data."""
        if not grade:
            return grade
        
        grade_lower = grade.lower().strip()
        
        # Get canonical grades from reference data
        canonical_grades = self.reference_data.get_grades()
        
        # Find the canonical name for this grade
        for canonical in canonical_grades:
            # Check if it's already the canonical name
            if canonical.lower() == grade_lower:
                return canonical  # Ensure exact case match
                
            # Check synonyms for this canonical name
            synonyms = self.reference_data.get_grade_synonyms(canonical)
            if synonyms:
                for synonym in synonyms:
                    if synonym.lower().strip() == grade_lower:
                        logger.info(f"🔄 STANDARDIZED: '{grade}' → '{canonical}' (canonical)")
                        return canonical
        
        # If no match found, keep original
        return grade

def process_product_for_review(
    product_data: Dict[str, Any], 
    provider: str = "openai"
) -> ReviewResults:
    """
    Main function: Process a single product and generate review results.
    
    Args:
        product_data: Dict with 'product_code', 'description', 'previous_extraction'
        provider: AI provider to use
    
    Returns:
        ReviewResults object
    """    
    processor = ReviewProcessor(provider=provider)

    result = processor.analyze_product(
        description=product_data.get('description', ''),
        previous_extraction=product_data.get('previous_extraction', {}),
        product_code=product_data.get('product_code', 'UNKNOWN'),
        category=product_data.get('category', '')
    )
    return result