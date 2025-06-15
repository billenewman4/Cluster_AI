"""
Model Caller for Evaluation Framework
Provides a clean interface to call DynamicBeefExtractor for evaluation purposes.
"""

import yaml
import logging
from pathlib import Path
from typing import List, Dict, Optional

# Fix import paths - assuming we run from project root with src on path
import sys
from pathlib import Path

# Add the project root to path so we can import from src
project_root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(project_root))

from src.AIs.llm_extraction.specific_extractors.dynamic_beef_extractor import DynamicBeefExtractor

logger = logging.getLogger(__name__)

# Load config with robust path handling
config_path = Path(__file__).resolve().parent.parent / "eval_config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

reference_data_path = config["reference_data_path"]
processed_dir = config["processed_dir"]

def call_model(product_descriptions: str, primal: str = "Chuck") -> List[Dict[str, Optional[str]]]:
    """
    Call the dynamic beef extractor on a list of product descriptions.
    Initializes the extractor once and processes all descriptions sequentially.
    
    Args:
        product_descriptions: List of product descriptions to extract data from
        
    Returns:
        List of dicts with subprimal_pred and grade_pred keys
    """
    # Initialize extractor once for the entire list
    logger.info(f"Initializing DynamicBeefExtractor with reference_data_path={reference_data_path}")
    extractor = DynamicBeefExtractor(
        reference_data_path=reference_data_path, 
        processed_dir=processed_dir
    )

    #set the primal
    extractor.set_primal(primal)
    
    results = []

    try:
        result = extractor.extract(product_descriptions)
        results.append({
            "product_description": product_descriptions,
            "subprimal_pred": result.subprimal,
            "grade_pred": result.grade
        })
    except Exception as e:
        logger.warning(f"Extraction failed for '{product_descriptions[:50]}...': {e}")
        results.append({
            "subprimal_pred": None,
            "grade_pred": None
        })
    
    return results
 
def call_model_on_list(product_descriptions: List[str], primal: str = "Chuck") -> List[Dict[str, Optional[str]]]:
    """
    Call the dynamic beef extractor on a list of product descriptions.
    Initializes the extractor once and processes all descriptions sequentially.
    
    Args:
        product_descriptions: List of product descriptions to extract data from
        
    Returns:
        List of dicts with subprimal_pred and grade_pred keys
    """
    # Initialize extractor once for the entire list
    logger.info(f"Initializing DynamicBeefExtractor with reference_data_path={reference_data_path}")
    extractor = DynamicBeefExtractor(
        reference_data_path=reference_data_path, 
        processed_dir=processed_dir
    )

    #set the primal
    extractor.set_primal(primal)
    
    results = []
    
    for i, product_description in enumerate(product_descriptions):
        try:
            result = extractor.extract(product_description)
            
            results.append({
                "product_description": product_description,
                "subprimal_pred": result.subprimal,
                "grade_pred": result.grade
            })
            
            if (i + 1) % 10 == 0:  # Log progress every 10 items
                logger.info(f"Processed {i + 1}/{len(product_descriptions)} descriptions")
                
        except Exception as e:
            logger.warning(f"Extraction failed for '{product_description[:50]}...': {e}")
            results.append({
                "subprimal_pred": None,
                "grade_pred": None
            })
    
    logger.info(f"Completed processing {len(product_descriptions)} descriptions")
    return results