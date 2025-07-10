"""
Model Caller for Evaluation Framework
Provides a clean interface to call the BeefProcessingWorkflow for evaluation purposes.
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

# Import the workflow directly
from src.AIs.graph import BeefProcessingWorkflow

logger = logging.getLogger(__name__)

# Load config with robust path handling
config_path = Path(__file__).resolve().parent.parent / "eval_config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

reference_data_path = config["reference_data_path"]
processed_dir = config["processed_dir"]

 
def call_model_on_list(product_descriptions: List[str], primal: str = "Chuck", product_codes: List[str] = None) -> List[Dict[str, Optional[str]]]:
    """
    Call the beef processing workflow on a list of product descriptions.
    Initializes the workflow once and processes all descriptions sequentially.
    
    Args:
        product_descriptions: List of product descriptions to extract data from
        primal: Primal cut for the beef products
        product_codes: Optional list of product codes (identifiers). If not provided, codes will be generated.
        
    Returns:
        List of dicts with extraction results (primal, subprimal, grade, usda_code)
    """
    # Initialize the BeefProcessingWorkflow once for the entire list
    logger.info("Initializing BeefProcessingWorkflow with default provider")
    
    # Create workflow (note: BeefProcessingWorkflow initializes extractors with default paths)
    workflow = BeefProcessingWorkflow(provider="openai")
    
    # Generate product codes if not provided
    if product_codes is None:
        product_codes = [f"test-{i}" for i in range(len(product_descriptions))]
    
    # Ensure product_codes and product_descriptions have the same length
    if len(product_codes) != len(product_descriptions):
        logger.warning(f"Number of product codes ({len(product_codes)}) doesn't match number of descriptions ({len(product_descriptions)}). Using generated codes.")
        product_codes = [f"test-{i}" for i in range(len(product_descriptions))]
    
    results = []
    
    # Process each description
    for i, (desc, code) in enumerate(zip(product_descriptions, product_codes)):
        logger.info(f"Processing description {i+1}/{len(product_descriptions)}: {desc[:50]}...")
        
        try:
            # Process through workflow
            result = workflow.process_product(
                product_code=code,
                product_description=desc,
                category=primal
            )
            
            # Extract unified result fields
            results.append({
                "product_description": desc,
                "product_code": code,
                "primal": result.get("extraction_result", {}).get("primal"),
                "subprimal_pred": result.get("extraction_result", {}).get("subprimal"),
                "grade_pred": result.get("extraction_result", {}).get("grade"),
                "usda_code_pred": result.get("extraction_result", {}).get("usda_code"),
                "needs_review": result.get("extraction_result", {}).get("needs_review", True)
            })
            
            if (i + 1) % 10 == 0:  # Log progress every 10 items
                logger.info(f"Processed {i + 1}/{len(product_descriptions)} descriptions")
                
        except Exception as e:
            logger.warning(f"Processing failed for '{desc[:50]}...': {e}")
            logger.warning(f"Processing failed for '{product_description[:50]}...': {e}")
            results.append({
                "primal": primal,
                "subprimal_pred": None,
                "grade_pred": None,
                "usda_code_pred": None,
                "needs_review": True
            })
            
    logger.info(f"Completed processing {len(product_descriptions)} descriptions")
    return results