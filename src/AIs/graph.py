"""
LangGraph Workflow for Beef Product Processing Pipeline
Orchestrates: Dynamic Beef Extractor 
"""

import logging
from typing import Dict, Any, List, Optional, TypedDict
from dataclasses import asdict
from pydantic import BaseModel, Field

from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages

# Import specialized extractors and their result models
from .llm_extraction.specific_extractors.dynamic_beef_extractor import DynamicBeefExtractor, SubprimalExtractionResult
from .llm_extraction.specific_extractors.grade_extractor import GradeExtractor, GradeExtractionResult
from .llm_extraction.specific_extractors.usda_codes_extractor import USDACodesExtractor, USDACodeExtractionResult

logger = logging.getLogger(__name__)

# Define unified extraction result Pydantic model to combine all extractor outputs
class BeefExtractionResult(BaseModel):
    """Unified extraction result model combining outputs from all specialized extractors."""
    # Primal is known externally (from category or user input) - not extracted
    primal: Optional[str] = Field(None, description="The primal cut (set externally, not extracted)")
    subprimal: Optional[str] = Field(None, description="The identified subprimal")
    
    # From GradeExtractionResult
    grade: Optional[str] = Field(None, description="The identified grade")
    
    # From USDACodeExtractionResult
    usda_code: Optional[str] = Field(None, description="The identified USDA code")
    
    # Common metadata fields
    confidence: float = Field(0.0, description="Overall confidence level in the extraction")
    needs_review: bool = Field(True, description="Whether human review is needed")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary for backward compatibility."""
        return self.model_dump()
        
class ProcessingState(TypedDict):
    """State schema for the processing pipeline."""
    # Input data
    product_code: str
    product_description: str
    category: Optional[str]
    
    # Extraction results
    extraction_result: Optional[Dict[str, Any]]
    
    # Processing metadata
    current_step: str
    errors: List[str]
    processing_complete: bool

class BeefProcessingWorkflow:
    """LangGraph workflow for processing beef products through extraction."""
    
    def __init__(self, provider: str = "openai"):
        """Initialize the workflow with AI components."""
        self.provider = provider
        
        # Initialize components
        try:
            # Initialize specialized extractors
            self.subprimal_extractor = DynamicBeefExtractor()
            self.grade_extractor = GradeExtractor()
            self.usda_extractor = USDACodesExtractor()
            logger.info("Initialized all extraction components successfully")
        except Exception as e:
            logger.error(f"Failed to initialize workflow components: {e}")
            raise
        
        # Build the graph
        self.workflow = self._build_graph()
    
    def _build_graph(self) -> StateGraph:
        """Build the LangGraph workflow."""
        # Define the graph
        workflow = StateGraph(ProcessingState)
        
        # Add nodes
        workflow.add_node("extract", self._extraction_node)
        workflow.add_node("complete", self._complete_node)
        
        # Define the flow with simple sequential logic
        workflow.set_entry_point("extract")
        
        # After extraction, complete the workflow
        workflow.add_edge("extract", "complete")
        workflow.add_edge("complete", END)
        
        return workflow.compile()
    
    def _extraction_node(self, state: ProcessingState) -> ProcessingState:
        """Node 1: Extract data using specialized extractors."""
        logger.debug(f"Starting extraction for product: {state['product_code']}")
        
        try:
            # Initialize the unified extraction result
            unified_result = BeefExtractionResult()
            description = state['product_description']
            
            # Set primal if category is available
            if state.get('category'):
                primal = self.subprimal_extractor.infer_primal_from_category(state['category'])
                if primal:
                    self.subprimal_extractor.set_primal(primal)
                    logger.debug(f"Set primal to: {primal}")
            
            # 1. Extract subprimal using DynamicBeefExtractor
            subprimal_result = self.subprimal_extractor.extract(description)
            if subprimal_result:
                # Set primal from the extractor's current_primal setting (not from extraction result)
                unified_result.primal = self.subprimal_extractor.current_primal
                unified_result.subprimal = subprimal_result.subprimal
                # Start with subprimal extractor's confidence
                unified_result.confidence = subprimal_result.confidence
                unified_result.needs_review = subprimal_result.needs_review
            
            # 2. Extract grade using GradeExtractor
            grade_result = self.grade_extractor.extract(description)
            if grade_result:
                unified_result.grade = grade_result.grade
                # Update confidence (simple average for now)
                if grade_result.confidence > 0:
                    unified_result.confidence = (unified_result.confidence + grade_result.confidence) / 2
                # Need review if either extractor flags for review
                unified_result.needs_review = unified_result.needs_review or grade_result.needs_review
            
            # 3. Extract USDA code using USDACodesExtractor
            usda_result = self.usda_extractor.extract(description)
            if usda_result:
                unified_result.usda_code = usda_result.usda_code
                # Update confidence if we have a code
                if usda_result.confidence > 0:
                    unified_result.confidence = (unified_result.confidence * 2 + usda_result.confidence) / 3
                # Need review if any extractor flags for review
                unified_result.needs_review = unified_result.needs_review or usda_result.needs_review
            
            # Convert unified result to dictionary for state update (backward compatibility)
            state['extraction_result'] = unified_result.to_dict()
            state['current_step'] = 'extraction_complete'
            
            logger.debug(f"Extraction completed for {state['product_code']}: confidence={unified_result.confidence:.2f}")
            
        except Exception as e:
            error_msg = f"Extraction failed: {str(e)}"
            logger.error(error_msg)
            state['errors'].append(error_msg)
            # Use empty BeefExtractionResult for errors
            state['extraction_result'] = BeefExtractionResult(needs_review=True).to_dict()
        
        return state
    
    def _complete_node(self, state: ProcessingState) -> ProcessingState:
        """Complete processing after extraction."""
        logger.info(f"Completing processing for {state['product_code']}")
        
        # Mark the process as complete
        state['current_step'] = 'completed'
        state['processing_complete'] = True
        
        logger.info(f"Processing completed for {state['product_code']}")
        
        return state
    
    def process_product(
        self, 
        product_code: str, 
        product_description: str, 
        category: Optional[str] = None
    ) -> Dict[str, Any]:
        """Process a single product through the extraction pipeline.
        
        Args:
            product_code: Unique identifier for the product
            product_description: Product description text
            category: Optional category (e.g., 'Beef Chuck') to help with primal identification
            
        Returns:
            Dictionary containing extraction results
        """
        logger.info(f"Starting extraction processing for product: {product_code}")
        
        # Initialize state
        initial_state = ProcessingState(
            product_code=product_code,
            product_description=product_description,
            category=category,
            extraction_result=None,
            current_step='initialized',
            errors=[],
            processing_complete=False
        )
        
        try:
            # Run the workflow
            final_state = self.workflow.invoke(initial_state)
            
            # Prepare result
            result = {
                'product_code': final_state['product_code'],
                'product_description': final_state['product_description'],
                'category': final_state.get('category'),
                'extraction_result': final_state['extraction_result'],
                'processing_steps_completed': final_state['current_step'],
                'errors': final_state['errors'],
                'processing_complete': final_state['processing_complete']
            }
            
            logger.info(f"Pipeline processing completed for {product_code}")
            return result
            
        except Exception as e:
            error_msg = f"Pipeline processing failed for {product_code}: {str(e)}"
            logger.error(error_msg)
            
            return {
                'product_code': product_code,
                'product_description': product_description,
                'category': category,
                'extraction_result': None,
                'processing_steps_completed': 'failed',
                'errors': [error_msg],
                'processing_complete': False
            }
    
    def process_batch(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process a batch of products through the workflow."""
        logger.info(f"🚀 WORKFLOW: Processing {len(products)} products")
        
        results = []
        for i, product in enumerate(products):
            if i % 50 == 0:  # Log progress every 50 items
                logger.info(f"📊 PROGRESS: {i+1}/{len(products)} products processed")
            
            result = self.process_product(
                product_code=product.get('product_code', f'BATCH_ITEM_{i}'),
                product_description=product.get('product_description', ''),
                category=product.get('category')
            )
            results.append(result)
        
        logger.info(f"✅ WORKFLOW: Completed batch processing - {len(results)} products processed")
        return results

# Convenience function for single product processing
def process_beef_product(
    product_code: str,
    product_description: str,
    category: Optional[str] = None,
    provider: str = "openai"
) -> Dict[str, Any]:
    """Convenience function to process a single beef product.
    
    Args:
        product_code: Unique identifier for the product
        product_description: Product description text
        category: Optional category to help with primal identification
        provider: AI provider to use
        
    Returns:
        Extraction results
    """
    workflow = BeefProcessingWorkflow(provider=provider)
    return workflow.process_product(product_code, product_description, category)

# Convenience function for batch processing
def process_beef_products_batch(
    products: List[Dict[str, Any]],
    provider: str = "openai"
) -> List[Dict[str, Any]]:
    """Convenience function to process multiple beef products.
    
    Args:
        products: List of product dictionaries
        provider: AI provider to use
        
    Returns:
        List of processing results
    """
    workflow = BeefProcessingWorkflow(provider=provider)
    return workflow.process_batch(products)
