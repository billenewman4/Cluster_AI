"""
LangGraph Workflow for Beef Product Processing Pipeline
Orchestrates: Dynamic Beef Extractor -> Clarification Processor -> Review AI
"""

import logging
from typing import Dict, Any, List, Optional, TypedDict
from dataclasses import asdict

from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages

from .llm_extraction.specific_extractors.dynamic_beef_extractor import DynamicBeefExtractor
from .Calrifications.clarification_processor import ClarificationProcessor, process_products_for_clarification
from .review.review_AI import ReviewProcessor, process_product_for_review

logger = logging.getLogger(__name__)

class ProcessingState(TypedDict):
    """State schema for the processing pipeline."""
    # Input data
    product_code: str
    product_description: str
    category: Optional[str]
    
    # Extraction results
    initial_extraction: Optional[Dict[str, Any]]
    
    # Clarification results
    clarification_questions: Optional[List[str]]
    
    # Review results
    final_extraction: Optional[Dict[str, Any]]
    
    # Processing metadata
    current_step: str
    errors: List[str]
    processing_complete: bool

class BeefProcessingWorkflow:
    """LangGraph workflow for processing beef products through extraction, clarification, and review."""
    
    def __init__(self, provider: str = "openai"):
        """Initialize the workflow with AI components."""
        self.provider = provider
        
        # Initialize components
        try:
            self.extractor = DynamicBeefExtractor()
            self.clarification_processor = ClarificationProcessor(provider=provider)
            self.review_processor = ReviewProcessor(provider=provider)
            logger.info("Initialized all workflow components successfully")
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
        workflow.add_node("clarify", self._clarification_node)
        workflow.add_node("complete_without_review", self._complete_without_review_node)
        
        # Define the flow with conditional logic
        workflow.set_entry_point("extract")
        
        # After extraction, decide whether to continue with clarification/review
        workflow.add_conditional_edges(
            "extract",
            self._should_review_decision,
            {
                "review_needed": "clarify",
                "no_review_needed": "complete_without_review"
            }
        )
        
        # If clarification runs, always go to review
        workflow.add_edge("clarify", "complete_without_review")
        workflow.add_edge("complete_without_review", END)
        
        return workflow.compile()
    
    def _extraction_node(self, state: ProcessingState) -> ProcessingState:
        """Node 1: Extract initial data using Dynamic Beef Extractor."""
        logger.debug(f"Starting extraction for product: {state['product_code']}")
        
        try:
            # Set primal if category is available
            if state.get('category'):
                primal = self.extractor.infer_primal_from_category(state['category'])
                if primal:
                    self.extractor.set_primal(primal)
                    logger.debug(f"Set primal to: {primal}")
            
            # Perform extraction
            result = self.extractor.extract(state['product_description'])
            
            # Convert ExtractionResult to dictionary - INCLUDE primal field
            extraction_dict = {
                'primal': result.primal,
                'subprimal': result.subprimal,
                'grade': result.grade,
                'size': result.size,
                'size_uom': result.size_uom,
                'brand': result.brand,
                'bone_in': result.bone_in,
                'confidence': result.confidence,
                'needs_review': result.needs_review
            }
            
            # Update state
            state['initial_extraction'] = extraction_dict
            state['current_step'] = 'extraction_complete'
            
            logger.debug(f"Extraction completed for {state['product_code']}: confidence={result.confidence:.2f}")
            
        except Exception as e:
            error_msg = f"Extraction failed: {str(e)}"
            logger.error(error_msg)
            state['errors'].append(error_msg)
            state['initial_extraction'] = {
                'primal': None,
                'subprimal': None,
                'grade': None,
                'size': None,
                'size_uom': None,
                'brand': None,
                'bone_in': False,
                'confidence': 0.0,
                'needs_review': True
            }
        
        return state
    
    def _clarification_node(self, state: ProcessingState) -> ProcessingState:
        """Node 2: Generate clarification questions if needed."""
        logger.debug(f"Starting clarification for product: {state['product_code']}")
        
        try:
            # Generate clarification questions based on initial extraction
            clarification_result = self.clarification_processor.analyze_product(
                description=state['product_description'],
                previous_extraction=state['initial_extraction'],
                product_code=state['product_code']
            )
            
            # Update state with questions
            state['clarification_questions'] = clarification_result.questions
            state['current_step'] = 'clarification_complete'
            
            logger.debug(f"Clarification completed for {state['product_code']}: {len(clarification_result.questions)} questions generated")
            
        except Exception as e:
            error_msg = f"Clarification failed: {str(e)}"
            logger.error(error_msg)
            state['errors'].append(error_msg)
            state['clarification_questions'] = []
        
        return state
    
    def _review_node(self, state: ProcessingState) -> ProcessingState:
        """Node 3: Review and correct extraction using Review AI."""
        logger.info(f"🔍 REVIEW: Starting review for product {state['product_code']}")
        
        try:
            # Use the existing review processor instance directly
            review_result = self.review_processor.analyze_product(
                description=state['product_description'],
                previous_extraction=state['initial_extraction'],
                product_code=state['product_code'],
                category=state.get('category', '')
            )
            
            # Convert ReviewResults to dictionary - INCLUDE primal field
            final_extraction = {
                'primal': state['initial_extraction'].get('primal'),  # Preserve primal from initial extraction
                'subprimal': review_result.subprimal,
                'grade': review_result.grade,
                'size': review_result.size,
                'size_uom': review_result.size_uom,
                'brand': review_result.brand,
                'bone_in': review_result.bone_in,
                'confidence': review_result.confidence,
                'needs_review': review_result.needs_review,
                'miss_categorized': review_result.miss_categorized
            }
            
            # Update state
            state['final_extraction'] = final_extraction
            state['current_step'] = 'review_complete'
            state['processing_complete'] = True
            
            logger.debug(f"✅ REVIEW: Completed for {state['product_code']}")
            
        except Exception as e:
            error_msg = f"Review failed: {str(e)}"
            logger.error(f"❌ REVIEW ERROR: {error_msg} for {state['product_code']}")
            state['errors'].append(error_msg)
            # Use initial extraction as fallback
            state['final_extraction'] = state['initial_extraction'].copy()
            state['final_extraction']['needs_review'] = True
            state['processing_complete'] = True
        
        return state
    
    def _should_review_decision(self, state: ProcessingState) -> str:
        """Decision function to determine if clarification/review is needed."""
        initial_extraction = state.get('initial_extraction', {})
        needs_review = initial_extraction.get('needs_review', True)
        confidence = initial_extraction.get('confidence', 0.0)
        
        if needs_review:
            logger.info(f"🔄 WORKFLOW: {state['product_code']} → Full review (confidence={confidence:.2f})")
            return "review_needed"
        else:
            logger.info(f"⚡ WORKFLOW: {state['product_code']} → Skip review (confidence={confidence:.2f})")
            return "no_review_needed"
    
    def _complete_without_review_node(self, state: ProcessingState) -> ProcessingState:
        """Complete processing without review when AI is confident."""
        logger.info(f"Completing processing for {state['product_code']} without review")
        
        # Use initial extraction as final extraction
        state['final_extraction'] = state['initial_extraction'].copy()
        state['clarification_questions'] = []  # No questions needed
        state['current_step'] = 'completed_without_review'
        state['processing_complete'] = True
        
        logger.info(f"Processing completed for {state['product_code']} without additional review")
        
        return state
    
    def process_product(
        self, 
        product_code: str, 
        product_description: str, 
        category: Optional[str] = None
    ) -> Dict[str, Any]:
        """Process a single product through the complete pipeline.
        
        Args:
            product_code: Unique identifier for the product
            product_description: Product description text
            category: Optional category (e.g., 'Beef Chuck') to help with primal identification
            
        Returns:
            Dictionary containing all processing results
        """
        logger.info(f"Starting pipeline processing for product: {product_code}")
        
        # Initialize state
        initial_state = ProcessingState(
            product_code=product_code,
            product_description=product_description,
            category=category,
            initial_extraction=None,
            clarification_questions=None,
            final_extraction=None,
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
                'initial_extraction': final_state['initial_extraction'],
                'clarification_questions': final_state['clarification_questions'],
                'final_extraction': final_state['final_extraction'],
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
                'initial_extraction': None,
                'clarification_questions': None,
                'final_extraction': None,
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
        Complete processing results
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
