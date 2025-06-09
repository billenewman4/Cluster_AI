"""
Extraction Controller Module
Orchestrates extraction workflow across categories.
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional

import pandas as pd

from .dynamic_beef_extractor import DynamicBeefExtractor

# Import reference data loader with absolute import to avoid relative import issues
try:
    from data_ingestion.utils.reference_data_loader import ReferenceDataLoader
except ImportError:
    # Fallback for different import contexts
    from src.data_ingestion.utils.reference_data_loader import ReferenceDataLoader

# Configure logging
logger = logging.getLogger(__name__)

VALID_SIZE_UNITS = {'oz', 'lb', 'g', 'kg', 'in', 'inch', 'inches'}

class ExtractionController:
    """
    Main controller for LLM-based extraction workflows.
    
    Orchestrates extraction across different product categories and 
    handles data loading/saving.
    """
    
    def __init__(self, processed_dir: str = "data/processed", 
                reference_data_path: str = "data/incoming/beef_cuts.xlsx"):
        """
        Initialize the extraction controller.
        
        Args:
            processed_dir: Directory with processed data files
            reference_data_path: Path to the reference data Excel file
        """
        
        self.processed_dir = Path(processed_dir)
        self.reference_data_path = reference_data_path
        
        try:
            # Load reference data
            self.reference_data = ReferenceDataLoader(reference_data_path)
            
            # Initialize dynamic beef extractor - handles all beef categories
            self.beef_extractor = DynamicBeefExtractor(reference_data_path, processed_dir)
        except Exception as e:
            logger.error(f"Failed to initialize extraction controller: {e}")
            raise RuntimeError(f"Cannot initialize extraction controller: {e}")
        
        logger.info(f"Initialized extraction controller with dynamic beef extractor")
    
    def extract_batch(self, 
                      df: pd.DataFrame, 
                      category_column: str = 'productcategory',
                      description_column: str = 'product_description',
                      batch_size: int = 20) -> pd.DataFrame:
        """
        Extract information from a batch of products in a DataFrame.
        
        Args:
            df: DataFrame containing product data
            category_column: Column name for product category
            description_column: Column name for product description
            batch_size: Batch size for processing
            
        Returns:
            DataFrame with extraction results
        """
        results = []
        
        # Group by category
        categories = df[category_column].unique()
        
        for category in categories:
            logger.info(f"Processing category: {category}")
            
            # Get products for this category
            category_df = df[df[category_column] == category]
            
            # Process in batches
            total_batches = (len(category_df) - 1) // batch_size + 1
            for i in range(0, len(category_df), batch_size):
                batch = category_df.iloc[i:i+batch_size]
                descriptions = batch[description_column].tolist()
                
                logger.info(f"Processing batch {i//batch_size + 1}/{total_batches} ({len(descriptions)} records)")
                
                # Extract data - if we're using dynamic extractor, pass the primal if we know it
                batch_results = self.beef_extractor.extract_batch(descriptions)
                
                # Append results
                for idx, result in enumerate(batch_results):
                    # Determine if extraction was successful
                    # Consider it successful if it has some extracted data and doesn't need review
                    is_successful = (
                        not result.needs_review and 
                        (result.subprimal is not None or result.grade is not None)
                    )
                    
                    extracted_data = {
                        'subprimal': result.subprimal,
                        'grade': result.grade,
                        'size': result.size,
                        'size_uom': result.size_uom,
                        'brand': result.brand,
                        'bone_in': result.bone_in,
                        'confidence': result.confidence
                    }
                    
                    if is_successful:
                        results.append({
                            'Description': descriptions[idx],  # Get original description by index
                            'Category': category,
                            'Extracted': extracted_data,
                            'Success': True,
                            'Error': None
                        })
                    else:
                        results.append({
                            'Description': descriptions[idx],  # Get original description by index
                            'Category': category,
                            'Extracted': extracted_data,
                            'Success': False,
                            'Error': 'Extraction needs review or incomplete data'
                        })
        
        # Convert results to DataFrame
        return pd.DataFrame(results)
        