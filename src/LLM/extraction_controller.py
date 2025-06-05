"""
Extraction Controller Module
Orchestrates extraction workflow across categories.
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional

import pandas as pd

from .extractors.dynamic_beef_extractor import DynamicBeefExtractor
from ..data_ingestion.utils.reference_data_loader import ReferenceDataLoader

# Configure logging
logger = logging.getLogger(__name__)

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
        
        # Load reference data
        self.reference_data = ReferenceDataLoader(reference_data_path)
        
        # Initialize dynamic beef extractor for all primals (including Chuck)
        self.dynamic_beef_extractor = DynamicBeefExtractor(reference_data_path, processed_dir)
        
        # Get all supported primal cuts
        supported_primals = self.dynamic_beef_extractor.get_supported_primals()
        
        # Category mapping for dispatching - build dynamically from reference data
        self.category_extractors = {
            # Map each beef primal to the dynamic extractor (including Chuck)
            f'Beef {primal}': self.dynamic_beef_extractor for primal in supported_primals
        }
        
        logger.info(f"Initialized extraction controller with {len(self.category_extractors)} category extractors")
    
    def extract_batch(self, 
                      df: pd.DataFrame, 
                      category_column: str = 'Category',
                      description_column: str = 'Description',
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
            
            # Check if we have a direct extractor match
            if category in self.category_extractors:
                extractor = self.category_extractors[category]
                primal = category.replace('Beef ', '') if category.startswith('Beef ') else None
            else:
                # For categories we don't recognize, try to infer the primal
                # or use the dynamic extractor without specifying a primal
                logger.info(f"No direct extractor found for category: {category}, using dynamic extractor")
                extractor = self.dynamic_beef_extractor
                primal = None
                
                # Try to identify if this is a beef category
                if 'beef' in category.lower() or 'steak' in category.lower():
                    # Try to match a primal from the category name
                    for known_primal in self.reference_data.get_primals():
                        if known_primal.lower() in category.lower():
                            primal = known_primal
                            logger.info(f"Inferred primal {primal} for category: {category}")
                            break
            
            # Get products for this category
            category_df = df[df[category_column] == category]
            
            # Process in batches
            total_batches = (len(category_df) - 1) // batch_size + 1
            for i in range(0, len(category_df), batch_size):
                batch = category_df.iloc[i:i+batch_size]
                descriptions = batch[description_column].tolist()
                
                logger.info(f"Processing batch {i//batch_size + 1}/{total_batches} ({len(descriptions)} records)")
                
                # Extract data - if we're using dynamic extractor, pass the primal if we know it
                if extractor == self.dynamic_beef_extractor and primal:
                    batch_results = extractor.extract_batch(descriptions, primal=primal)
                else:
                    batch_results = extractor.extract_batch(descriptions)
                
                # Append results
                for result in batch_results:
                    if result.successful:
                        results.append({
                            'Description': result.description,
                            'Category': category,
                            'Primal': result.primal,
                            'Extracted': result.extracted_data,
                            'Success': True,
                            'Error': None
                        })
                    else:
                        results.append({
                            'Description': result.description,
                            'Category': category,
                            'Primal': result.primal if hasattr(result, 'primal') else None,
                            'Extracted': {},
                            'Success': False,
                            'Error': result.error
                        })
        
        # Convert results to DataFrame
        return pd.DataFrame(results)
        
    def extract_single(self, description: str, category: str) -> Dict[str, Any]:
        """
        Extract information from a single product description.
        
        Args:
            description: Product description text
            category: Product category
            
        Returns:
            Dictionary with extracted information
        """
        # Try to get an extractor for this category
        if category in self.category_extractors:
            extractor = self.category_extractors[category]
            primal = category.replace('Beef ', '') if category.startswith('Beef ') else None
            
            # For all beef primals, use dynamic extractor with primal hint
            if primal:
                result = self.dynamic_beef_extractor.extract(description, primal=primal)
            # Otherwise use dynamic extractor without hint
            else:
                result = self.dynamic_beef_extractor.extract(description)
        else:
            # For unknown categories, use dynamic extractor
            logger.warning(f"No extractor found for category: {category}, using dynamic extractor")
            result = self.dynamic_beef_extractor.extract(description)
        
        if result.successful:
            return result.extracted_data
        else:
            logger.error(f"Extraction failed: {result.error}")
            return {}
    
    def run_extraction(self, categories: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """Run extraction for specified categories.
        
        Args:
            categories: List of categories to process, or None for all available
            
        Returns:
            Dict[str, pd.DataFrame]: Results for each category
        """
        logger.info("Starting LLM extraction process")
        
        # If no categories specified, use all available extractors
        if not categories:
            categories = list(self.category_extractors.keys())
            
        results = {}
        
        for category in categories:
            category_lower = category.lower()
            
            if category_lower not in self.category_extractors:
                logger.warning(f"No extractor available for category: {category}")
                continue
                
            try:
                logger.info(f"Processing category: {category}")
                extractor = self.category_extractors[category_lower]
                category_df = extractor.process_category(category)
                results[category] = category_df
                
                if len(category_df) > 0:
                    # Log extraction stats
                    needs_review_count = category_df['needs_review'].sum()
                    avg_confidence = category_df['llm_confidence'].mean()
                    
                    logger.info(f"Successfully processed {len(category_df)} records for {category}")
                    logger.info(f"Average confidence: {avg_confidence:.3f}")
                    logger.info(f"Records needing review: {needs_review_count}")
                    
                    # Save results to file
                    output_file = self.processed_dir / f"extracted_{category.lower().replace(' ', '_')}.parquet"
                    category_df.to_parquet(output_file, index=False)
                    logger.info(f"Saved extraction results to {output_file}")
                
            except Exception as e:
                logger.error(f"Failed to process category {category}: {str(e)}")
                results[category] = pd.DataFrame()
        
        return results


def main():
    """Main entry point for LLM extraction stage."""
    controller = ExtractionController()
    results = controller.run_extraction(["Beef Chuck"])
    
    # Output summary statistics
    for category, df in results.items():
        if not df.empty:
            print(f"\n{category}: {len(df)} records processed")
            print(f"Average confidence: {df['llm_confidence'].mean():.3f}")
            print(f"Records needing review: {df['needs_review'].sum()}")
    
    print("\nLLM extraction completed")


if __name__ == "__main__":
    main()
