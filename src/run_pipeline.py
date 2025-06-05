#!/usr/bin/env python3
"""
Main pipeline orchestration script.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent))

from data_ingestion.data_processor import DataProcessor
from llm_extraction.beef_chuck_extractor import BeefChuckExtractor
from llm_extraction.batch_processor import BatchProcessor
from output_generation.file_writer import FileWriter
from output_generation.report_generator import ReportGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Run the meat inventory pipeline')
    parser.add_argument('--categories', 
                       default='Beef Chuck',
                       help='Comma-separated list of categories to process')
    parser.add_argument('--skip-stage1', 
                       action='store_true',
                       help='Skip data ingestion stage')
    parser.add_argument('--test-run',
                       action='store_true', 
                       help='Process only first 10 records for testing')
    
    args = parser.parse_args()
    
    # Parse categories
    categories = [cat.strip() for cat in args.categories.split(',')]
    
    try:
        # Create necessary directories
        os.makedirs('logs', exist_ok=True)
        os.makedirs('outputs', exist_ok=True)
        
        logger.info("🚀 Starting Meat Inventory Pipeline")
        logger.info(f"Categories to process: {categories}")
        
        # Stage 1: Data Ingestion
        if not args.skip_stage1:
            logger.info("📥 Stage 1: Data Ingestion")
            ingestion_pipeline = DataProcessor()
            ingestion_pipeline.run()
        else:
            logger.info("⏭️  Skipping Stage 1: Data Ingestion")
        
        # Stage 2: LLM Extraction
        logger.info("🤖 Stage 2: LLM Extraction")
        
        # Initialize extractors dictionary
        extractors = {}
        for category in categories:
            if category.lower() == 'beef chuck':
                extractors['beef chuck'] = BeefChuckExtractor()
            else:
                logger.warning(f"No extractor available for category: {category}")
        
        if not extractors:
            logger.error("No extractors configured. Exiting.")
            return 1
        
        # Initialize batch processor with optimizations
        batch_processor = BatchProcessor(extractors=extractors)
        
        # Process each category
        all_results = []
        for category in categories:
            if category.lower() not in extractors:
                logger.warning(f"Skipping {category} - no extractor configured")
                continue
                
            logger.info(f"Processing category: {category}")
            
            # Load and filter data
            import pandas as pd
            df = pd.read_parquet('data/processed/inventory_base.parquet')
            
            # Filter for category
            filtered_df = df[df['category_description'].str.lower() == category.lower()]
            logger.info(f"Found {len(filtered_df)} records for {category}")
            
            if len(filtered_df) == 0:
                logger.warning(f"No records found for {category}")
                continue
            
            # Test run - limit to first 10 records
            if args.test_run:
                filtered_df = filtered_df.head(10)
                logger.info(f"Test run: Processing only {len(filtered_df)} records")
            
            # Process with new optimized batch processor
            try:
                result_df = batch_processor.process_batch(filtered_df, category)
                all_results.append(result_df)
                logger.info(f"Successfully processed {len(result_df)} records for {category}")
                
            except Exception as e:
                logger.error(f"Failed to process {category}: {e}")
                continue
        
        if not all_results:
            logger.error("No results to process. Exiting.")
            return 1
        
        # Stage 3: Output Generation
        logger.info("📤 Stage 3: Output Generation")
        
        # Combine all results
        combined_df = pd.concat(all_results, ignore_index=True)
        
        # Generate outputs using modular components
        file_writer = FileWriter()
        report_generator = ReportGenerator()
        
        # Organize results by category for file writer
        results_by_category = {}
        for category in categories:
            category_data = combined_df  # For simplicity, using all data
            results_by_category[category] = category_data
        
        # Write output files
        output_files = file_writer.write_all_outputs(results_by_category)
        
        # Simple summary instead of complex report generator
        logger.info(f"Output files created: {list(output_files.keys())}")
        
        # Summary statistics
        total_records = len(combined_df)
        needs_review = combined_df['needs_review'].sum() if 'needs_review' in combined_df.columns else 0
        avg_confidence = combined_df['confidence'].mean() if 'confidence' in combined_df.columns else 0.0
        
        logger.info("✅ Pipeline Complete!")
        logger.info(f"📊 Summary:")
        logger.info(f"   Total records processed: {total_records}")
        logger.info(f"   Records needing review: {needs_review} ({needs_review/total_records*100:.1f}%)")
        logger.info(f"   Average confidence: {avg_confidence:.2f}")
        
        # Return exit code based on flagged results
        if needs_review > 0:
            logger.warning("Some records need review - check flagged output files")
            return 1
        else:
            return 0
            
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return 1

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code) 