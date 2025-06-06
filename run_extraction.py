#!/usr/bin/env python3
"""
Run the beef cut extraction pipeline with support for manual review.

This script provides a simple command-line interface to:
1. Run the extraction process on specified beef categories
2. Generate both Parquet and Excel outputs
3. Highlight items needing manual review

Usage:
    python run_extraction.py --categories "Beef Chuck,Beef Loin" --output-dir ./outputs
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/extraction.log', mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for the extraction pipeline."""
    parser = argparse.ArgumentParser(description='Run the beef cut extraction pipeline')
    parser.add_argument(
        '--categories',
        type=str,
        default='Beef Chuck',
        help='Comma-separated list of beef categories to process (e.g., "Beef Chuck,Beef Loin")'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='./outputs',
        help='Directory to save extraction outputs'
    )
    parser.add_argument(
        '--reference-data',
        type=str,
        default='./data/incoming/beef_cuts.xlsx',
        help='Path to reference data Excel file'
    )
    parser.add_argument(
        '--test-run',
        action='store_true',
        help='Process only a limited number of records per category (for testing)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=10,
        help='Maximum number of products to process per category'
    )
    
    args = parser.parse_args()
    
    # Parse categories into a list
    categories = [category.strip() for category in args.categories.split(',')]
    logger.info(f"Categories to process: {categories}")
    
    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs('logs', exist_ok=True)
    
    # Import here to avoid circular imports
    from src.LLM.extraction_controller import ExtractionController
    
    # Initialize and run the extraction controller
    try:
        controller = ExtractionController(
            processed_dir=args.output_dir,
            reference_data_path=args.reference_data
        )
        
        # Always pass the limit parameter, regardless of test_run status
        # When limit is 0, it will process all available products
        limit = None if args.limit == 0 else args.limit
        results = controller.run_extraction(categories, limit=limit)
        
        # Print summary
        total_processed = sum(len(df) for df in results.values())
        total_needs_review = sum(df['needs_review'].sum() for df in results.values() if not df.empty)
        
        logger.info("=" * 50)
        logger.info(f"Extraction completed successfully:")
        logger.info(f"- Total categories processed: {len(results)}")
        logger.info(f"- Total records processed: {total_processed}")
        logger.info(f"- Records needing review: {total_needs_review}")
        logger.info(f"- Review Excel files saved in: {args.output_dir}")
        logger.info("=" * 50)
        
        # List the review files generated
        review_files = [f for f in os.listdir(args.output_dir) if f.endswith('_review.xlsx')]
        if review_files:
            logger.info("Review files generated:")
            for file in review_files:
                logger.info(f"- {file}")
                
        return 0
        
    except Exception as e:
        logger.error(f"Failed to run extraction: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
