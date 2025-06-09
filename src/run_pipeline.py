#!/usr/bin/env python3
"""
Main pipeline orchestration script.
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add project root to path to ensure all modules can be found
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))
sys.path.append(str(Path(__file__).parent))

from llm_extraction.extraction_controller import ExtractionController
from output_generation.file_writer import FileWriter
from data_ingestion.processor import DataProcessor

# Optional Firebase functionality
try:
    from database.excel_to_firestore import ExcelToFirestore
    firebase_available = True
except ImportError:
    firebase_available = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline.log'),
        logging.StreamHandler()
    ]
)

# Set OpenAI and LLM extraction modules to DEBUG level for more API call details
#logging.getLogger('openai').setLevel(logging.DEBUG)
#logging.getLogger('llm_extraction').setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Run the meat inventory pipeline',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    # Core pipeline options
    parser.add_argument(
        '--categories', 
        default='Beef Chuck',
        help='Comma-separated list of categories to process'
    )
    parser.add_argument(
        '--test-run',
        action='store_true', 
        help='Process only first 10 records for testing'
    )
    parser.add_argument(
        '--upload-to-firebase',
        action='store_true',
        help='Upload master Excel file to Firebase Firestore'
    )
    
    args = parser.parse_args()
    
    # Parse categories
    categories = [cat.strip() for cat in args.categories.split(',')]
    
    try:
        # Create necessary directories
        os.makedirs('logs', exist_ok=True)
        os.makedirs('outputs', exist_ok=True)
        
        logger.info("🚀 Starting Meat Inventory Pipeline")
        logger.info(f"Categories to process: {categories}")
        
        # Define paths
        project_root = Path(__file__).parent.parent
        reference_data_path = project_root / "data" / "incoming" / "beef_cuts.xlsx"
        test_data_path = project_root / "data" / "incoming" / "Product_Query_2025_06_06.csv"
        
        # Validate paths exist
        if not reference_data_path.exists():
            logger.error(f"Reference data file not found: {reference_data_path}")
            return 1
            
        if not test_data_path.exists():
            logger.error(f"Test data file not found: {test_data_path}")
            return 1
        
        # Stage 1: Initialize extraction controller with reference data
        logger.info("📥 Stage 1: Initialize Extraction Controller")
        extractor = ExtractionController(reference_data_path=str(reference_data_path))
        
        # Stage 2: Process data using DataProcessor
        logger.info("🔄 Stage 2: Data Processing")
        processor = DataProcessor()
        
        # Determine limit for test runs
        limit_per_category = 10 if args.test_run else None
        
        df = processor.process_file(
            str(test_data_path), 
            category=categories, 
            limit_per_category=limit_per_category
        )
        
        if df.empty:
            logger.error("No data processed from input file")
            return 1
            
        logger.info(f"Processed {len(df)} records")
        
        # Stage 3: LLM Extraction
        logger.info("🤖 Stage 3: LLM Extraction")
        results_df = extractor.extract_batch(df)
        
        if results_df.empty:
            logger.error("No results from extraction")
            return 1
            
        logger.info(f"Extracted data for {len(results_df)} records")
        
        # Stage 4: Output Generation
        logger.info("📤 Stage 4: Output Generation")
        writer = FileWriter()
        output_files = writer.write_all_outputs(df, results_df)
        
        if not output_files:
            logger.error("No output files generated")
            return 1
            
        logger.info(f"Generated output files: {list(output_files.keys())}")
        
        # Stage 5: Firebase Upload (Optional)
        if args.upload_to_firebase:
            if not firebase_available:
                logger.error("Firebase upload requested but ExcelToFirestore module not available")
                logger.error("Please ensure Firebase dependencies are installed")
            else:
                logger.info("🔥 Stage 5: Firebase Upload")
                
                # Get the master Excel file path
                master_excel_file = output_files.get('excel_master')
                if not master_excel_file or not Path(master_excel_file).exists():
                    logger.error("Cannot upload to Firebase: Master Excel file not found")
                else:
                    try:
                        # Initialize Firebase uploader
                        uploader = ExcelToFirestore(base_collection_prefix="meat_inventory")
                        
                        # Upload the master Excel file
                        collection_name, stats = uploader.import_excel(excel_path=master_excel_file)
                        
                        # Log success
                        logger.info(f"✅ Successfully uploaded to Firebase!")
                        logger.info(f"   Collection: {collection_name}")
                        logger.info(f"   Records uploaded: {stats.get('success', 0)}")
                        logger.info(f"   Total processed: {stats.get('total', 0)}")
                        
                        if stats.get('errors', 0) > 0:
                            logger.warning(f"   Errors: {stats.get('errors', 0)}")
                            
                    except Exception as e:
                        logger.error(f"Firebase upload failed: {e}")
        else:
            logger.info("⏭️ Stage 5: Firebase Upload (Skipped - use --upload-to-firebase to enable)")
        
        logger.info("✅ Pipeline Complete!")
        
        # Summary statistics
        if 'confidence' in results_df.columns:
            avg_confidence = results_df['confidence'].mean() if len(results_df) > 0 else 0.0
            logger.info(f"📊 Average confidence: {avg_confidence:.2f}")
        
        return 0
            
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return 1

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code) 