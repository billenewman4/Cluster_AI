#!/usr/bin/env python3
"""
Main pipeline orchestration script.
"""

import argparse
import logging
import os
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add project root to path to ensure all modules can be found
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))
sys.path.append(str(Path(__file__).parent))

# Import the new LangGraph workflow
from AIs.graph import BeefProcessingWorkflow
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

logger = logging.getLogger(__name__)


def process_categories(categories: list, test_run: bool = False, upload_to_firebase: bool = False, provider: str = 'openai'):
    """
    Core pipeline processing function that can be called from other scripts.
    
    Args:
        categories: List of category names to process
        test_run: If True, process only first 10 records per category
        upload_to_firebase: If True, upload results to Firebase
        provider: AI provider to use (openai, anthropic, etc.)
    
    Returns:
        dict: Processing results and statistics
    """
    try:
        # Create necessary directories
        os.makedirs('logs', exist_ok=True)
        os.makedirs('outputs', exist_ok=True)
        
        logger.info("🚀 Starting Meat Inventory Pipeline with LangGraph Workflow")
        logger.info(f"Categories to process: {categories}")
        logger.info(f"AI Provider: {provider}")
        
        # Define paths
        project_root = Path(__file__).parent.parent
        test_data_path = project_root / "data" / "incoming" / "Product_Query_2025_06_06.csv"
        
        # Validate paths exist
        if not test_data_path.exists():
            logger.error(f"Test data file not found: {test_data_path}")
            return 1
        
        # Stage 1: Process data using DataProcessor
        logger.info("🔄 Stage 1: Data Processing")
        processor = DataProcessor()
        
        # Determine limit for test runs
        limit_per_category = 10 if test_run else None
        
        df = processor.process_file(
            str(test_data_path), 
            category=categories, 
            limit_per_category=limit_per_category
        )
        
        if df.empty:
            logger.error("No data processed from input file")
            return 1
            
        logger.info(f"Processed {len(df)} records")
        
        # Stage 2: LangGraph Workflow Processing
        logger.info("🧠 Stage 2: LangGraph Workflow Processing")
        
        # Initialize the workflow
        workflow = BeefProcessingWorkflow(provider=args.provider)
        
        # Prepare products for the workflow
        products = []
        for _, row in df.iterrows():
            products.append({
                'product_code': str(row.get('product_code', 'UNKNOWN')),
                'product_description': str(row.get('product_description', '')),
                'category': str(row.get('category_description', '')) if pd.notna(row.get('category_description')) else None
            })
        
        logger.info(f"Processing {len(products)} products through LangGraph workflow...")
        
        # Process through workflow
        workflow_results = workflow.process_batch(products)
        
        # Convert workflow results back to DataFrame format
        results_data = []
        clarification_data = []
        
        for i, (original_row, result) in enumerate(zip(df.itertuples(), workflow_results)):
            # Extract data from workflow result
            final_extraction = result.get('final_extraction', {})
            initial_extraction = result.get('initial_extraction', {})
            
            # Create result row in the format FileWriter expects
            # Use original data for basic info, AI extraction for analysis
            result_row = {
                'Description': result['product_description'],
                'Extracted': final_extraction,
                'product_code': original_row.product_code,
                'product_description': original_row.product_description,
                'category': getattr(original_row, 'category_description', original_row.category if hasattr(original_row, 'category') else ''),
                'species': 'Beef',  # Default for now
                'primal': _extract_primal_from_category(getattr(original_row, 'category_description', '')),
                'subprimal': final_extraction.get('subprimal'),
                'grade': final_extraction.get('grade'),
                'size': final_extraction.get('size'),
                'size_uom': final_extraction.get('size_uom'),
                'brand': getattr(original_row, 'brand_name', final_extraction.get('brand', '')),
                'bone_in': final_extraction.get('bone_in', False),
                'confidence': final_extraction.get('confidence', 0.0),
                'needs_review': final_extraction.get('needs_review', True),
                'miss_categorized': final_extraction.get('miss_categorized', False),
                'processing_complete': result['processing_complete'],
                'processing_steps': result.get('processing_steps_completed', ''),
                'workflow_path': 'conditional' if not initial_extraction.get('needs_review', True) else 'full_review'
            }
            results_data.append(result_row)
            
            # Collect clarification questions if any
            questions = result.get('clarification_questions', [])
            if questions:
                for question in questions:
                    clarification_data.append({
                        'product_code': result['product_code'],
                        'product_description': result['product_description'],
                        'question': question,
                        'subprimal': final_extraction.get('subprimal', ''),
                        'grade': final_extraction.get('grade', ''),
                        'size': final_extraction.get('size', ''),
                        'size_uom': final_extraction.get('size_uom', ''),
                        'brand': final_extraction.get('brand', ''),
                        'bone_in': final_extraction.get('bone_in', ''),
                        'confidence': final_extraction.get('confidence', ''),
                        'needs_review': final_extraction.get('needs_review', '')
                    })
        
        # Create results DataFrame
        results_df = pd.DataFrame(results_data)
        
        if results_df.empty:
            logger.error("No results from workflow processing")
            return 1
            
        logger.info(f"Processed {len(results_df)} records through workflow")
        
        # Log workflow statistics
        total_processed = len(workflow_results)
        successful = sum(1 for r in workflow_results if r['processing_complete'])
        conditional_processing = sum(1 for r in results_data if r['workflow_path'] == 'conditional')
        full_review_processing = sum(1 for r in results_data if r['workflow_path'] == 'full_review')
        needs_review_count = sum(1 for r in results_data if r.get('needs_review', False))
        miss_categorized_count = sum(1 for r in results_data if r.get('miss_categorized', False))
        avg_confidence = results_df['confidence'].mean()
        total_questions = len(clarification_data)
        
        logger.info(f"📊 PIPELINE RESULTS:")
        logger.info(f"   Total products: {total_processed}")
        logger.info(f"   Successfully processed: {successful}")
        logger.info(f"   Fast path (skip review): {conditional_processing}")
        logger.info(f"   Full review path: {full_review_processing}")
        logger.info(f"   📝 Review bot flagged for review: {needs_review_count}")
        logger.info(f"   🚨 Review bot flagged miss-categorized: {miss_categorized_count}")
        logger.info(f"   📋 Clarification questions: {total_questions}")
        logger.info(f"   📈 Average confidence: {avg_confidence:.2f}")
        
        # Stage 3: Output Generation
        logger.info("📤 Stage 3: Output Generation")
        writer = FileWriter()
        output_files = writer.write_all_outputs(df, results_df)
        
        if not output_files:
            logger.error("No output files generated")
            return 1
            
        logger.info(f"Generated output files: {list(output_files.keys())}")
        
        # Export clarification questions if any were generated
        if clarification_data:
            clarification_df = pd.DataFrame(clarification_data)
            clarification_file = f"outputs/clarification_questions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            clarification_df.to_excel(clarification_file, index=False)
            logger.info(f"Exported {len(clarification_data)} clarification questions to: {clarification_file}")
        
        # Stage 4: Firebase Upload (Optional)
        if args.upload_to_firebase:
            if not firebase_available:
                logger.error("Firebase upload requested but ExcelToFirestore module not available")
                logger.error("Please ensure Firebase dependencies are installed")
            else:
                logger.info("🔥 Stage 4: Firebase Upload")
                
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
            logger.info("⏭️ Stage 4: Firebase Upload (Skipped - use --upload-to-firebase to enable)")
        
        logger.info("✅ Pipeline Complete!")
        
        # Return results for programmatic use
        return {
            'success': True,
            'total_processed': total_processed,
            'successful': successful,
            'conditional_processing': conditional_processing,
            'full_review_processing': full_review_processing,
            'needs_review_count': needs_review_count,
            'miss_categorized_count': miss_categorized_count,
            'avg_confidence': avg_confidence,
            'total_questions': total_questions,
            'output_files': output_files,
            'results_df': results_df
        }
            
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return {
            'success': False,
            'error': str(e)
        }

def main():
    """Command line interface for the pipeline."""
    parser = argparse.ArgumentParser(
        description='Run the meat inventory pipeline with LangGraph workflow',
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
    parser.add_argument(
        '--provider',
        default='openai',
        help='AI provider to use (openai, anthropic, etc.)'
    )
    
    args = parser.parse_args()
    
    # Parse categories
    categories = [cat.strip() for cat in args.categories.split(',')]
    
    # Call the core processing function
    result = process_categories(
        categories=categories,
        test_run=args.test_run,
        upload_to_firebase=args.upload_to_firebase,
        provider=args.provider
    )
    
    # Return appropriate exit code
    return 0 if result.get('success', False) else 1

def _extract_primal_from_category(category: str) -> str:
    """Extract primal from category string."""
    if not category:
        return ''
    
    # Remove 'beef' prefix and normalize
    primal = category.lower().replace('beef', '').strip()
    
    # Common primal mappings
    primal_mapping = {
        'chuck': 'Chuck',
        'rib': 'Rib', 
        'loin': 'Loin',
        'round': 'Round',
        'brisket': 'Brisket',
        'plate': 'Plate',
        'flank': 'Flank'
    }
    
    return primal_mapping.get(primal, primal.title())

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code) 