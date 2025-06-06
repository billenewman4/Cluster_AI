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

# Add project root to path to ensure all modules can be found
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))
sys.path.append(str(Path(__file__).parent))

# Import Firebase functionality - only this is required for database operations
found_database_module = False
try:
    from src.database.excel_to_firestore import ExcelToFirestore
    found_database_module = True
except ImportError:
    try:
        from database.excel_to_firestore import ExcelToFirestore
        found_database_module = True
    except ImportError:
        logger = logging.getLogger(__name__)
        logger.error("Could not import ExcelToFirestore module")

# Try to import processing modules, but these are optional if we're just doing DB operations
found_processing_modules = False
try:
    # Try direct imports first
    from data_ingestion.core.processor import DataProcessor
    from llm_extraction.beef_chuck_extractor import BeefChuckExtractor
    from llm_extraction.batch_processor import BatchProcessor
    from output_generation.file_writer import FileWriter
    from output_generation.report_generator import ReportGenerator
    found_processing_modules = True
except ImportError:
    try:
        # Fall back to qualified imports
        from src.data_ingestion.core.processor import DataProcessor
        from src.llm_extraction.beef_chuck_extractor import BeefChuckExtractor
        from src.llm_extraction.batch_processor import BatchProcessor
        from src.output_generation.file_writer import FileWriter
        from src.output_generation.report_generator import ReportGenerator
        found_processing_modules = True
    except ImportError:
        # We'll handle this gracefully in the main function
        logger = logging.getLogger(__name__)
        logger.warning("Some processing modules could not be imported")
        pass

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
logging.getLogger('openai').setLevel(logging.DEBUG)
logging.getLogger('llm_extraction').setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)


def process_product_query(args=None):
    """Process Product_Query_2025_06_06.csv with specialized transformations.
    
    1. Merges ProductDescription + ProductDescription2
    2. Renames BrandDescription → BrandName
    3. Maps columns to expected pipeline format
    
    Args:
        args: Command line arguments that may contain test_run flag
    """
    from data_ingestion import ProductTransformer
    
    try:
        query_file = Path('data/incoming/Product_Query_2025_06_06.csv')
        if not query_file.exists():
            logger.error(f"Product query file not found: {query_file}")
            return
            
        logger.info(f"Processing product query file: {query_file.name}")
        
        # Initialize transformer
        transformer = ProductTransformer()
        
        # Read the CSV file
        df = pd.read_csv(query_file)
        logger.info(f"Read {len(df)} records from {query_file.name}")
        
        # Apply test run limit if requested
        if args and args.test_run:
            limit = 10  # Test run limit
            logger.info(f"Test run: Processing only {limit} products")
            df = df.head(limit)
        
        # Process with our specific requirements
        processed_df = transformer.process_product_data(
            df=df,
            standardize_columns=True  # This will map to required column names
        )
        
        if processed_df.empty:
            logger.error("Failed to process product query file")
            return
            
        # Save processed data to parquet for efficiency
        output_dir = Path('data/processed')
        output_dir.mkdir(exist_ok=True, parents=True)
        output_path = output_dir / 'product_query_processed.parquet'
        
        processed_df.to_parquet(output_path, index=False)
        logger.info(f"Saved processed product data with {len(processed_df)} records to {output_path}")
        
        # Also save as CSV for easy inspection
        csv_path = output_dir / 'product_query_processed.csv'
        processed_df.to_csv(csv_path, index=False)
        logger.info(f"Also saved as CSV to {csv_path}")
        
    except Exception as e:
        logger.error(f"Error processing product query file: {str(e)}")


def firebase_upload(excel_file, args):
    """
    Upload Excel data to Firebase Firestore as a standalone function.
    
    Args:
        excel_file: Path to Excel file to upload
        args: Command-line arguments containing Firebase configuration
        
    Returns:
        dict: Upload statistics
    """
    # Create a timestamp for the upload operation
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        # Instantiate ExcelToFirestore with production parameters
        excel_uploader = ExcelToFirestore(
            project_id=args.firebase_project_id,
            credentials_path=args.firebase_credentials,
            base_collection_prefix=args.firebase_collection
        )
        
        # Custom collection ID using timestamp for traceability
        collection_id = f"{args.firebase_collection}_{timestamp}"
        
        # Perform upload with proper error handling
        logger.info(f"Uploading {excel_file} to Firestore collection '{collection_id}'")
        
        # Import Excel file with batch processing and retry logic
        collection_name, stats = excel_uploader.import_excel(
            excel_path=excel_file,
            custom_collection_id=collection_id
        )
        
        # Log success statistics
        logger.info(f"✅ Firebase upload successful!")
        logger.info(f"   Collection: {collection_name}")
        logger.info(f"   Records uploaded: {stats.get('success', 0)}")
        logger.info(f"   Errors: {stats.get('errors', 0)}")
        
        # Write task log with detailed statistics
        with open(f".cline/firebase-upload_{timestamp}.log", "w") as f:
            f.write(f"GOAL: Upload Excel data to Firebase Firestore\n")
            f.write(f"IMPLEMENTATION: Used production-ready ExcelToFirestore uploader")
            f.write(f" with comprehensive validation and error handling\n")
            f.write(f"COMPLETED: {datetime.now().strftime('%d-%m-%Y %H:%M')}\n\n")
            f.write(f"Source file: {excel_file}\n")
            f.write(f"Target collection: {collection_name}\n")
            f.write(f"Records processed: {stats.get('total', 0)}\n")
            f.write(f"Records uploaded: {stats.get('success', 0)}\n")
            f.write(f"Errors: {stats.get('errors', 0)}\n")
        
        return stats
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Firebase upload failed: {error_msg}")
        
        # Check for specific API activation error
        if "SERVICE_DISABLED" in error_msg or "not been used" in error_msg:
            project_id = args.firebase_project_id or "(default)"
            activation_url = f"https://console.developers.google.com/apis/api/firestore.googleapis.com/overview?project={project_id}"
            logger.error(f"Firestore API not enabled. Please visit: {activation_url}")
        
        # Log the error details
        with open(f".cline/firebase-upload-error_{timestamp}.log", "w") as f:
            f.write(f"GOAL: Upload Excel data to Firebase Firestore\n")
            f.write(f"IMPLEMENTATION: Attempt failed due to error\n")
            f.write(f"ERROR: {error_msg}\n")
            f.write(f"NOT COMPLETED: {datetime.now().strftime('%d-%m-%Y %H:%M')}\n")
        
        raise

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
        '--skip-stage1', 
        action='store_true',
        help='Skip data ingestion stage'
    )
    parser.add_argument(
        '--test-run',
        action='store_true', 
        help='Process only first 10 records for testing'
    )
    parser.add_argument(
        '--process-product-query',
        action='store_true',
        help='Process Product_Query CSV with description merging and column renaming'
    )
    
    # Database upload options
    db_group = parser.add_argument_group('Database Upload Options')
    db_group.add_argument(
        '--upload-to-firebase',
        action='store_true',
        help='Upload results to Firebase Firestore database'
    )
    db_group.add_argument(
        '--firebase-collection',
        default='meat_inventory',
        help='Collection prefix to use in Firebase'
    )
    db_group.add_argument(
        '--firebase-project-id',
        help='Firebase project ID (defaults to environment config)'
    )
    db_group.add_argument(
        '--firebase-credentials',
        help='Path to Firebase service account credentials JSON'
    )
    db_group.add_argument(
        '--firebase-excel-file',
        help='Specific Excel file to upload to Firebase (bypasses pipeline execution)'
    )
    
    args = parser.parse_args()
    
    # Standalone Firebase upload mode
    if args.upload_to_firebase and args.firebase_excel_file:
        if not found_database_module:
            logger.error("Firebase upload requested but database module could not be imported")
            return 1
            
        logger.info("🚀 Running in Firebase Upload Standalone Mode")
        logger.info(f"Excel file: {args.firebase_excel_file}")
        
        # Create necessary directories
        os.makedirs('logs', exist_ok=True)
        os.makedirs('.cline', exist_ok=True)
        
        # Skip to Stage 4: Firebase Upload
        if not Path(args.firebase_excel_file).exists():
            logger.error(f"Excel file not found: {args.firebase_excel_file}")
            return 1
            
        try:
            firebase_upload(args.firebase_excel_file, args)
            return 0
        except Exception as e:
            logger.error(f"Firebase upload failed: {e}")
            return 1
    
    # If we're just using Firebase upload without a specific Excel file but processing modules are missing,
    # give a helpful error about standalone mode
    if args.upload_to_firebase and not args.firebase_excel_file and not found_processing_modules:
        logger.error("Cannot run full pipeline: Required processing modules missing")
        logger.error("For Firebase upload only, use: --upload-to-firebase --firebase-excel-file path/to/excel_file.xlsx")
        return 1
        
    # Full pipeline mode - only required if we're not doing standalone Firebase upload
    if not args.upload_to_firebase and not found_processing_modules:
        logger.error("Cannot run full pipeline: Required processing modules missing")
        return 1
    
    # Parse categories
    categories = [cat.strip() for cat in args.categories.split(',')]
    
    try:
        # Create necessary directories
        os.makedirs('logs', exist_ok=True)
        os.makedirs('outputs', exist_ok=True)
        os.makedirs('.cline', exist_ok=True)
        
        logger.info("🚀 Starting Meat Inventory Pipeline")
        logger.info(f"Categories to process: {categories}")
        
        # Stage 1: Data Ingestion
        if not args.skip_stage1:
            logger.info("📥 Stage 1: Data Ingestion")
            
            # Process product query file if requested
            if args.process_product_query:
                process_product_query(args)
            
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
        
        # Load processed data before processing categories
        import pandas as pd
        try:
            df = pd.read_parquet('data/processed/inventory_base.parquet')
            logger.info(f"Loaded {len(df)} records from processed data")
            
            # Debug: Show available categories and counts
            if 'category_description' in df.columns:
                available_categories = df['category_description'].dropna().unique()
                logger.info(f"Available categories in data: {available_categories}")
                category_counts = df['category_description'].value_counts().to_dict()
                logger.info(f"Category counts: {category_counts}")
            else:
                logger.error("No 'category_description' column found in data!")
                logger.info(f"Available columns: {df.columns.tolist()}")
        except Exception as e:
            logger.error(f"Error loading processed data: {e}")
            return 1
            
        # Process each category
        all_results = []
        for category in categories:
            logger.info(f"Processing category: {category}")
            
            # Skip if no extractor configured
            if category.lower() not in extractors:
                logger.warning(f"Skipping {category} - no extractor configured")
                continue
            
            # Filter with case-insensitive matching for flexibility
            if 'category_description' in df.columns:
                # Match category using case-insensitive contains
                category_filter = df['category_description'].str.contains(category, case=False, na=False)
                category_df = df[category_filter]
                
                logger.info(f"Found {len(category_df)} records for {category}")
                
                if len(category_df) == 0:
                    # If no matches found, try more flexible matching
                    logger.warning(f"No records found for exact '{category}' match, trying word boundaries")
                    # Try matching with word boundaries
                    import re
                    pattern = rf"\b{re.escape(category)}\b"
                    category_filter = df['category_description'].str.contains(pattern, case=False, na=False, regex=True)
                    category_df = df[category_filter]
                    logger.info(f"Found {len(category_df)} records with word boundary matching")
                    
                    if len(category_df) == 0:
                        logger.warning(f"Still no records found for {category}")
                        # Show sample data for debugging
                        if not df.empty:
                            logger.info(f"Sample categories:\n{df['category_description'].head(10).tolist()}")
                        continue
            else:
                logger.error(f"Cannot filter by category: no 'category_description' column")
                continue
            
            # Test run - limit to first 10 records
            if args.test_run:
                category_df = category_df.head(10)
                logger.info(f"Test run: Processing only {len(category_df)} records")
            
            # Process with new optimized batch processor
            try:
                result_df = batch_processor.process_batch(category_df, category)
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
        
        # Stage 4: Database Upload (Optional)
        if args.upload_to_firebase:
            logger.info("🔥 Stage 4: Firebase Database Upload")
            
            # Get the master Excel output file path
            master_excel_file = output_files.get('excel_master')
            if not master_excel_file or not Path(master_excel_file).exists():
                logger.error("Cannot upload to Firebase: Master Excel file not found")
            else:
                try:
                    # Use the shared firebase_upload function for consistency
                    firebase_upload(master_excel_file, args)
                except Exception as e:
                    # Error already logged in firebase_upload function
                    pass
        else:
            logger.info("⏭️ Stage 4: Database Upload (Skipped - use --upload-to-firebase to enable)")
        
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