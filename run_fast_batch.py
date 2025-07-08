#!/usr/bin/env python3
"""
Fast Batch Processing Script - Simplified Wrapper
Processes multiple beef categories with basic checkpointing and parallel execution.
"""

import os
import sys
import json
import logging
import argparse
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Add project paths
project_root = Path(__file__).parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "src"))

# Import core pipeline function
from src.run_pipeline import process_categories

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Beef categories to process
BEEF_CATEGORIES = [
    'Beef Chuck', 'Beef Rib', 'Beef Loin', 'Beef Round', 'Beef Flank',
    'Beef Other', 'Beef Ground', 'Beef Variety', 'Beef Plate', 'Beef Brisket', 'Beef Trim'
]

def load_checkpoint() -> list:
    """Load completed categories from checkpoint file."""
    checkpoint_file = "fast_batch_checkpoint.json"
    try:
        if Path(checkpoint_file).exists():
            with open(checkpoint_file, 'r') as f:
                return json.load(f).get("completed", [])
        except Exception as e:
            logger.warning(f"Could not load checkpoint: {e}")
    return []

def save_checkpoint(completed: list):
    """Save completed categories to checkpoint file."""
    checkpoint_file = "fast_batch_checkpoint.json"
    try:
        with open(checkpoint_file, 'w') as f:
            json.dump({
                "completed": completed,
                "timestamp": datetime.now().isoformat()
            }, f, indent=2)
        except Exception as e:
            logger.warning(f"Could not save checkpoint: {e}")
    
def process_single_category(category: str, test_run: bool, upload_firebase: bool, provider: str) -> dict:
    """Process a single category through the main pipeline."""
    logger.info(f"🔄 Processing {category}")
    
    try:
        result = process_categories(
            categories=[category],
            test_run=test_run,
            upload_to_firebase=upload_firebase,
            provider=provider
        )
        
        if result.get('success', False):
            logger.info(f"✅ {category}: {result.get('total_processed', 0)} records processed")
            return {'category': category, 'success': True, 'result': result}
        else:
            logger.error(f"❌ {category} failed: {result.get('error', 'Unknown error')}")
            return {'category': category, 'success': False, 'error': result.get('error', 'Unknown error')}
            
    except Exception as e:
        logger.error(f"❌ {category} crashed: {str(e)}")
        return {'category': category, 'success': False, 'error': str(e)}

def _create_consolidated_master_excel(successful_results: list):
    """Create one master Excel file from all successful pipeline runs."""
    try:
        logger.info("📋 Creating consolidated master Excel file...")
        logger.info(f"   Processing {len(successful_results)} successful results")
        
        # Collect all results DataFrames
        all_dataframes = []
        for i, result in enumerate(successful_results):
            category = result.get('category', 'Unknown')
            results_df = result['result'].get('results_df')
            
            logger.info(f"   Category {category}: DataFrame shape = {results_df.shape if results_df is not None and hasattr(results_df, 'shape') else 'None or invalid'}")
            
            if results_df is not None and hasattr(results_df, 'shape') and not results_df.empty:
                # Add category column for identification
                results_df = results_df.copy()
                results_df['batch_category'] = category
                all_dataframes.append(results_df)
                logger.info(f"   ✅ Added {len(results_df)} records from {category}")
        else:
                logger.warning(f"   ❌ No data from {category}")
        
        if not all_dataframes:
            logger.warning("No data found to consolidate - all DataFrames were empty or None")
            return
        
        # Concatenate all DataFrames
        consolidated_df = pd.concat(all_dataframes, ignore_index=True)
        
        # Create master Excel file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_path = f"outputs/fast_batch_master_{timestamp}.xlsx"
        
        # Write to Excel with multiple sheets
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            # All data sheet
            consolidated_df.to_excel(writer, sheet_name='All_Products', index=False)
            
            # Category sheets
            for category in consolidated_df['batch_category'].unique():
                if pd.notna(category):
                    category_df = consolidated_df[consolidated_df['batch_category'] == category]
                    sheet_name = str(category).replace(' ', '_')[:31]  # Excel sheet name limits
                    category_df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        logger.info(f"📋 Created consolidated master Excel: {excel_path}")
        logger.info(f"   Total records: {len(consolidated_df)}")
        logger.info(f"   Categories: {consolidated_df['batch_category'].nunique()}")
        logger.info(f"   Category breakdown: {dict(consolidated_df['batch_category'].value_counts())}")
        
    except Exception as e:
        logger.error(f"Failed to create consolidated Excel: {e}")

def run_fast_batch(test_run: bool = False, upload_firebase: bool = False, provider: str = 'openai', 
                  max_workers: int = 2, resume: bool = True):
    """Run fast batch processing with optional parallel execution."""
    
    logger.info("🚀 Starting Fast Batch Processing")
    
    # Load checkpoint if resuming
    completed = load_checkpoint() if resume else []
    remaining = [cat for cat in BEEF_CATEGORIES if cat not in completed]
    
    if not remaining:
        logger.info("🎉 All categories already completed!")
        return
    
    logger.info(f"📋 Processing {len(remaining)} categories (skipping {len(completed)} completed)")
    
    # Process categories
    results = []
    if max_workers == 1:
        # Sequential processing
        for category in remaining:
            result = process_single_category(category, test_run, upload_firebase, provider)
            results.append(result)
            
            # Update checkpoint after each success
            if result['success']:
                completed.append(category)
                save_checkpoint(completed)
    else:
        # Parallel processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_category = {
                executor.submit(process_single_category, cat, test_run, upload_firebase, provider): cat 
                for cat in remaining
            }
            
            # Process completed tasks
            for future in as_completed(future_to_category):
                result = future.result()
                results.append(result)
                
                # Update checkpoint after each success
                if result['success']:
                    completed.append(result['category'])
                    save_checkpoint(completed)
    
    # Final summary
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    total_processed = sum(r['result'].get('total_processed', 0) for r in successful)
    
    logger.info(f"🎉 BATCH COMPLETE!")
    logger.info(f"   ✅ Successful: {len(successful)}")
    logger.info(f"   ❌ Failed: {len(failed)}")
    logger.info(f"   📊 Total records: {total_processed}")
    
    if failed:
        logger.error(f"❌ Failed categories: {[r['category'] for r in failed]}")
    
    # Create consolidated master Excel from all successful runs
    if successful:
        _create_consolidated_master_excel(successful)
    
    # Clean up checkpoint if all completed
    if len(completed) == len(BEEF_CATEGORIES):
        checkpoint_file = "fast_batch_checkpoint.json"
        if Path(checkpoint_file).exists():
            Path(checkpoint_file).unlink()
            logger.info("🧹 Checkpoint cleaned up")

def main():
    """Command line interface."""
    parser = argparse.ArgumentParser(description='Fast batch processing for beef extraction')
    parser.add_argument('--test-run', action='store_true', help='Process only 10 records per category')
    parser.add_argument('--upload-firebase', action='store_true', help='Upload results to Firebase')
    parser.add_argument('--no-resume', action='store_true', help='Start fresh (ignore checkpoint)')
    parser.add_argument('--provider', default='openai', help='AI provider (openai, anthropic, etc.)')
    parser.add_argument('--workers', type=int, default=2, help='Number of parallel workers (1=sequential)')
    
    args = parser.parse_args()
    
    # Set model
    os.environ['OPENAI_MODEL'] = 'gpt-4o-mini'
    
    try:
        run_fast_batch(
            test_run=args.test_run,
            upload_firebase=args.upload_firebase,
            provider=args.provider,
            max_workers=args.workers,
            resume=not args.no_resume
        )
        return 0
        
    except KeyboardInterrupt:
        logger.info("⏸️ Interrupted. Progress saved in checkpoint. Resume with same command.")
        return 1
    except Exception as e:
        logger.error(f"❌ Processing failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 