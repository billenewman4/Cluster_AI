#!/usr/bin/env python3
"""
Fast Batch Processing Script for Beef Extraction
A thin wrapper that adds batching, checkpointing, and resume capabilities
to the main pipeline logic from run_pipeline.py
"""

import os
import sys
import json
import logging
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "src"))

# Import the core pipeline function
from src.run_pipeline import process_categories
from src.Caching import refresh_cache

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/fast_batch.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FastBatchProcessor:
    """Fast batch processor with checkpointing and resume capability."""
    
    def __init__(self, provider: str = 'openai'):
        self.checkpoint_file = "checkpoint_batch_progress.json"
        self.results_dir = Path("outputs/batch_results")
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.provider = provider
        
        # Refresh approved items cache before processing
        refresh_result = refresh_cache("reviewed_beef_cuts_latest_master_20250616_20250617_102108")
        logger.info(f"Refreshed approved items cache: {refresh_result}")
        
    def load_checkpoint(self) -> Dict:
        """Load processing checkpoint."""
        try:
            if Path(self.checkpoint_file).exists():
                with open(self.checkpoint_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load checkpoint: {e}")
        return {"completed_categories": [], "processed_records": 0}
    
    def save_checkpoint(self, checkpoint: Dict):
        """Save processing checkpoint."""
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint, f, indent=2)
        except Exception as e:
            logger.warning(f"Could not save checkpoint: {e}")
    
    def run_fast_batch(self, test_run: bool = False, upload_firebase: bool = False):
        """Run the fast batch processing by calling the main pipeline for each category."""
        logger.info("🚀 Starting Fast Batch Processing")
        
        # Define beef categories to process
        beef_categories = [
            'Beef Chuck', 'Beef Rib', 'Beef Loin', 'Beef Round', 'Beef Flank',
            'Beef Other', 'Beef Ground', 'Beef Variety', 'Beef Plate', 'Beef Brisket',
            'Beef Trim'
        ]
        
        # Load checkpoint
        checkpoint = self.load_checkpoint()
        completed_categories = checkpoint.get("completed_categories", [])
        
        all_results = []
        overall_stats = {
            'total_processed': 0,
            'successful': 0,
            'categories_completed': 0,
            'categories_skipped': 0,
            'errors': []
        }
        
        for category in beef_categories:
            # Skip if already completed
            if category in completed_categories:
                logger.info(f"⏭️ Skipping {category} (already completed)")
                overall_stats['categories_skipped'] += 1
                continue
            
            logger.info(f"📊 Processing category: {category}")
            
            try:
                # Call the main pipeline function for this single category
                result = process_categories(
                    categories=[category],  # Process one category at a time
                    test_run=test_run,
                    upload_to_firebase=upload_firebase,
                    provider=self.provider
                )
                
                if result.get('success', False):
                    # Update overall statistics
                    overall_stats['total_processed'] += result.get('total_processed', 0)
                    overall_stats['successful'] += result.get('successful', 0)
                    overall_stats['categories_completed'] += 1
                    
                    # Store result
                    all_results.append(result)
                    
                    # Update checkpoint
                    checkpoint["completed_categories"].append(category)
                    checkpoint["processed_records"] += result.get('total_processed', 0)
                    checkpoint["last_category"] = category
                    checkpoint["last_updated"] = datetime.now().isoformat()
                    self.save_checkpoint(checkpoint)
                    
                    logger.info(f"✅ Completed {category}: {result.get('total_processed', 0)} records processed")
                    
                else:
                    error_msg = f"Pipeline failed for {category}: {result.get('error', 'Unknown error')}"
                    logger.error(f"❌ {error_msg}")
                    overall_stats['errors'].append(error_msg)
                    break
                
            except Exception as e:
                error_msg = f"Failed to process {category}: {str(e)}"
                logger.error(f"❌ {error_msg}")
                overall_stats['errors'].append(error_msg)
                logger.info("💾 Progress saved in checkpoint. You can resume later.")
                break
        
        # Final summary
        if all_results:
            logger.info(f"🎉 BATCH COMPLETE!")
            logger.info(f"   Categories completed: {overall_stats['categories_completed']}")
            logger.info(f"   Categories skipped: {overall_stats['categories_skipped']}")
            logger.info(f"   Total records processed: {overall_stats['total_processed']}")
            logger.info(f"   Successful extractions: {overall_stats['successful']}")
            
            if overall_stats['errors']:
                logger.warning(f"   Errors encountered: {len(overall_stats['errors'])}")
            
            # Clean up checkpoint if everything completed successfully
            if overall_stats['categories_completed'] > 0 and not overall_stats['errors']:
                if Path(self.checkpoint_file).exists():
                    Path(self.checkpoint_file).unlink()
                    logger.info("🧹 Checkpoint file cleaned up")
        
        return overall_stats

def main():
    """Command line interface for fast batch processing."""
    parser = argparse.ArgumentParser(description='Fast batch processing for beef extraction')
    parser.add_argument('--test-run', action='store_true', help='Process only 10 records per category')
    parser.add_argument('--upload-firebase', action='store_true', help='Upload results to Firebase')
    parser.add_argument('--resume', action='store_true', help='Resume from checkpoint')
    parser.add_argument('--provider', default='openai', help='AI provider to use (openai, anthropic, etc.)')
    
    args = parser.parse_args()
    
    # Set model environment variable
    os.environ['OPENAI_MODEL'] = 'gpt-4o-mini'
    
    processor = FastBatchProcessor(provider=args.provider)
    
    if args.resume:
        logger.info("🔄 Resuming from checkpoint...")
    
    try:
        results = processor.run_fast_batch(
            test_run=args.test_run,
            upload_firebase=args.upload_firebase
        )
        
        if results['errors']:
            logger.error("❌ Fast batch processing completed with errors!")
            return 1
        else:
            logger.info("✅ Fast batch processing completed successfully!")
            return 0
        
    except KeyboardInterrupt:
        logger.info("⏸️ Processing interrupted by user. Progress saved in checkpoint.")
        logger.info("💡 Run with --resume to continue from where you left off.")
        return 1
    except Exception as e:
        logger.error(f"❌ Processing failed: {e}")
        logger.info("💾 Progress may be saved in checkpoint. Try --resume to continue.")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 