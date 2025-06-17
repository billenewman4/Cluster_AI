#!/usr/bin/env python3
"""
Fast Batch Processing Script for Beef Extraction
Optimized for speed and reliability with checkpointing and resume capability.
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

from src.AIs.llm_extraction.specific_extractors.dynamic_beef_extractor import DynamicBeefExtractor
from src.AIs.llm_extraction.batch_processor import BatchProcessor
from src.database.beef_cuts_store import BeefCutsStore

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
    
    def __init__(self):
        self.checkpoint_file = "checkpoint_batch_progress.json"
        self.results_dir = Path("outputs/batch_results")
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize stores
        self.beef_store = BeefCutsStore()
        
        # Setup extractors for each beef category
        self.extractors = {
            'beef chuck': DynamicBeefExtractor(),
            'beef rib': DynamicBeefExtractor(), 
            'beef loin': DynamicBeefExtractor(),
            'beef round': DynamicBeefExtractor(),
            'beef flank': DynamicBeefExtractor(),
            'beef other': DynamicBeefExtractor(),
            'beef ground': DynamicBeefExtractor(),
            'beef variety': DynamicBeefExtractor(),
            'beef plate': DynamicBeefExtractor(),
            'beef brisket': DynamicBeefExtractor(),
            'beef trim': DynamicBeefExtractor()
        }
        
        # Initialize batch processor with conservative rate limiting
        self.batch_processor = BatchProcessor(
            extractors=self.extractors,
            cache_file="data/processed/.fast_batch_cache.json"
        )
        
        # More conservative rate limiting to avoid issues
        self.batch_processor.max_concurrent = 3  # Reduced from 5
        self.batch_processor.requests_per_minute = 50  # Very conservative
        
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
    
    def process_category_batch(self, category: str, df: pd.DataFrame, checkpoint: Dict) -> pd.DataFrame:
        """Process a single category with progress tracking."""
        logger.info(f"🚀 Processing {len(df)} records for category: {category}")
        
        # Set the primal for the extractor
        primal = self._get_primal_from_category(category)
        extractor = self.extractors.get(category.lower())
        if extractor:
            extractor.set_primal(primal)
        
        try:
            # Process the batch
            results_df = self.batch_processor.process_batch(df, category)
            
            # Save intermediate results
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = self.results_dir / f"{category.replace(' ', '_').lower()}_{timestamp}.csv"
            results_df.to_csv(output_file, index=False)
            logger.info(f"💾 Saved {len(results_df)} results to {output_file}")
            
            # Update checkpoint
            checkpoint["completed_categories"].append(category)
            checkpoint["processed_records"] += len(results_df)
            checkpoint["last_category"] = category
            checkpoint["last_updated"] = datetime.now().isoformat()
            self.save_checkpoint(checkpoint)
            
            return results_df
            
        except Exception as e:
            logger.error(f"❌ Error processing {category}: {e}")
            raise
    
    def _get_primal_from_category(self, category: str) -> str:
        """Extract primal from category description."""
        category_lower = category.lower()
        if 'chuck' in category_lower:
            return 'Chuck'
        elif 'rib' in category_lower:
            return 'Rib'
        elif 'loin' in category_lower:
            return 'Loin'
        elif 'round' in category_lower:
            return 'Round'
        elif 'flank' in category_lower:
            return 'Flank'
        elif 'plate' in category_lower:
            return 'Plate'
        elif 'brisket' in category_lower:
            return 'Brisket'
        elif 'variety' in category_lower:
            return 'Variety'
        else:
            return 'Other'
    
    def upload_to_firebase(self, results_df: pd.DataFrame, category: str):
        """Upload results to Firebase."""
        try:
            logger.info(f"🔥 Uploading {len(results_df)} records to Firebase for {category}")
            result = self.beef_store.store_extraction_from_dataframe(results_df, category)
            logger.info(f"✅ Firebase upload complete: {result}")
        except Exception as e:
            logger.error(f"❌ Firebase upload failed for {category}: {e}")
    
    def run_fast_batch(self, test_run: bool = False, upload_firebase: bool = False):
        """Run the fast batch processing."""
        logger.info("🚀 Starting Fast Batch Processing")
        
        # Load data
        df = pd.read_parquet('data/processed/inventory_base.parquet')
        
        # Filter for beef categories (using mapped names after ProductTransformer)
        beef_categories = [
            'Beef Chuck', 'Beef Rib', 'Beef Loin', 'Beef Round', 'Beef Flank',
            'Beef Other', 'Beef Ground', 'Beef Variety', 'Beef Plate', 'Beef Brisket',
            'Beef Trim'
        ]
        
        # Load checkpoint
        checkpoint = self.load_checkpoint()
        completed_categories = checkpoint.get("completed_categories", [])
        
        all_results = []
        
        for category in beef_categories:
            # Skip if already completed
            if category in completed_categories:
                logger.info(f"⏭️ Skipping {category} (already completed)")
                continue
            
            # Filter data for this category
            category_df = df[df['category_description'] == category].copy()
            
            if category_df.empty:
                logger.warning(f"⚠️ No data found for category: {category}")
                continue
            
            # Limit for test run
            if test_run:
                category_df = category_df.head(10)
                logger.info(f"🧪 Test mode: Processing only {len(category_df)} records for {category}")
            
            logger.info(f"📊 Found {len(category_df)} records for {category}")
            
            # Process this category
            try:
                results_df = self.process_category_batch(category, category_df, checkpoint)
                all_results.append(results_df)
                
                # Upload to Firebase if requested
                if upload_firebase:
                    self.upload_to_firebase(results_df, category)
                
                logger.info(f"✅ Completed {category}: {len(results_df)} records processed")
                
            except Exception as e:
                logger.error(f"❌ Failed to process {category}: {e}")
                logger.info("💾 Progress saved in checkpoint. You can resume later.")
                break
        
        # Combine all results
        if all_results:
            final_df = pd.concat(all_results, ignore_index=True)
            
            # Save master file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            master_file = self.results_dir / f"master_beef_extraction_{timestamp}.csv"
            final_df.to_csv(master_file, index=False)
            
            logger.info(f"🎉 COMPLETE! Processed {len(final_df)} total records")
            logger.info(f"📁 Master file saved: {master_file}")
            
            # Clean up checkpoint
            if Path(self.checkpoint_file).exists():
                Path(self.checkpoint_file).unlink()
                logger.info("🧹 Checkpoint file cleaned up")
        
        return all_results

def main():
    parser = argparse.ArgumentParser(description='Fast batch processing for beef extraction')
    parser.add_argument('--test-run', action='store_true', help='Process only 10 records per category')
    parser.add_argument('--upload-firebase', action='store_true', help='Upload results to Firebase')
    parser.add_argument('--resume', action='store_true', help='Resume from checkpoint')
    
    args = parser.parse_args()
    
    # Set model environment variable
    os.environ['OPENAI_MODEL'] = 'gpt-4o-mini'
    
    processor = FastBatchProcessor()
    
    if args.resume:
        logger.info("🔄 Resuming from checkpoint...")
    
    try:
        results = processor.run_fast_batch(
            test_run=args.test_run,
            upload_firebase=args.upload_firebase
        )
        logger.info("✅ Fast batch processing completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("⏸️ Processing interrupted by user. Progress saved in checkpoint.")
        logger.info("💡 Run with --resume to continue from where you left off.")
    except Exception as e:
        logger.error(f"❌ Processing failed: {e}")
        logger.info("💾 Progress may be saved in checkpoint. Try --resume to continue.")

if __name__ == "__main__":
    main() 