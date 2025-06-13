"""
Extract Products with Comments from Firebase

Retrieves products with non-blank comments from Firebase collection.
"""

import sys
import argparse
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(project_root))

from src.database.firebase_client import FirebaseClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def find_latest_collection(firebase_client, base_prefix="beef_cuts"):
    """Find the latest collection based on timestamp suffix."""
    current_time = datetime.now()
    
    # Try to find collections by testing timestamps going backwards
    for days_back in range(7):  # Search last 7 days
        test_date = current_time - pd.Timedelta(days=days_back)
        for hour in range(24):
            for minute in range(0, 60, 15):  # Check every 15 minutes
                timestamp = test_date.replace(hour=hour, minute=minute, second=0)
                collection_name = f"{base_prefix}_{timestamp.strftime('%Y%m%d_%H%M%S')}"
                
                # Test if collection exists by trying to get metadata
                try:
                    metadata = firebase_client.get_document(
                        doc_id="_import_metadata",
                        collection=collection_name
                    )
                    
                    if metadata:
                        logger.info(f"Found latest collection: {collection_name}")
                        return collection_name
                except Exception:
                    continue
    
    # Fallback: use base prefix as collection name
    logger.warning(f"No timestamped collections found, using base: {base_prefix}")
    return base_prefix

def extract_products_with_comments(collection_name=None, project_id=None, credentials_path=None):
    """Extract products that have non-blank comments."""
    
    # Initialize Firebase client
    firebase_client = FirebaseClient(
        project_id=project_id,
        credentials_path=credentials_path,
        collection_name="beef_cuts"  # Default
    )
    
    # Find latest collection if not specified
    if not collection_name:
        collection_name = find_latest_collection(firebase_client)
    
    # Get all documents
    logger.info(f"Fetching documents from collection: {collection_name}")
    all_documents = firebase_client.get_documents(
        collection=collection_name,
        limit=10000
    )
    
    # Filter for non-blank comments
    products_with_comments = []
    for doc in all_documents:
        comments = doc.get('comments', '').strip()
        if comments:
            product_data = {
                'document_id': doc.get('id', ''),
                'product_description': doc.get('product_description', ''),
                'subprimal': doc.get('subprimal', ''),
                'grade': doc.get('grade', ''),
                'size': doc.get('size', ''),
                'uom': doc.get('uom', ''),
                'comments': comments,
                'approved': doc.get('approved', '')
            }
            products_with_comments.append(product_data)
    
    logger.info(f"Found {len(products_with_comments)} products with comments")
    
    # Save to Excel
    if products_with_comments:
        df = pd.DataFrame(products_with_comments)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create outputs directory if it doesn't exist
        output_dir = project_root / "outputs"
        output_dir.mkdir(exist_ok=True)
        
        output_file = output_dir / f"products_with_comments_{timestamp}.xlsx"
        df.to_excel(output_file, index=False)
        logger.info(f"Saved to: {output_file}")
        return str(output_file)
    
    return None

def main():
    parser = argparse.ArgumentParser(description="Extract products with comments")
    parser.add_argument("--collection", help="Firebase collection name (auto-detects latest if not provided)")
    parser.add_argument("--project-id", help="Firebase project ID")
    parser.add_argument("--credentials", help="Path to credentials JSON")
    
    args = parser.parse_args()
    
    try:
        result = extract_products_with_comments(
            collection_name=args.collection,
            project_id=args.project_id,
            credentials_path=args.credentials
        )
        
        if result:
            print(f"✅ Success! Output saved to: {result}")
        else:
            print("❌ No products with comments found")
    except Exception as e:
        logger.error(f"Script failed: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()