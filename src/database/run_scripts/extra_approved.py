"""
Extract Approved Products from Firebase

Retrieves products marked as approved from Firebase collection.
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

def extract_approved_products(collection_name, project_id=None, credentials_path=None):
    """Extract products that are marked as approved."""
    
    # Initialize Firebase client
    firebase_client = FirebaseClient(
        project_id=project_id,
        credentials_path=credentials_path,
        collection_name=collection_name
    )
    
    # Get all documents
    logger.info(f"Fetching documents from collection: {collection_name}")
    all_documents = firebase_client.get_documents(
        collection=collection_name,
        limit=10000
    )
    
    # Filter for approved products
    approved_products = []
    for doc in all_documents:
        approved_status = str(doc.get('approved', '')).strip().lower()
        
        # Check if marked as approved
        is_approved = approved_status in [
            'approved', 'yes', 'y', 'true', '1', 'accept', 'accepted', '✓'
        ]
        
        if is_approved:
            product_data = {
                'document_id': doc.get('id', ''),
                'product_description': doc.get('product_description', ''),
                'species': doc.get('species', ''),
                'primal': doc.get('primal', ''),
                'subprimal': doc.get('subprimal', ''),
                'grade': doc.get('grade', ''),
                'family': doc.get('family', ''),
                'approved': doc.get('approved', '')
            }
            approved_products.append(product_data)
    
    logger.info(f"Found {len(approved_products)} approved products")
    
    # Save to Excel
    if approved_products:
        df = pd.DataFrame(approved_products)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"approved_products_{timestamp}.xlsx"
        df.to_excel(output_file, index=False)
        logger.info(f"Saved to: {output_file}")
        return output_file
    
    return None

def main():
    parser = argparse.ArgumentParser(description="Extract approved products")
    parser.add_argument("collection", help="Firebase collection name")
    parser.add_argument("--project-id", help="Firebase project ID")
    parser.add_argument("--credentials", help="Path to credentials JSON")
    
    args = parser.parse_args()
    
    result = extract_approved_products(
        collection_name=args.collection,
        project_id=args.project_id,
        credentials_path=args.credentials
    )
    
    if result:
        print(f"✅ Success! Output saved to: {result}")
    else:
        print("❌ No approved products found")

if __name__ == "__main__":
    main()