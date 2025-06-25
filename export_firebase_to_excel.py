#!/usr/bin/env python3
"""
Script to export Firebase Firestore data to Excel format
"""

import sys
import json
import pandas as pd
from datetime import datetime
import argparse

def export_firestore_to_excel(collection_name, output_file=None):
    """
    Export Firestore collection data to Excel format
    """
    try:
        # Import Firebase MCP client
        from src.database.firebase_mcp import FirebaseMCP
        
        # Initialize Firebase client
        firebase_client = FirebaseMCP()
        
        print(f"Exporting collection: {collection_name}")
        
        # Query all documents from the collection
        docs = firebase_client.query_collection(
            collection_path=collection_name,
            filters=[{"field": "id", "op": "GREATER_THAN", "compare_value": {"string_value": ""}}],
            limit=1000  # Adjust as needed
        )
        
        if not docs:
            print(f"No documents found in collection: {collection_name}")
            return
        
        # Convert to DataFrame
        data_list = []
        for doc in docs:
            # Remove the __path__ field as it's not needed in Excel
            doc_data = {k: v for k, v in doc.items() if k != '__path__'}
            data_list.append(doc_data)
        
        df = pd.DataFrame(data_list)
        
        # Generate output filename if not provided
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"firebase_export_{collection_name}_{timestamp}.xlsx"
        
        # Ensure output directory exists
        import os
        output_dir = "outputs"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        output_path = os.path.join(output_dir, output_file)
        
        # Export to Excel
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Firebase_Data', index=False)
            
            # Auto-adjust column widths
            worksheet = writer.sheets['Firebase_Data']
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"Successfully exported {len(data_list)} records to: {output_path}")
        print(f"Columns exported: {', '.join(df.columns.tolist())}")
        
        return output_path
        
    except ImportError:
        raise ImportError("Firebase MCP client not available. Please install the required dependencies.")
    except Exception as e:
        print(f"Error exporting data: {str(e)}")
        return None

def main():
    parser = argparse.ArgumentParser(description='Export Firebase Firestore data to Excel')
    parser.add_argument('--collection', '-c', 
                       default='meat_inventory_master_20250610_231446',
                       help='Firestore collection name to export')
    parser.add_argument('--output', '-o', 
                       help='Output Excel file name (optional)')
    
    args = parser.parse_args()
    
    print("Firebase to Excel Exporter")
    print("=" * 40)
    
    output_path = export_firestore_to_excel(args.collection, args.output)
    
    if output_path:
        print(f"\n✅ Export completed successfully!")
        print(f"📁 File saved: {output_path}")
    else:
        print("\n❌ Export failed!")
        sys.exit(1)

if __name__ == "__main__":
    main() 