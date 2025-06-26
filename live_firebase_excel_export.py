#!/usr/bin/env python3
"""
Live Firebase to Excel Export
Uses the existing firebase_client.py to get real Firebase data and export to Excel
"""

import sys
import pandas as pd
from datetime import datetime
import os
from pathlib import Path

# Add the src directory to the path so we can import our Firebase client
current_dir = Path(__file__).resolve().parent
src_dir = current_dir / "src"
sys.path.insert(0, str(current_dir))

from src.database.firebase_client import FirebaseClient

def export_live_firebase_data(collection_name="reviewed_beef_cuts_latest_master_20250616_20250617_102108"):
    """
    Export live data from Firebase using the existing firebase_client.py
    """
    print("Live Firebase to Excel Export")
    print("=" * 50)
    print(f"Connecting to Firebase...")
    
    try:
        # Initialize Firebase client using existing code
        firebase_client = FirebaseClient(collection_name=collection_name)
        
        print(f"✅ Connected to Firebase successfully!")
        print(f"📋 Querying collection: {collection_name}")
        
        # Get all documents from the collection
        documents = firebase_client.get_documents(
            collection=collection_name,
            limit=5000  # Increase limit for the larger collection
        )
        
        if not documents:
            print(f"❌ No documents found in collection: {collection_name}")
            return None
            
        print(f"📊 Retrieved {len(documents)} records from Firebase")
        
        # Convert to DataFrame
        df = pd.DataFrame(documents)
        
        print(f"📊 DataFrame has {len(df.columns)} columns: {list(df.columns)}")
        print(f"📊 Sample data from first record:")
        for col in ['approved', 'comments', 'product_description', 'brand', 'grade']:
            if col in df.columns:
                print(f"   {col}: '{df[col].iloc[0]}'")
        
        # Generate output filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"reviewed_firebase_export_{timestamp}.xlsx"
        
        # Ensure output directory exists
        output_dir = "outputs"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        output_path = os.path.join(output_dir, output_file)
        
        # Create Excel file with multiple sheets
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Main data sheet
            df.to_excel(writer, sheet_name='Reviewed_Firebase_Data', index=False)
            
            # Brand Analysis (if brand column exists)
            if 'brand_name' in df.columns and 'brand' in df.columns:
                brand_col = 'brand_name' if df['brand_name'].notna().any() else 'brand'
                brand_analysis = df.groupby(brand_col).agg({
                    'product_code': 'count',
                    'confidence': 'mean' if 'confidence' in df.columns else 'count',
                    'needs_review': 'sum' if 'needs_review' in df.columns else 'count',
                    'approved': lambda x: (x != '').sum() if 'approved' in df.columns else 0  # Count non-empty approved
                }).round(3).reset_index()
                brand_analysis.columns = ['Brand', 'Product_Count', 'Avg_Confidence', 'Items_Need_Review', 'Items_Approved']
                brand_analysis.to_excel(writer, sheet_name='Brand_Analysis', index=False)
            
            # Grade Analysis (if grade column exists)
            if 'grade' in df.columns:
                grade_analysis = df.groupby('grade').agg({
                    'product_code': 'count',
                    'confidence': 'mean' if 'confidence' in df.columns else 'count',
                    'approved': lambda x: (x != '').sum() if 'approved' in df.columns else 0
                }).round(3).reset_index()
                grade_analysis.columns = ['Grade', 'Product_Count', 'Avg_Confidence', 'Items_Approved']
                grade_analysis.to_excel(writer, sheet_name='Grade_Analysis', index=False)
            
            # Subprimal Analysis (if subprimal column exists)
            if 'subprimal' in df.columns:
                subprimal_analysis = df.groupby('subprimal').agg({
                    'product_code': 'count',
                    'confidence': 'mean' if 'confidence' in df.columns else 'count',
                    'needs_review': 'sum' if 'needs_review' in df.columns else 'count',
                    'approved': lambda x: (x != '').sum() if 'approved' in df.columns else 0
                }).round(3).reset_index()
                subprimal_analysis.columns = ['Subprimal', 'Product_Count', 'Avg_Confidence', 'Items_Need_Review', 'Items_Approved']
                subprimal_analysis.to_excel(writer, sheet_name='Subprimal_Analysis', index=False)
            
            # Approval Status Analysis
            if 'approved' in df.columns:
                approval_summary = {
                    'Status': ['Total Records', 'Items with Approval Status', 'Items Needing Approval', 'Approval Rate (%)'],
                    'Count': [
                        len(df),
                        (df['approved'] != '').sum(),
                        (df['approved'] == '').sum(),
                        round(((df['approved'] != '').sum() / len(df)) * 100, 2) if len(df) > 0 else 0
                    ]
                }
                approval_df = pd.DataFrame(approval_summary)
                approval_df.to_excel(writer, sheet_name='Approval_Status', index=False)
            
            # Comments Analysis
            if 'comments' in df.columns:
                comments_summary = {
                    'Status': ['Total Records', 'Items with Comments', 'Items without Comments', 'Comments Rate (%)'],
                    'Count': [
                        len(df),
                        (df['comments'] != '').sum(),
                        (df['comments'] == '').sum(),
                        round(((df['comments'] != '').sum() / len(df)) * 100, 2) if len(df) > 0 else 0
                    ]
                }
                comments_df = pd.DataFrame(comments_summary)
                comments_df.to_excel(writer, sheet_name='Comments_Analysis', index=False)
            
            # Summary sheet
            summary_data = {
                'Metric': [
                    'Total Records',
                    'Unique Product Codes',
                    'Unique Brands',
                    'Unique Grades',
                    'Unique Subprimals',
                    'Items Needing Review',
                    'Items with Approval Status',
                    'Items with Comments',
                    'Average Confidence Score',
                    'Export Date',
                    'Source Collection'
                ],
                'Value': [
                    len(df),
                    df['product_code'].nunique() if 'product_code' in df.columns else 'N/A',
                    df['brand_name'].nunique() if 'brand_name' in df.columns else df['brand'].nunique() if 'brand' in df.columns else 'N/A',
                    df['grade'].nunique() if 'grade' in df.columns else 'N/A',
                    df['subprimal'].nunique() if 'subprimal' in df.columns else 'N/A',
                    df['needs_review'].sum() if 'needs_review' in df.columns else 'N/A',
                    (df['approved'] != '').sum() if 'approved' in df.columns else 'N/A',
                    (df['comments'] != '').sum() if 'comments' in df.columns else 'N/A',
                    round(df['confidence'].mean(), 3) if 'confidence' in df.columns else 'N/A',
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    collection_name
                ]
            }
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Auto-adjust column widths for all sheets
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"\n✅ Export completed successfully!")
        print(f"📁 File saved: {output_path}")
        print(f"📊 Records exported: {len(documents)}")
        print()
        print("Excel file contains multiple sheets:")
        print("📋 Reviewed_Firebase_Data: Complete reviewed product data")
        if 'brand_name' in df.columns or 'brand' in df.columns:
            print("🏷️  Brand_Analysis: Analysis by brand with approval status")
        if 'grade' in df.columns:
            print("🥇 Grade_Analysis: Analysis by grade with approval status")
        if 'subprimal' in df.columns:
            print("🥩 Subprimal_Analysis: Analysis by subprimal cuts with approval status")
        if 'approved' in df.columns:
            print("✅ Approval_Status: Overview of approval status")
        if 'comments' in df.columns:
            print("💬 Comments_Analysis: Overview of comments")
        print("📈 Summary: Key statistics and metrics")
        print()
        print("Data includes reviewed Firebase data with:")
        print("- Product codes and detailed descriptions")
        print("- Brand and grade classifications")
        print("- Subprimal cut specifications")
        print("- Quality confidence scores")
        print("- Review and categorization flags")
        print("- Approval status and comments fields")
        print("- Size and unit of measure details")
        
        return output_path
        
    except Exception as e:
        print(f"❌ Export failed: {str(e)}")
        import traceback
        print(f"Error details: {traceback.format_exc()}")
        return None

def main():
    """Main execution function"""
    # Use the correct reviewed collection
    collection_name = "reviewed_beef_cuts_latest_master_20250616_20250617_102108"
    
    if len(sys.argv) > 1:
        collection_name = sys.argv[1]
    
    print(f"Using collection: {collection_name}")
    output_path = export_live_firebase_data(collection_name)
    
    if output_path:
        print(f"\n🎉 Success! Your reviewed Firebase data has been exported to: {output_path}")
    else:
        print("\n💥 Export failed! Check the error messages above.")
        sys.exit(1)

if __name__ == "__main__":
    main() 