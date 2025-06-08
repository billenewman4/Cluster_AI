"""
File Writer Module
Handles writing output files in various formats.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

class FileWriter:
    """Handles writing output files."""
    
    def __init__(self, outputs_dir: str = "outputs"):
        self.outputs_dir = Path(outputs_dir)
        self.outputs_dir.mkdir(parents=True, exist_ok=True)
        
        # Final schema columns in order
        self.final_schema = [
            'source_filename', 'row_number', 'product_code', 'product_description',
            'category_description', 'subprimal', 'grade', 'size', 'size_uom', 
            'brand', 'bone_in', 'confidence'
        ]
    
    def prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare DataFrame with proper column order and types."""
        
        # Ensure all required columns exist
        for col in self.final_schema:
            if col not in df.columns:
                if col == 'bone_in':
                    df[col] = False
                else:
                    df[col] = None
        
        # Select and order columns (remove needs_review for final output)
        output_df = df[self.final_schema].copy()
        
        # Clean up null values for better CSV output
        output_df = output_df.fillna('')
        
        return output_df
    
    def separate_quality_flags(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Separate records that need review from clean records."""
        
        # Records needing review: needs_review=True OR confidence < 0.5
        if 'needs_review' in df.columns:
            flagged_mask = (df['needs_review'] == True) | (df['confidence'] < 0.5)
        else:
            flagged_mask = (df['confidence'] < 0.5)
        
        clean_df = df[~flagged_mask].copy()
        flagged_df = df[flagged_mask].copy()
        
        logger.info(f"Separated data: {len(clean_df)} clean records, {len(flagged_df)} flagged records")
        
        return clean_df, flagged_df
    
    def write_category_outputs(self, df: pd.DataFrame, category: str) -> Dict[str, str]:
        """Write output files for a specific category."""
        
        if df.empty:
            logger.warning(f"No data to write for category: {category}")
            return {}
        
        # Prepare DataFrame
        prepared_df = self.prepare_dataframe(df)
        
        # Separate clean and flagged records
        clean_df, flagged_df = self.separate_quality_flags(prepared_df)
        
        # Generate filenames
        category_safe = category.lower().replace(' ', '_')
        
        clean_csv_path = self.outputs_dir / f"{category_safe}_extracted.csv"
        clean_parquet_path = self.outputs_dir / f"{category_safe}_extracted.parquet"
        flagged_csv_path = self.outputs_dir / f"{category_safe}_extracted_flagged.csv"
        
        output_files = {}
        
        # Write clean records
        if not clean_df.empty:
            try:
                # Ensure proper data types for CSV output
                clean_df = clean_df.astype(str)  # Convert all to string for safe CSV writing
                clean_df.to_csv(clean_csv_path, index=False)
                
                output_files['clean_csv'] = str(clean_csv_path)
                
                logger.info(f"Written {len(clean_df)} clean records to {clean_csv_path}")
                
            except Exception as e:
                logger.error(f"Error writing clean records for {category}: {str(e)}")
                raise
        
        # Write flagged records
        if not flagged_df.empty:
            try:
                flagged_df.to_csv(flagged_csv_path, index=False)
                output_files['flagged_csv'] = str(flagged_csv_path)
                
                logger.info(f"Written {len(flagged_df)} flagged records to {flagged_csv_path}")
                
            except Exception as e:
                logger.error(f"Error writing flagged records for {category}: {str(e)}")
                raise
        
        return output_files
    
    def create_master_excel(self, results: Dict[str, pd.DataFrame]) -> str:
        """Create a master Excel file with all processed data.
        
        Args:
            results: Dictionary of DataFrames by category
            
        Returns:
            str: Path to the created Excel file
        """
        try:
            # Create a timestamp for uniqueness
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            excel_path = self.outputs_dir / f"meat_inventory_master_{timestamp}.xlsx"
            
            # Use ExcelWriter for a single file with multiple sheets
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                # First create a consolidated sheet with all data
                all_data = []
                
                # Collect all data with category labels
                for category, df in results.items():
                    if df.empty:
                        continue
                    
                    # Add category label if not already present
                    category_df = df.copy()
                    if 'category_description' not in category_df.columns:
                        category_df['category_description'] = category
                    
                    all_data.append(category_df)
                
                # Create consolidated sheet if we have data
                if all_data:
                    # Concatenate all data
                    consolidated_df = pd.concat(all_data, ignore_index=True)
                    
                    # Prepare for Excel export
                    consolidated_df = self.prepare_dataframe(consolidated_df)
                    
                    # Write to Excel sheet
                    consolidated_df.to_excel(writer, sheet_name='All_Products', index=False)
                    
                    # Add category-specific sheets
                    for category, df in results.items():
                        if df.empty:
                            continue
                            
                        # Prepare each category sheet
                        category_df = self.prepare_dataframe(df)
                        
                        # Create valid Excel sheet name (31 char limit, no special chars)
                        sheet_name = category.replace(' ', '_')[:31]
                        
                        # Write to category-specific sheet
                        category_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                logger.info(f"Created master Excel file with {len(consolidated_df)} records at {excel_path}")
                return str(excel_path)
                
        except Exception as e:
            logger.error(f"Failed to create master Excel file: {str(e)}")
            return None
            
    def write_all_outputs(self, results: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Write output files for all categories.
        
        Args:
            results: Dictionary of DataFrames by category
            
        Returns:
            Dict: Dictionary of output file paths with keys for each output type
        """
        # Individual category output files
        all_output_files = {}
        
        for category, df in results.items():
            if df.empty:
                logger.warning(f"Skipping empty results for category: {category}")
                continue
            
            try:
                category_files = self.write_category_outputs(df, category)
                all_output_files[category] = category_files
                
            except Exception as e:
                logger.error(f"Failed to write outputs for {category}: {str(e)}")
                continue
        
        # Create master Excel file for Firebase upload if we have any results
        if any(not df.empty for df in results.values()):
            master_excel = self.create_master_excel(results)
            if master_excel:
                all_output_files['excel_master'] = master_excel
                
        return all_output_files
    
    def validate_outputs(self, output_files: Dict[str, Dict[str, str]]) -> bool:
        """Validate that output files were created successfully."""
        
        all_valid = True
        
        for category, files in output_files.items():
            for file_type, file_path in files.items():
                path = Path(file_path)
                
                if not path.exists():
                    logger.error(f"Output file missing: {file_path}")
                    all_valid = False
                elif path.stat().st_size == 0:
                    logger.warning(f"Output file is empty: {file_path}")
                else:
                    logger.debug(f"Output file validated: {file_path}")
        
        return all_valid 