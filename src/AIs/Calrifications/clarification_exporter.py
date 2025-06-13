"""
Clarification Exporter: Export clarification questions to CSV
"""

import csv
import logging
from datetime import datetime
from typing import List, Optional
from pathlib import Path

from .clarification_processor import ClarificationResult

logger = logging.getLogger(__name__)

class ClarificationExporter:
    """Exports clarification questions to CSV."""
    
    def __init__(self, output_dir: str = "outputs/clarification"):
        """Initialize exporter with output directory."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def export_to_csv(self, results: List[ClarificationResult], filename: Optional[str] = None) -> str:
        """Export clarification results to CSV."""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"clarification_questions_{timestamp}.csv"
        
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Header with extraction results
            writer.writerow([
                'Product Code', 
                'Product Description', 
                'Question',
                'Subprimal',
                'Grade', 
                'Size',
                'Size UOM',
                'Brand',
                'Bone In',
                'Confidence',
                'Needs Review'
            ])
            
            # Data rows - one row per question
            total_questions = 0
            for result in results:
                # Extract common fields
                extraction = result.extraction_results or {}
                subprimal = extraction.get('subprimal', '')
                grade = extraction.get('grade', '')
                size = extraction.get('size', '')
                size_uom = extraction.get('size_uom', '')
                brand = extraction.get('brand', '')
                bone_in = extraction.get('bone_in', '')
                confidence = extraction.get('confidence', '')
                needs_review = extraction.get('needs_review', '')
                
                for question in result.questions:
                    writer.writerow([
                        result.product_code,
                        result.product_description,
                        question,
                        subprimal,
                        grade,
                        size,
                        size_uom,
                        brand,
                        bone_in,
                        confidence,
                        needs_review
                    ])
                    total_questions += 1
        
        logger.info(f"Exported {total_questions} questions from {len(results)} products to {filepath}")
        return str(filepath)
    
    def export_summary_csv(self, results: List[ClarificationResult], filename: Optional[str] = None) -> str:
        """Export summary with question counts per product."""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"clarification_summary_{timestamp}.csv"
        
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Header with key extraction fields
            writer.writerow([
                'Product Code', 
                'Product Description', 
                'Question Count', 
                'Has Questions',
                'Subprimal',
                'Grade',
                'Confidence'
            ])
            
            # Data rows
            for result in results:
                extraction = result.extraction_results or {}
                writer.writerow([
                    result.product_code,
                    result.product_description,
                    len(result.questions),
                    'Yes' if result.questions else 'No',
                    extraction.get('subprimal', ''),
                    extraction.get('grade', ''),
                    extraction.get('confidence', '')
                ])
        
        logger.info(f"Exported summary for {len(results)} products to {filepath}")
        return str(filepath)

def export_clarification_results(
    results: List[ClarificationResult], 
    output_dir: str = "outputs/clarification"
) -> dict:
    """
    Main function: Export clarification results to CSV files.
    
    Args:
        results: List of ClarificationResult objects
        output_dir: Directory to save CSV files
    
    Returns:
        Dict with paths to exported files
    """
    logger.info(f"Exporting clarification results for {len(results)} products")
    
    exporter = ClarificationExporter(output_dir)
    exported_files = {}
    
    # Export detailed questions
    questions_file = exporter.export_to_csv(results)
    exported_files['questions'] = questions_file
    
    # Export summary
    summary_file = exporter.export_summary_csv(results)
    exported_files['summary'] = summary_file
    
    # Print summary
    total_questions = sum(len(r.questions) for r in results)
    products_with_questions = sum(1 for r in results if r.questions)
    
    logger.info("="*50)
    logger.info("EXPORT COMPLETE")
    logger.info("="*50)
    logger.info(f"Products: {len(results)}")
    logger.info(f"Products with questions: {products_with_questions}")
    logger.info(f"Total questions: {total_questions}")
    logger.info(f"Files exported: {len(exported_files)}")
    for file_type, path in exported_files.items():
        logger.info(f"  {file_type}: {path}")
    logger.info("="*50)
    
    return exported_files 