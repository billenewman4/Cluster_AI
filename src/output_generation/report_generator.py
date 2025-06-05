"""
Report Generator Module
Creates summary reports and statistics for pipeline results.
"""

import pandas as pd
import json
import logging
from datetime import datetime
from typing import Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)

class ReportGenerator:
    """Generates summary reports and statistics."""
    
    def __init__(self, logs_dir: str = "logs"):
        self.logs_dir = Path(logs_dir)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_summary_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate comprehensive summary statistics for a single DataFrame."""
        
        if df.empty:
            return {
                'total_records': 0,
                'clean_records': 0,
                'flagged_records': 0,
                'average_confidence': 0.0
            }
        
        # Separate clean and flagged
        needs_review_col = df.get('needs_review', pd.Series([False] * len(df)))
        confidence_col = df.get('llm_confidence', pd.Series([0.0] * len(df)))
        
        flagged_mask = (needs_review_col == True) | (confidence_col < 0.5)
        clean_count = (~flagged_mask).sum()
        flagged_count = flagged_mask.sum()
        
        # Generate detailed breakdowns
        stats = {
            'total_records': len(df),
            'clean_records': clean_count,
            'flagged_records': flagged_count,
            'average_confidence': confidence_col.mean(),
            'subprimal_breakdown': df['subprimal'].value_counts().to_dict() if 'subprimal' in df.columns else {},
            'grade_breakdown': df['grade'].value_counts().to_dict() if 'grade' in df.columns else {},
            'size_distribution': {
                'average_size': df['size'].mean() if 'size' in df.columns else None,
                'size_unit_breakdown': df['size_uom'].value_counts().to_dict() if 'size_uom' in df.columns else {}
            },
            'bone_in_count': df['bone_in'].sum() if 'bone_in' in df.columns else 0,
            'brand_breakdown': df['brand'].value_counts().head(10).to_dict() if 'brand' in df.columns else {}
        }
        
        return stats
    
    def estimate_api_cost(self, summary: Dict, cost_per_1k_tokens: float = 0.03) -> float:
        """Estimate API costs based on usage."""
        # Rough estimate: ~100 tokens per request for specialized prompts
        total_requests = summary['total_records_processed']
        estimated_tokens = total_requests * 100
        estimated_cost = (estimated_tokens / 1000) * cost_per_1k_tokens
        
        return estimated_cost
    
    def write_json_log(self, summary: Dict, output_files: Dict[str, Dict]) -> str:
        """Write structured JSON log."""
        
        log_data = {
            'pipeline_run': summary,
            'output_files': output_files,
            'exit_code': 0 if summary['total_flagged_records'] == 0 else 1
        }
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = self.logs_dir / f"pipeline_run_{timestamp}.json"
        
        try:
            with open(log_path, 'w') as f:
                json.dump(log_data, f, indent=2, default=str)
            
            logger.info(f"JSON log written to {log_path}")
            return str(log_path)
            
        except Exception as e:
            logger.error(f"Error writing JSON log: {str(e)}")
            raise
    
    def print_console_summary(self, summary: Dict) -> None:
        """Print human-readable summary to console."""
        
        print("\n" + "="*70)
        print("           MEAT INVENTORY PIPELINE SUMMARY")
        print("="*70)
        
        print(f"Timestamp: {summary['timestamp']}")
        print(f"Categories Processed: {summary['categories_processed']}")
        print(f"Total Records: {summary['total_records_processed']}")
        print(f"Clean Records: {summary['total_clean_records']}")
        print(f"Flagged Records: {summary['total_flagged_records']}")
        
        if summary['total_records_processed'] > 0:
            clean_pct = (summary['total_clean_records'] / summary['total_records_processed']) * 100
            print(f"Success Rate: {clean_pct:.1f}%")
        
        # Add cost estimate if available
        if 'estimated_api_cost_usd' in summary:
            print(f"Estimated API Cost: ${summary['estimated_api_cost_usd']:.4f}")
        
        print("\nCATEGORY BREAKDOWN:")
        print("-" * 40)
        
        for category, details in summary['category_details'].items():
            print(f"\n{category}:")
            print(f"  Total Records: {details['total_records']}")
            print(f"  Clean: {details['clean_records']} | Flagged: {details['flagged_records']}")
            print(f"  Avg Confidence: {details['average_confidence']:.3f}")
            
            # Show top subprimals detected
            if details['subprimal_breakdown']:
                top_subprimals = dict(list(details['subprimal_breakdown'].items())[:5])
                print(f"  Top Subprimals: {top_subprimals}")
            
            # Show grade distribution
            if details['grade_breakdown']:
                top_grades = dict(list(details['grade_breakdown'].items())[:3])
                print(f"  Grades: {top_grades}")
            
            # Show bone-in stats
            if 'bone_in_count' in details:
                bone_in_pct = (details['bone_in_count'] / details['total_records']) * 100
                print(f"  Bone-In Products: {details['bone_in_count']} ({bone_in_pct:.1f}%)")
        
        print("\n" + "="*70)
        
        if summary['total_flagged_records'] > 0:
            print(f"⚠️  WARNING: {summary['total_flagged_records']} records require manual review")
            print("   Check the *_flagged.csv files for details")
        else:
            print("✅ All records processed successfully!")
        
        print("="*70 + "\n")
    
    def generate_detailed_report(self, results: Dict[str, pd.DataFrame]) -> str:
        """Generate a detailed text report."""
        
        report_lines = []
        report_lines.append("DETAILED MEAT INVENTORY PIPELINE REPORT")
        report_lines.append("=" * 50)
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")
        
        for category, df in results.items():
            if df.empty:
                continue
            
            report_lines.append(f"CATEGORY: {category.upper()}")
            report_lines.append("-" * 30)
            
            # Basic stats
            total_records = len(df)
            flagged_records = (df['needs_review'] == True).sum()
            avg_confidence = df['llm_confidence'].mean()
            
            report_lines.append(f"Total Records: {total_records}")
            report_lines.append(f"Flagged for Review: {flagged_records}")
            report_lines.append(f"Average Confidence: {avg_confidence:.3f}")
            report_lines.append("")
            
            # Subprimal breakdown
            if 'subprimal' in df.columns:
                subprimal_counts = df['subprimal'].value_counts()
                report_lines.append("SUBPRIMAL CUTS DETECTED:")
                for subprimal, count in subprimal_counts.head(10).items():
                    if pd.notna(subprimal):
                        pct = (count / total_records) * 100
                        report_lines.append(f"  {subprimal}: {count} ({pct:.1f}%)")
                report_lines.append("")
            
            # Grade breakdown
            if 'grade' in df.columns:
                grade_counts = df['grade'].value_counts()
                report_lines.append("GRADE DISTRIBUTION:")
                for grade, count in grade_counts.items():
                    if pd.notna(grade):
                        pct = (count / total_records) * 100
                        report_lines.append(f"  {grade}: {count} ({pct:.1f}%)")
                report_lines.append("")
            
            # Quality issues
            low_confidence = df[df['llm_confidence'] < 0.5]
            if len(low_confidence) > 0:
                report_lines.append("QUALITY CONCERNS:")
                report_lines.append(f"  Low confidence records: {len(low_confidence)}")
                
                # Show some examples
                if len(low_confidence) > 0:
                    report_lines.append("  Sample low-confidence descriptions:")
                    for _, row in low_confidence.head(3).iterrows():
                        desc = row['raw_description'][:60] + "..." if len(row['raw_description']) > 60 else row['raw_description']
                        report_lines.append(f"    - {desc} (confidence: {row['llm_confidence']:.2f})")
                report_lines.append("")
        
        return "\n".join(report_lines) 