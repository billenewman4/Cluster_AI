#!/usr/bin/env python3
"""Test Stage 3 of the pipeline - Output Generation"""

import sys
sys.path.append('src')

import pandas as pd
from output_generation import FileWriter, ReportGenerator

def test_stage3():
    """Test output generation stage."""
    print("Testing Stage 3: Output Generation")
    print("=" * 40)
    
    try:
        # Create sample extracted data for testing
        sample_data = {
            'source_filename': ['test_file.xlsx'] * 5,
            'row_number': [1, 2, 3, 4, 5],
            'product_code': ['CHK001', 'CHK002', 'CHK003', 'CHK004', 'CHK005'],
            'raw_description': [
                'Prime Chuck Flap 15# USDA Choice',
                'Angus Flat Iron Steaks 8oz',
                'Choice Grade Clod Shoulder 12#',
                'Certified Angus Chuck Roll 20#',
                'Select Teres Major 3oz portions'
            ],
            'category_description': ['Beef Chuck'] * 5,
            'species': ['Beef'] * 5,
            'primal': ['Chuck'] * 5,
            'subprimal': ['chuck flap', 'flat iron', 'clod', 'chuck roll', 'teres major'],
            'grade': ['Prime', 'Choice', 'Choice', 'Choice', 'Select'],
            'size': [15.0, 8.0, 12.0, 20.0, 3.0],
            'size_uom': ['#', 'oz', '#', '#', 'oz'],
            'brand': ['USDA', 'Angus', None, 'Certified Angus', None],
            'bone_in': [False, False, True, False, False],
            'llm_confidence': [0.85, 0.92, 0.78, 0.88, 0.95],
            'needs_review': [False, False, True, False, False]
        }
        
        df = pd.DataFrame(sample_data)
        print(f"✅ Created sample data: {len(df)} records")
        
        # Test FileWriter
        writer = FileWriter()
        print(f"✅ FileWriter initialized")
        
        # Test writing outputs
        output_files = writer.write_category_outputs(df, 'beef_chuck')
        print(f"✅ Output files written successfully")
        print(f"Output files: {list(output_files.keys())}")
        
        # Test ReportGenerator
        reporter = ReportGenerator()
        print(f"✅ ReportGenerator initialized")
        
        # Generate reports
        summary = reporter.generate_summary_stats(df)
        print(f"✅ Summary stats generated")
        print(f"Summary: {summary}")
        
        # Test detailed report with correct signature
        results_dict = {'beef_chuck': df}
        detailed_report = reporter.generate_detailed_report(results_dict)
        print(f"✅ Detailed report generated")
        print(f"Report length: {len(detailed_report)} characters")
        
        print("\n✅ Stage 3 (Output Generation) working correctly!")
        return True
        
    except Exception as e:
        print(f'❌ Error in Stage 3: {e}')
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_stage3()
    sys.exit(0 if success else 1) 