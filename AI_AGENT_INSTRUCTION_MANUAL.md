# AI Agent Instruction Manual: Beef Grade System Updates

## 📋 Overview

This manual provides step-by-step instructions for an AI agent to validate that two critical updates to the beef processing pipeline are working correctly:

1. **Canadian Grade System**: Verify support for Canadian beef grades (AAA, AA, A)
2. **New Hereford Grade**: Verify "Hereford" is recognized as a beef grade

The focus is on reviewing system outputs to confirm these changes have been properly implemented.

## 🎯 Objectives

- [ ] Verify `ReferenceDataLoader` automatically picks up new grades
- [ ] Update dynamic prompt generation to include new grades
- [ ] If necessary, update prompt to better account for new grades
- [ ] Test the complete pipeline with new grade recognition
- [ ] Review outputs to ensure new grades are being picked up

## 📊 Validation Steps

### Step 1: Check Reference Data Loading

**Task**: Verify the new grades are loaded from beef_cuts.xlsx

**Instructions**:

1. **Test grade loading**:
```bash
python -c "
from src.data_ingestion.utils.reference_data_loader import ReferenceDataLoader
rd = ReferenceDataLoader()
grades = rd.get_grades()
print('All loaded grades:', sorted(grades))
print('Canadian AAA found:', 'Canadian AAA' in grades)
print('Canadian AA found:', 'Canadian AA' in grades) 
print('Canadian A found:', 'Canadian A' in grades)
print('Hereford found:', 'Hereford' in grades)
"
```

**Expected Output**:
- Should show "Canadian AAA found: True"
- Should show "Canadian AA found: True"
- Should show "Canadian A found: True"  
- Should show "Hereford found: True"

### Step 2: Test Grade Synonym Recognition

**Task**: Verify grade synonyms work correctly

**Instructions**:

1. **Test synonym mapping**:
```bash
python -c "
from src.data_ingestion.utils.reference_data_loader import ReferenceDataLoader
rd = ReferenceDataLoader()
grade_terms = rd.get_all_grade_terms()
print('AAA synonym found:', 'AAA' in grade_terms)
print('AA synonym found:', 'AA' in grade_terms)
print('A synonym found:', 'A' in grade_terms)
print('All grade terms:', sorted(grade_terms))
"
```

**Expected Output**:
- Should show "AAA synonym found: True"
- Should show "AA synonym found: True"
- Should show "A synonym found: True"

### Step 3: Test Extraction Pipeline

**Task**: Test actual extraction with new grades

**Instructions**:

1. **Create test file**: `test_new_grades.csv`
```csv
product_code,product_description,category_description
TEST001,Beef Chuck Shoulder Clod AAA 15#,Beef Chuck
TEST002,Beef Loin Strip Loin Canadian AA 8oz,Beef Loin
TEST003,Beef Rib Prime Rib Hereford 12lb,Beef Rib
TEST004,Chuck Roll AA Premium 10lb,Beef Chuck
TEST005,Hereford Chuck Eye Roll 6oz,Beef Chuck
```

2. **Run extraction test**:
```bash
# Copy test file to incoming directory
cp test_new_grades.csv data/incoming/

# Run pipeline test
python src/run_pipeline.py --categories "Beef Chuck" --test-run
```

3. **Check outputs**:
```bash
# Look for extracted grades in output files
ls -la outputs/
grep -i "canadian\|aaa\|hereford" outputs/*.csv 2>/dev/null || echo "No new grades found in outputs"
```

### Step 4: Review Extraction Results

**Task**: Manually review extraction results for new grades

**Instructions**:

1. **Check latest output files**:
```bash
# Find most recent output files
find outputs/ -name "*.csv" -newer outputs/beef_chuck_extracted.csv 2>/dev/null | head -5

# Or check all recent CSV files
ls -lt outputs/*.csv | head -5
```

2. **Review extracted grades**:
```bash
# Check for Canadian grades in any CSV files
find outputs/ -name "*.csv" -exec grep -l -i "canadian\|aaa.*grade\|hereford" {} \;

# Look at specific extraction results
tail -20 outputs/beef_chuck_extracted.csv 2>/dev/null || echo "File not found"
```

3. **Check master Excel file**:
```bash
# Find latest master Excel file
ls -lt outputs/master_beef_extraction_*.xlsx 2>/dev/null | head -1
```

### Step 5: Validate Confidence Scores

**Task**: Ensure new grades have reasonable confidence scores

**Instructions**:

1. **Check extraction confidence**:
```bash
# Look for confidence scores in recent extraction logs
tail -50 logs/pipeline.log | grep -i "confidence\|canadian\|aaa\|hereford"
```

2. **Review cache for new grades**:
```bash
# Check if new grades appear in cache
grep -i "canadian\|aaa\|hereford" data/processed/.fast_batch_cache.json 2>/dev/null || echo "No new grades in cache yet"
```

## 📋 Validation Checklist

**Grade Recognition Checklist**:

- [ ] **Canadian AAA**: Found in reference data loader
- [ ] **Canadian AA**: Found in reference data loader  
- [ ] **Canadian A**: Found in reference data loader
- [ ] **Hereford**: Found in reference data loader
- [ ] **AAA Synonym**: Recognized as "Canadian AAA"
- [ ] **AA Synonym**: Recognized as "Canadian AA"
- [ ] **A Synonym**: Recognized as "Canadian A"
- [ ] **Extraction Works**: New grades extracted from test descriptions
- [ ] **Confidence Scores**: Reasonable confidence (>0.5) for new grades
- [ ] **Output Files**: New grades appear in CSV/Excel outputs
- [ ] **No Regression**: Existing grades still work correctly

## 🔍 Common Issues to Look For

**Issue 1**: New grades not in reference data
- **Symptom**: Grade loading test shows "False" for new grades
- **Check**: Verify beef_cuts.xlsx has Grades sheet with new entries

**Issue 2**: Synonyms not working
- **Symptom**: "AAA" not found in grade terms but "Canadian AAA" is
- **Check**: Verify synonyms column in Excel has proper format

**Issue 3**: Extraction not working
- **Symptom**: Test descriptions with new grades don't extract properly
- **Check**: Look at pipeline logs for errors

**Issue 4**: Low confidence scores
- **Symptom**: New grades extracted but with confidence < 0.5
- **Check**: Review dynamic prompt generation includes new grades

## 📊 Success Criteria

**Must Pass**:
- All new grades (Canadian AAA, AA, A, Hereford) found in reference data
- Synonyms (AAA, AA, A) map to correct canonical grades
- Test extractions work for new grades
- Output files contain new grades when processing test data

**Should Pass**:
- Confidence scores >0.5 for new grade extractions
- No regression in existing grade recognition
- Cache system works with new grades

## 🚀 Quick Validation Script

**Run this complete validation**:

```bash
#!/bin/bash
echo "🔍 Validating New Grade Implementation..."

# Test 1: Reference Data
echo "📚 Testing reference data loading..."
python -c "
from src.data_ingestion.utils.reference_data_loader import ReferenceDataLoader
rd = ReferenceDataLoader()
grades = rd.get_grades()
new_grades = ['Canadian AAA', 'Canadian AA', 'Canadian A', 'Hereford']
for grade in new_grades:
    found = grade in grades
    print(f'✅ {grade}: {found}' if found else f'❌ {grade}: {found}')
"

# Test 2: Synonyms
echo "📝 Testing grade synonyms..."
python -c "
from src.data_ingestion.utils.reference_data_loader import ReferenceDataLoader
rd = ReferenceDataLoader()
terms = rd.get_all_grade_terms()
synonyms = ['AAA', 'AA', 'A']
for syn in synonyms:
    found = syn in terms
    print(f'✅ {syn} synonym: {found}' if found else f'❌ {syn} synonym: {found}')
"

# Test 3: Check recent outputs
echo "📤 Checking recent outputs..."
find outputs/ -name "*.csv" -newer outputs/beef_chuck_extracted.csv 2>/dev/null | while read file; do
    if grep -q -i "canadian\|aaa\|hereford" "$file" 2>/dev/null; then
        echo "✅ New grades found in: $(basename $file)"
    fi
done

echo "🎉 Validation complete!"
```

---

**Validation Time Estimate**: 30-60 minutes  
**Focus**: Verification only - no implementation changes  
**Success Metric**: All new grades properly recognized and extracted

## 📝 Documentation Updates

After successful implementation, update the following documentation:

1. **README.md**: Add Canadian grades to the extracted attributes table
2. **Dynamic prompt examples**: Include Canadian grade examples
3. **API documentation**: Update grade field descriptions
4. **User guides**: Add Canadian grading system explanation

## 🔒 Rollback Plan

If issues arise, follow this rollback procedure:

1. **Backup current beef_cuts.xlsx**:
```bash
cp data/incoming/beef_cuts.xlsx data/incoming/beef_cuts_backup.xlsx
```

2. **Restore original file** if needed

3. **Clear caches**:
```bash
rm -f data/processed/.*.json
```

4. **Revert code changes** using git:
```bash
git checkout HEAD -- src/AIs/llm_extraction/specific_extractors/dynamic_prompt_generator.py
```

---

**Implementation Time Estimate**: 2-4 hours
**Testing Time Estimate**: 1-2 hours  
**Total Time Estimate**: 3-6 hours

**Priority**: High - Required for Canadian market support
**Risk Level**: Medium - Affects core extraction logic
**Dependencies**: None - Self-contained changes 