# Implementation Plan

## Current State
The current implementation uses a graph-based workflow with three main components:
1. **Dynamic Beef Extractor** - Extracts beef attributes from product descriptions
2. **Clarification Processor** - Generates clarification questions
3. **Review Processor** - Reviews and corrects extraction results

## Requested Changes

1. **Remove clarification and review parts from the graph workflow**
   - Simplify the graph to only use the extraction component
   - Remove unnecessary nodes and conditional logic

2. **Create separate LLM extractors for different aspects**
   - Keep existing extractor but make it subprimal only (currently DynamicBeefExtractor)
   - Create new grade extractor (new component)
   - Create new USDA codes extractor (new placeholder component)

## Implementation Steps

### 1. Simplify the Graph Workflow

1. Modify `graph.py` to:
   - Remove imports for clarification and review processors
   - Remove clarification_node and review_node methods
   - Simplify the graph to only have the extraction node and completion node
   - Update the ProcessingState class to remove clarification and review fields
   - Update the process_product method to skip clarification and review steps

### 2. Create New Extractors

1. **Modify DynamicBeefExtractor** for subprimal extraction only
   - Update the extraction logic to focus solely on subprimal extraction
   - Remove grade extraction functionality
   - Update prompt templates to focus only on subprimal attributes

2. **Create GradeExtractor**
   - Create new file: `src/AIs/llm_extraction/specific_extractors/grade_extractor.py`
   - Inherit from BaseLLMExtractor
   - Focus on extracting only grade information from descriptions
   - Reuse reference data and grade mapping logic from existing extractors
   - Implement specialized prompt templates for grade extraction

3. **Create USDACodesExtractor (placeholder)**
   - Create new file: `src/AIs/llm_extraction/specific_extractors/usda_codes_extractor.py`
   - Inherit from BaseLLMExtractor
   - Create basic structure for extracting USDA codes
   - Add placeholder methods for implementation later

### 3. Update Pipeline Integration

1. Modify `run_pipeline.py` to:
   - Import and initialize all three extractors (subprimal, grade, USDA codes)
   - Call each extractor for its specific purpose
   - Combine the extraction results into a single output
   - Update the result processing to handle the new data structure

### 4. Testing

1. Create unit tests for each extractor to verify their specialized extraction capabilities
2. Test the integration with sample product descriptions
3. Verify that the extraction results are accurate and complete

## Progress Tracking

- [x] Review current code structure
- [ ] Modify graph.py to remove clarification and review parts
- [ ] Modify DynamicBeefExtractor for subprimal-only extraction
- [ ] Create GradeExtractor
- [ ] Create USDACodesExtractor placeholder
- [ ] Update pipeline integration
- [ ] Create and run tests
- [ ] Document the new architecture
