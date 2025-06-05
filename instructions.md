# Beef Cut Extraction Pipeline Extension

## Objective

Extend the existing beef chuck extraction pipeline to dynamically handle all primal beef cuts listed in the `beef_cuts.xlsx` spreadsheet, ensuring the pipeline stays in sync with any changes to the spreadsheet at runtime without requiring code changes.

## Architecture Decision

After evaluating the options, we've selected a **Dynamic Prompt Generation** approach for handling all primal cuts. Here's the rationale and implementation plan:

### Design Decision: Dynamic Prompts vs. Per-Primal Classes

#### Selected Approach: Dynamic Prompts

We've implemented a dynamic prompt generation system that:
- Loads all primal cut data from the spreadsheet at runtime
- Generates specialized prompts for each primal cut using the reference data
- Maintains a single, flexible extractor class that works with any primal
- Preserves backward compatibility with the existing beef chuck extraction functionality

#### Rationale for This Approach

1. **Maintainability**: A single extractor with dynamic prompts is easier to maintain than separate classes for each primal cut. When extraction logic needs to be updated, changes only need to be made in one place.

2. **Scalability**: As new primal cuts are added to the spreadsheet, the pipeline automatically incorporates them without code changes.

3. **Consistency**: The single-class approach ensures consistent extraction methodology across all primal cuts.

4. **DRY Principle**: Avoids duplicating extraction logic across multiple similar classes.

5. **Runtime Flexibility**: The system can dynamically adjust to changes in the reference data without requiring redeployment.

6. **Backward Compatibility**: The specialized beef chuck extractor is still available for backward compatibility.

## Implementation Details

### New Components

1. **ReferenceDataLoader (`src/data_ingestion/utils/reference_data_loader.py`)**
   - Loads and manages the beef cut reference data from the Excel spreadsheet
   - Provides structured access to primal cuts, subprimals, and their synonyms
   - Also loads grade information and synonyms

2. **DynamicPromptGenerator (`src/LLM/prompts/dynamic_prompt_generator.py`)**
   - Generates system and user prompts customized for each primal cut
   - Incorporates reference data from the spreadsheet into the prompts
   - Provides post-processing rules specific to each primal

3. **DynamicBeefExtractor (`src/LLM/extractors/dynamic_beef_extractor.py`)**
   - Handles extraction for any primal cut using dynamic prompts
   - Uses the reference data to inform extraction and post-processing
   - Includes primal-cut inference from descriptions
   - Supports batch processing with primal hints

### Updated Components

1. **ExtractionController (`src/LLM/extraction_controller.py`)**
   - Now supports both the specialized beef chuck extractor and the dynamic extractor
   - Automatically maps all primal cuts to the appropriate extractor
   - Provides backward compatibility while supporting the new functionality
   - Includes flexible batch and single extraction methods

## Data Flow

1. **Initialization**:
   - Reference data is loaded from `beef_cuts.xlsx` at startup
   - Extractors are initialized with the reference data
   - Category-to-extractor mappings are created dynamically

2. **Extraction Process**:
   - For known beef chuck descriptions: use the specialized extractor
   - For other primal cuts: use the dynamic extractor with primal hint
   - For unknown categories: attempt to infer the primal cut or use generic approach

3. **Output**:
   - Consistent JSON format across all primal cuts
   - Extraction results include primal cut information
   - Error handling preserves the same behavior as the original system

## Synchronization with Spreadsheet

The system dynamically loads the spreadsheet at runtime and builds all necessary structures from it:

- No hard-coded lists of cuts or synonyms
- Reference data is refreshed when the system starts
- Updates to the spreadsheet are automatically reflected in the next run

## Testing and Validation

To ensure the extension maintains quality across all primal cuts:

1. Run extraction using the `extract_batch` method with a mixed dataset of beef cuts
2. Compare results between specialized beef chuck extractor and dynamic extractor
3. Verify consistent output format and quality
4. Test with spreadsheet updates to confirm synchronization

## Notes for Future Enhancements

1. Could add caching for reference data to improve performance
2. Potential for extending to non-beef species like pork or lamb
3. Consider adding a validation layer for spreadsheet data integrity