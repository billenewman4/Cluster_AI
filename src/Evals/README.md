# Beef Extraction Model Evaluation Framework

This directory contains the evaluation framework for the beef extraction model that predicts sub-primal cuts and grades from product descriptions.

## 📊 Overview

The evaluation framework uses LangSmith to systematically test the `DynamicBeefExtractor` model's performance across different beef primal cuts. It provides automated evaluation of:

- **Sub-primal prediction accuracy**
- **Grade prediction accuracy** 
- **Confidence scoring**
- **Performance across different primal cuts**

## 🗂️ Directory Structure

```
src/Evals/
├── README.md                    # This file
├── EVALUATION_PRD.md           # Product Requirements Document
├── eval_config.yaml            # Configuration settings
├── eval_process.py             # Main evaluation script
├── Data_processing/            # Data conversion utilities
│   └── excelToParquet.py      # Excel to Parquet converter
├── model_caller/               # Model interface
│   └── model_caller.py        # Batch model calling interface
└── data/                       # Evaluation datasets
    ├── ground_truth/           # Ground truth data
    ├── input_data/            # Reference data
    └── processed/             # Processed datasets
```

## 📋 Available Datasets

The following LangSmith datasets are available for evaluation:

### Primary Dataset
- **`Beef Extraction Evaluation`** - Original comprehensive dataset (285 examples, Chuck primal only)

### Primal-Specific Test Datasets
- **`test_chuck`** - Chuck primal cuts (9 examples)
- **`test_loin`** - Loin primal cuts (5 examples)
- **`test_rib`** - Rib primal cuts (8 examples)
- **`test_round`** - Round primal cuts (6 examples)
- **`test_flank`** - Flank primal cuts (4 examples)
- **`test_variety`** - Variety cuts (5 examples)
- **`test_ground`** - Ground beef products (6 examples)
- **`test_other`** - Other beef products (4 examples)

**Total: 47 examples across 8 primal categories**

## 🚀 Quick Start

### Prerequisites

1. **Environment Setup**:
   ```bash
   # Ensure you have LangSmith API key configured
   export LANGCHAIN_API_KEY="your_langsmith_api_key"
   export LANGCHAIN_TRACING_V2=true
   ```

2. **Dependencies**:
   - LangSmith client
   - pandas
   - DynamicBeefExtractor model

### Running Evaluations

#### Single Dataset Evaluation
```python
from src.Evals.eval_process import eval_process

# Run evaluation on a specific dataset
eval_process("test_chuck")
eval_process("test_loin")
# ... etc
```

#### Command Line Evaluation
```bash
# Run evaluation on the original comprehensive dataset
python3 src/Evals/eval_process.py

# Run evaluation on specific primal dataset
python3 -c "from src.Evals.eval_process import eval_process; eval_process('test_chuck')"
```

#### Batch Evaluation (All Primals)
```bash
for dataset in test_chuck test_loin test_rib test_round test_flank test_variety test_ground test_other; do
  echo "Running evaluation for $dataset"
  python3 -c "from src.Evals.eval_process import eval_process; eval_process('$dataset')"
done
```

## 📈 Evaluation Metrics

The framework evaluates two primary metrics:

### 1. Sub-primal Accuracy
- **Metric**: Exact match between predicted and ground truth sub-primal
- **Scoring**: Binary (1.0 for exact match, 0.0 for mismatch)
- **Purpose**: Measures the model's ability to correctly identify specific cuts

### 2. Grade Accuracy  
- **Metric**: Exact match between predicted and ground truth grade
- **Scoring**: Binary (1.0 for exact match, 0.0 for mismatch)
- **Purpose**: Measures the model's ability to correctly identify quality grades

## 🔧 Configuration

Edit `eval_config.yaml` to customize:

```yaml
# Path configurations
ground_truth_path: src/Evals/data/ground_truth/ground_truth.parquet
reference_data_path: src/Evals/data/input_data/beef_cuts.xlsx
processed_dir: src/Evals/data/processed
results_dir: src/Evals/data/results

# Column mappings
columns:
  description: product_description
  subprimal: sub_primal_gt
  grade: grade_gt

# Evaluation settings
random_seed: 42
```

## 📊 Results and Analysis

### LangSmith Integration

All evaluation results are automatically tracked in LangSmith with:
- **Experiment tracking** with unique IDs
- **Detailed comparison views** for each dataset
- **Performance metrics** and scoring
- **Individual prediction analysis**

### Accessing Results

Each evaluation run provides a LangSmith URL for detailed analysis:
```
View the evaluation results for experiment: 'beef-extraction-eval-[ID]' at:
https://smith.langchain.com/o/[org]/datasets/[dataset]/compare?selectedSessions=[session]
```

## 🛠️ Development

### Adding New Datasets

1. **Prepare Data**: Ensure CSV/Excel files have required columns:
   - `product_description`
   - `category_description` 
   - `subprimal`
   - `grade`

2. **Create Dataset**: Use the dataset creation script:
   ```python
   # See create_datasets.py for reference implementation
   ```

3. **Run Evaluation**: Use existing `eval_process.py` with new dataset name

### Extending Evaluators

Add custom evaluators in `eval_process.py`:

```python
def custom_evaluator(inputs: dict, outputs: dict, reference_outputs: dict):
    """Custom evaluation logic"""
    # Your evaluation logic here
    return {
        "key": "custom_metric",
        "score": score,
        "comment": "Evaluation details"
    }
```

## 📝 Data Sources

### Ground Truth Data
- **Source**: Pipeline extraction results from test runs
- **Format**: CSV files with model predictions as ground truth
- **Coverage**: 8 beef primal categories
- **Size**: 47 total examples

### Reference Data
- **Source**: `beef_cuts.xlsx` - Comprehensive beef cut reference
- **Purpose**: Provides canonical names and synonyms for validation
- **Coverage**: All major beef primals and sub-primals

## 🔍 Troubleshooting

### Common Issues

1. **Import Errors**:
   ```bash
   # Ensure src is in Python path
   export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
   ```

2. **LangSmith Connection**:
   ```bash
   # Verify API key is set
   echo $LANGCHAIN_API_KEY
   ```

3. **Dataset Not Found**:
   ```python
   # List available datasets
   from langsmith import Client
   client = Client()
   datasets = list(client.list_datasets())
   print([d.name for d in datasets])
   ```

### Performance Considerations

- **API Rate Limits**: Evaluations include built-in rate limiting
- **Batch Size**: Default concurrency is 2 to avoid overwhelming the API
- **Timeout**: Each evaluation has reasonable timeouts for stability

## 📚 Additional Resources

- **[EVALUATION_PRD.md](./EVALUATION_PRD.md)** - Detailed product requirements
- **[LangSmith Documentation](https://docs.smith.langchain.com/)** - LangSmith evaluation guide
- **[Model Documentation](../AIs/llm_extraction/specific_extractors/)** - DynamicBeefExtractor details

## 🎯 Future Enhancements

- [ ] **Fuzzy matching** for sub-primal evaluation
- [ ] **Confidence score analysis** and thresholding
- [ ] **Cross-primal performance** comparison
- [ ] **Automated regression testing** on model updates
- [ ] **Performance benchmarking** and optimization
- [ ] **Custom evaluation metrics** for domain-specific requirements 