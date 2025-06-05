# Meat Inventory Pipeline

An intelligent data processing pipeline that extracts structured information from meat supplier inventory files using Large Language Models (LLMs). The pipeline ingests Excel/CSV files containing product descriptions and automatically extracts standardized meat attributes like species, primal cuts, grades, sizes, and brands.

## 🎯 Features

- **Multi-format Support**: Processes Excel (.xlsx, .xls), CSV, and TSV files
- **Intelligent Column Mapping**: Automatically detects and maps various column naming conventions
- **LLM-Powered Extraction**: Uses OpenAI GPT models to extract structured meat attributes
- **Quality Control**: Flags records that need manual review based on confidence scores
- **Robust Error Handling**: Includes rate limiting, retries, and comprehensive logging
- **Caching**: Prevents duplicate API calls for identical descriptions
- **Comprehensive Testing**: Unit tests with >80% coverage

## 📁 Project Structure

```
meat-inventory-pipeline/
├── data/
│   ├── incoming/          # Raw supplier files (Excel/CSV)
│   └── processed/         # Processed parquet files
├── docs/                  # Reference documentation
├── src/
│   ├── stage1_ingest.py   # Data ingestion and preprocessing
│   ├── stage2_llm.py      # LLM-based attribute extraction
│   ├── stage3_output.py   # Output generation and quality control
│   └── run_pipeline.py    # Main orchestration script
├── outputs/               # Final CSV and parquet outputs
├── logs/                  # Pipeline execution logs
├── tests/                 # Unit tests
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

## 🚀 Quick Start

### Prerequisites

- Python 3.10 or higher
- OpenAI API key
- Supplier inventory files in Excel or CSV format

### Installation

1. **Clone and setup the project**:
```bash
git clone <repository-url>
cd meat-inventory-pipeline
pip install -r requirements.txt
```

2. **Configure environment variables**:
```bash
# Copy the example and edit with your API key
cp env_example.txt .env

# Edit .env file with your OpenAI API key
OPENAI_API_KEY=your_openai_api_key_here
OPENAI_MODEL=gpt-4
MAX_REQUESTS_PER_MINUTE=100
LOG_LEVEL=INFO
```

3. **Place your data files**:
```bash
# Move your supplier files to the incoming directory
cp /path/to/your/files/*.xlsx data/incoming/
```

### Basic Usage

Run the complete pipeline for Beef Chuck category:

```bash
python src/run_pipeline.py --categories "Beef Chuck"
```

Run with verbose logging:

```bash
python src/run_pipeline.py --categories "Beef Chuck" --verbose
```

Process multiple categories:

```bash
python src/run_pipeline.py --categories "Beef Chuck,Beef Loin"
```

Force re-processing of all data:

```bash
python src/run_pipeline.py --categories "Beef Chuck" --force-stage1
```

## 📊 Expected Data Format

Your incoming files should contain at minimum these columns (various naming conventions supported):

| Required Field | Accepted Column Names |
|----------------|-----------------------|
| Product Code   | `product_code`, `item_code`, `code`, `sku` |
| Description    | `product_description`, `description`, `product_name`, `item_name` |
| Category       | `category_description`, `category`, `dept`, `department` |

### Example Input Data

```csv
Item Code,Product Description,Department
A123,Beef Chuck Shoulder Clod 15# Choice Certified Angus,Beef Chuck
B456,Prime Beef Chuck Flat Iron Steak 8oz,Beef Chuck
C789,Wagyu Beef Chuck Roll 12lb,Beef Chuck
```

## 🔄 Pipeline Stages

### Stage 1: Data Ingestion (`stage1_ingest.py`)

- **Input**: Excel/CSV files in `data/incoming/`
- **Process**:
  - Reads all supported files with automatic format detection
  - Normalizes column names to standard format
  - Cleans and standardizes text data
  - Creates audit trail with source filename and row numbers
  - Removes duplicate records
- **Output**: `data/processed/inventory_base.parquet`

### Stage 2: LLM Extraction (`stage2_llm.py`)

- **Input**: Processed parquet file
- **Process**:
  - Filters records by category (e.g., "Beef Chuck")
  - Uses specialized agent profiles for different meat categories
  - Calls OpenAI API with structured prompts
  - Applies post-processing rules and validation
  - Implements rate limiting and caching
- **Output**: Structured DataFrames with extracted attributes

### Stage 3: Output Generation (`stage3_output.py`)

- **Input**: Extraction results from Stage 2
- **Process**:
  - Validates data quality and completeness
  - Separates clean records from those needing review
  - Generates summary statistics and reports
  - Estimates API costs
- **Output**: 
  - `outputs/{category}_extracted.csv` - Clean records
  - `outputs/{category}_extracted_flagged.csv` - Records needing review
  - `outputs/{category}_extracted.parquet` - Analytics-ready format
  - `logs/pipeline_run_{timestamp}.json` - Execution summary

## 📋 Extracted Attributes

For each product description, the pipeline extracts:

| Attribute | Description | Example Values |
|-----------|-------------|----------------|
| `species` | Type of meat | Beef, Pork, Lamb |
| `primal` | Primary cut section | Chuck, Loin, Rib |
| `subprimal` | Specific cut within primal | Shoulder Clod, Flat Iron |
| `grade` | USDA grade or quality level | Prime, Choice, Select, Wagyu |
| `size` | Numeric size value | 15, 8.5 |
| `size_uom` | Unit of measurement | lb, oz, #, kg |
| `brand` | Brand or certification | Certified Angus, Creekstone |
| `llm_confidence` | Extraction confidence score | 0.0 to 1.0 |

## 🔧 Configuration

### Agent Profiles

Currently supports **Beef Chuck** category with specialized prompts and validation rules. Additional categories can be added by extending the agent profiles in `stage2_llm.py`.

### Quality Control Thresholds

Records are flagged for review if:
- LLM confidence score < 0.5
- Invalid grades detected
- Cuts not found in reference hierarchy
- Invalid size units

### Rate Limiting

- Default: 100 requests/minute
- Implements exponential backoff with jitter
- Configurable via environment variables

## 🧪 Testing

Run the test suite:

```bash
# Install test dependencies
pip install pytest

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

## 📈 Monitoring and Logs

### Execution Logs

All pipeline runs generate structured JSON logs in `logs/` with:
- Execution timestamps and duration
- Record counts and success rates
- API usage and cost estimates
- Error details and warnings

### Output Summary

Each run produces a comprehensive summary:

```
============================================================
           MEAT INVENTORY PIPELINE SUMMARY
============================================================
Timestamp: 2024-01-15T10:30:00
Categories Processed: 1
Total Records: 1,250
Clean Records: 1,180
Flagged Records: 70
Success Rate: 94.4%

CATEGORY BREAKDOWN:
----------------------------------------

Beef Chuck:
  Total Records: 1,250
  Clean: 1,180 | Flagged: 70
  Avg Confidence: 0.847
  Species: {'Beef': 1250}
  Grades: {'Choice': 650, 'Prime': 400, 'Select': 200}
============================================================
✅ All records processed successfully!
============================================================
```

## 🔍 Troubleshooting

### Common Issues

1. **Missing API Key**:
   ```
   Error: OPENAI_API_KEY not found in environment variables
   ```
   Solution: Create `.env` file with your OpenAI API key

2. **No Supported Files**:
   ```
   Error: No supported data files found in data/incoming/
   ```
   Solution: Ensure Excel/CSV files are in `data/incoming/` directory

3. **Missing Required Columns**:
   ```
   Error: File supplier.xlsx is missing required columns: ['category_description']
   ```
   Solution: Verify your files contain the required columns with supported naming conventions

4. **Rate Limit Exceeded**:
   ```
   Warning: Rate limit reached, sleeping for 30.2 seconds
   ```
   Solution: Normal behavior - the pipeline will automatically wait and retry

### Debug Mode

Enable verbose logging for detailed troubleshooting:

```bash
python src/run_pipeline.py --categories "Beef Chuck" --verbose
```

## 🔮 Future Enhancements

- [ ] Support for additional meat categories (Pork, Lamb, etc.)
- [ ] Web-based UI for pipeline management
- [ ] Real-time processing capabilities
- [ ] Integration with inventory management systems
- [ ] Advanced ML models for improved extraction accuracy
- [ ] Batch processing optimization for large datasets

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-category`)
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass (`pytest`)
6. Submit a pull request

## 📞 Support

For questions, issues, or feature requests, please open an issue on the GitHub repository.

---

**Built with ❤️ for the meat processing industry** 