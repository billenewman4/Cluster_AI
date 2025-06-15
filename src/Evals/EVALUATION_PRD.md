# Product Requirements Document (PRD)

## 1. Purpose
Provide an automated, repeatable evaluation framework that measures the accuracy of our model's predictions for two critical fields extracted from beef product descriptions:
1. **Sub-primal** (e.g., "Ribeye", "Striploin")
2. **Grade** (e.g., "Choice", "Select", "Prime")

The framework will compare model outputs against curated ground-truth labels stored in `src/Evals/` and surface actionable metrics to drive continuous model improvement.

---

## 2. Background & Context
• The current model ingests unstructured product descriptions and predicts structured attributes (sub-primal, grade, etc.).  
• `src/Evals/meat_inventory_master_*.xlsx` (and future parquet/CSV equivalents) contains the authoritative ground-truth labels compiled by SMEs.  
• No reproducible evaluation pipeline exists; ad-hoc tests hamper regression tracking and A/B analysis.

---

## 3. Goals & Success Criteria
1. **Automated Evaluation CLI**
   • Single command evaluates a model checkpoint or API endpoint over the full validation set.
2. **Robust Metrics**
   • Precision, recall, F1 for each label set.  
   • Confusion matrices and per-class breakdowns.
3. **Versioned Reports**
   • Results saved with timestamp + model hash; diff against previous runs.
4. **CI Integration**
   • GitHub Actions job fails if F1 drops > configurable threshold.
5. **Developer Ergonomics**
   • <60 s local setup; clear README.

---

## 4. User Stories
1. **ML Engineer** wants to run `python -m evals.run --model my_model.ckpt` and receive a markdown/html report.
2. **Reviewer** wants a dashboard summarising historical performance trends.
3. **PM** wants notification when a PR degrades grade prediction by >2 pts.

---

## 5. Functional Requirements
FR-1  Data Loader ingests ground-truth file(s) in Excel/CSV/Parquet.  
FR-2  Prediction Loader calls local model class OR REST endpoint (configurable).  
FR-3  Evaluator leverages **LangChain's evaluation modules** (e.g. `StringEvaluator`, `CriteriaEvaluator`) to compute: accuracy, precision, recall, F1 (macro & micro), and generate confusion matrices.  
FR-4  Output Writer saves:
• `results/{run_id}/metrics.json`  
• `results/{run_id}/confusion_subprimal.png`, `…grade.png`  
• `results/{run_id}/report.md`
FR-5  CLI accepts flags: `--model`, `--endpoint`, `--limit`, `--output-dir`, `--baseline-run`.
FR-6  CI workflow caches dataset & posts comment on PR with diff table.
FR-7  Framework extensible to additional attributes (cut size, packaging) via config YAML.

---

## 6. MVP Implementation Guide (step-by-step)
1. **Dependencies** [Done]
   ```bash
   pip install langchain pandas scikit-learn seaborn matplotlib
   ```
2. **Ground-truth prep**  [Done]
   • Convert `meat_inventory_master_*.xlsx` → `ground_truth.parquet` via a short script in `data_pipeline/reader.py` (one-off).
3. **Model Interface**  
   • Re-use existing `DynamicBeefExtractor` (in `src/AIs/llm_extraction/specific_extractors`) for single-row inference and `BatchProcessor` (in `src/AIs/llm_extraction/batch_processor.py`) for bulk runs. No remote mode—everything stays local.  
   • Steps:
     1. Instantiate `ReferenceDataLoader` once → pass to `DynamicBeefExtractor(primal="All")`.
     2. For evaluation, feed the full `product_description` column either:  
        – directly to `extractor.extract()` inside a simple loop **or**  
        – via `BatchProcessor({"beef": extractor}).process_batch(df, category="beef")` for parallelism & caching.
     3. Collect `subprimal` and `grade` fields from each `ExtractionResult` into a new DataFrame `pred_df` aligned 1-to-1 with ground truth rows.
4. **Evaluation Script**  
   • In `evals/run.py` load GT + predictions.  
   • Instantiate LangChain evaluators:  
     ```python
     from langchain.evaluation import StringEvaluator
     sub_eval = StringEvaluator(metric="f1")
     grade_eval = StringEvaluator(metric="f1")
     ```
   • Pass GT vs prediction lists → receive metric scores.
5. **Reporting**  
   • Dump `metrics.json` and render a simple markdown report (`reporting.py`) showing overall F1 and per-class breakdown tables (using `sklearn.metrics.classification_report`).
6. **CLI Wrapper**  
   • Create `cli.py` exposing `python -m evals.run --model <path|url>` that orchestrates the above modules.
7. **(Optional) CI Check**  
   • Add lightweight GitHub Action calling the CLI on a 200-row sample dataset; fail if macro-F1 drops >1 pt.  

This sequence keeps the first release lean—no dashboards or database storage until metrics prove valuable.

---

## 7. Data Specification
| Column | Type | Description |
|--------|------|-------------|
| `product_description` | string | Raw input text |
| `sub_primal_gt` | string | Ground-truth sub-primal |
| `grade_gt` | string | Ground-truth grade |

Dataset resides in `src/Evals/ground_truth.parquet` (source Excel converted during ingestion).

---

## 8. Architecture & Components
1. **`data_pipeline/reader.py`**: loads and normalises dataset.  
2. **`model_interface.py`**: wrapper exposing `predict(batch: List[str]) -> List[Dict]` for local/remote.  
3. **`metrics.py`**: sklearn-based computations.  
4. **`reporting.py`**: renders markdown & plots via seaborn/matplotlib.  
5. **`cli.py`**: Argparse entrypoint tying it together.

---

## 9. Acceptance Criteria
• Running `python -m evals.run --model dummy` on sample dataset produces report with ≥90 % macro-F1 (dummy uses ground-truth).  
• Documentation in `README.md` explains installation, CLI usage, extending.

---

## 10. Milestones & Timeline
| Date | Milestone |
|------|-----------|
| +0 d | Approve PRD |
| +3 d | Data loader + model interface POC |
| +6 d | Metric & report generation |
| +8 d | CLI + local docs |
| +10 d | GitHub Actions integration |
| +12 d | First evaluation baseline committed |

---

## 11. Risks & Mitigations
• **Label drift**: schedule weekly sanity script validating new GT rows.  
• **Model API latency**: allow async batching; timeout handling.  
• **Excel format changes**: enforce parquet source-of-truth checked into repo.

---

## 12. Open Questions
1. What is the minimum acceptable macro-F1 for CI gating?  
2. Should we store per-run prediction CSVs (adds weight to repo)?  
3. Preferred dashboard stack (Streamlit vs Superset)? 