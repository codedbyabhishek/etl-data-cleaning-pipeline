# ETL & Data Cleaning Pipeline

ETL and data quality pipeline project (Dec 2025 - Feb 2026).

## Highlights
- Designed ETL flow for CSV + database-style records.
- Implemented cleaning rules for missing values, schema normalization, and duplicate handling.
- Enforced data integrity checks before reporting.
- Automated routine KPI reporting to reduce manual effort.

## Quickstart
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/generate_raw.py
python src/etl_pipeline.py
python src/reporting.py
```

## Pipeline Stages
- Extract: load raw customer/order files
- Transform: standardize types, impute missing, remove invalid records
- Load: write curated datasets and quality report

## Outputs
- `data/staging/staged_orders.csv`
- `data/curated/curated_orders.csv`
- `reports/data_quality_report.csv`
- `reports/automated_kpi_report.csv`
