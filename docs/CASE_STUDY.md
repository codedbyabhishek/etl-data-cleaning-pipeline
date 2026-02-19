# Case Study: ETL & Data Cleaning Pipeline

## Business Problem
Raw operational data from different systems had duplication, missing values, and inconsistent schema quality.

## Objective
Build a production-style ETL workflow with strong data-quality controls and repeatable reporting outputs.

## Data Scope
- 120K+ order records
- Multi-source ingestion: raw CSV + simulated DB extract + customer master
- Transformations: imputation, standardization, validation, enrichment, bucketing

## Solution
- Built modular extract-transform-load pipeline.
- Added data-quality scoring (completeness, uniqueness, validity).
- Introduced audit-style reporting for pipeline observability.
- Automated KPI reports by status, tier, and monthly trend.

## Key Outcomes
- Consistent curated dataset for analytics consumers.
- Reduced manual cleanup and reporting effort.
- Clear quality transparency for data stakeholders.
