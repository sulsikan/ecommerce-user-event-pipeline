---
name: data-validation-designer
description: Design data quality rules, validation tests, severity policy, and quarantine or remediation paths for ecommerce pipelines.
---

# Data Validation Designer

## When to Use
- Use this skill for data quality rules, tests, assertions, anomaly checks, and
  remediation policy.
- Use it after schema and ETL artifacts exist.
- Do not use it to define dashboard layout or alert routing except to expose
  validation signals that monitoring can consume.

## Required Inputs
- `_workspace/00_input/request-summary.md`
- `_workspace/02_schema_design.md`
- `_workspace/03_etl_logic.md`
- known business rules, SLAs, or failure tolerance if available

## Workflow
1. Convert schema expectations into testable rules.
2. Convert ETL assumptions into freshness, completeness, and reconciliation
   checks.
3. Assign severity, owner, threshold, and action for each rule.
4. Define quarantine, rollback, reprocessing, and exception handling paths.
5. Name validation metrics for monitoring consumption.

## Output
Write `_workspace/04_validation_rules.md` with these sections:
- Scope and assumptions
- Rule inventory
- Rule definitions with severity, threshold, owner, and action
- Freshness, completeness, validity, uniqueness, and reconciliation checks
- Quarantine and remediation policy
- Metrics exposed to monitoring
- Test data or fixture needs
- Open questions

## Validation
- Every critical schema key has null and uniqueness coverage.
- Every required ETL dependency has freshness or availability coverage.
- Rule names are stable and human-readable enough for dashboards and alerts.
- Each severe rule has an action, not only a detection query.
- Exceptions are documented so they do not silently weaken quality gates.
