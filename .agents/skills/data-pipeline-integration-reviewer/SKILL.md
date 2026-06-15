---
name: data-pipeline-integration-reviewer
description: Review data pipeline design artifacts for schema, ETL, validation, and monitoring coherence before final synthesis.
---

# Data Pipeline Integration Reviewer

## When to Use
- Use this skill after schema, ETL, validation, and monitoring artifacts exist.
- Use it when the main risk is boundary mismatch between otherwise plausible
  pipeline design parts.
- Do not use it as a producer for missing artifacts; report missing artifacts
  and the smallest repair path.

## Required Inputs
- `_workspace/00_input/request-summary.md`
- `_workspace/02_schema_design.md`
- `_workspace/03_etl_logic.md`
- `_workspace/04_validation_rules.md`
- `_workspace/05_monitoring_plan.md`

## Workflow
1. Compare schema datasets to ETL outputs.
2. Compare ETL failure modes to validation rules and monitoring signals.
3. Compare validation rule names, severities, and metrics to alert and dashboard
   plans.
4. Check that human-facing labels are readable and that raw IDs are not the only
   operational context.
5. Report pass, fix, or redo status with concrete evidence.

## Output
Write `_workspace/06_integration_review.md` with these sections:
- Review status: `pass`, `fix`, or `redo`
- Boundary checks performed
- Blocking mismatches
- Non-blocking improvements
- Evidence by artifact
- Required revisions
- Residual risk

## Validation
- Read both sides of every boundary being reviewed.
- Cite the artifacts compared for each finding.
- Distinguish confirmed failures from unverified assumptions.
- Prefer actionable repair paths over broad critique.
- Approve only when the final synthesis can trace schema fields through ETL,
  validation, and monitoring.
