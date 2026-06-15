---
name: etl-logic-designer
description: Design ecommerce ETL and ELT logic, orchestration steps, transformation contracts, idempotency, and recovery behavior.
---

# ETL Logic Designer

## When to Use
- Use this skill for extraction, transformation, load, orchestration, and retry
  design.
- Use it after schema contracts exist or when the request is specifically about
  pipeline logic.
- Do not use it as the final quality gate; validation and monitoring remain
  separate specialist responsibilities.

## Required Inputs
- `_workspace/00_input/request-summary.md`
- `_workspace/01_pipeline_workplan.md`
- `_workspace/02_schema_design.md`
- source freshness, volume, and destination constraints if available

## Workflow
1. Map each source dataset or event to the canonical schema.
2. Define extraction mode: batch, incremental, CDC, streaming, or hybrid.
3. Define transformation steps, joins, enrichments, deduplication, and late data
   handling.
4. Define load strategy, partitions, checkpoints, idempotency, and rollback.
5. Define operational failure modes and recovery behavior.

## Output
Write `_workspace/03_etl_logic.md` with these sections:
- Scope and assumptions
- Source-to-target mapping
- Phase-by-phase ETL flow
- Transformation rules
- Incremental processing and checkpointing
- Idempotency, retry, and recovery policy
- Performance and scaling notes
- Dependencies for validation and monitoring
- Open questions

## Validation
- Every target dataset in the schema artifact has a producing ETL step.
- Every derived field has a named source or transformation rule.
- Incremental logic handles duplicates, missing records, late arrivals, and
  replays.
- Recovery policy distinguishes retryable failures from data-quality failures.
- Monitoring dependencies are named while the exact dashboard design remains
  owned by the monitoring specialist.
