---
name: data-schema-designer
description: Design ecommerce data contracts, tables, events, keys, schema evolution, and field-level ownership for pipeline work.
---

# Data Schema Designer

## When to Use
- Use this skill for schema, contract, table, topic, or event-shape design.
- Use it when downstream ETL, validation, or monitoring needs a stable data
  contract.
- Do not use it for writing transformation logic except to state required
  derivations and field semantics.

## Required Inputs
- `_workspace/00_input/request-summary.md`
- `_workspace/01_pipeline_workplan.md`
- source system descriptions, sample records, or inferred ecommerce entities
- destination constraints such as warehouse, lakehouse, topic, or BI needs

## Workflow
1. Identify business entities, events, and grain.
2. Define canonical datasets, primary keys, natural keys, partition keys, and
   relationships.
3. Specify field names, types, nullability, semantics, ownership, and sensitive
   data handling.
4. Define schema evolution policy, backfill impact, and compatibility rules.
5. Mark assumptions and unresolved questions explicitly.

## Output
Write `_workspace/02_schema_design.md` with these sections:
- Scope and assumptions
- Dataset inventory
- Entity and event contracts
- Field dictionary
- Keys, grain, partitions, and relationships
- Evolution and compatibility policy
- Downstream requirements for ETL, validation, and monitoring
- Open questions

## Validation
- Each dataset has a clear grain and owner.
- Each key field has uniqueness and nullability expectations.
- Field names and semantics are stable enough for validation rules and metrics.
- Sensitive fields have handling notes.
- The design avoids raw internal IDs as the only human-facing label when a
  readable label is needed downstream.
