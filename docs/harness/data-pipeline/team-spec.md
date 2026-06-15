# Data Pipeline Harness Team Spec

## Goal
Provide a reusable, hierarchical agent team for ecommerce data pipeline design.
The team coordinates schema design, ETL logic, data validation rules, monitoring
configuration, and integration review through deterministic markdown handoffs.

## Architecture
- Primary pattern: Hierarchical Delegation.
- Local pattern: Pipeline, because schema contracts feed ETL, ETL feeds
  validation, and validation feeds monitoring.
- Quality gate: Producer-Reviewer, because the integration reviewer checks
  boundary coherence before final synthesis.

The hierarchy is intentionally shallow: one top-level orchestrator delegates to
domain specialists and then to one integration reviewer. Add another
coordination layer only if a future pipeline has independently managed platform,
analytics, and governance tracks.

## Roles
| Role | Responsibility | Skill | Writes |
| --- | --- | --- | --- |
| Pipeline orchestrator | Scope the request, order phases, resolve conflicts, synthesize final design | `.agents/skills/data-pipeline-orchestrator/SKILL.md` | `_workspace/00_input/request-summary.md`, `_workspace/01_pipeline_workplan.md`, `_workspace/final/data-pipeline-design.md` |
| Schema designer | Define datasets, fields, keys, grain, ownership, and evolution policy | `.agents/skills/data-schema-designer/SKILL.md` | `_workspace/02_schema_design.md` |
| ETL logic designer | Define extraction, transformation, loading, idempotency, and recovery behavior | `.agents/skills/etl-logic-designer/SKILL.md` | `_workspace/03_etl_logic.md` |
| Validation designer | Define data quality rules, thresholds, severity, and remediation policy | `.agents/skills/data-validation-designer/SKILL.md` | `_workspace/04_validation_rules.md` |
| Monitoring designer | Define metrics, dashboards, alerts, SLOs, and runbook hooks | `.agents/skills/pipeline-monitoring-designer/SKILL.md` | `_workspace/05_monitoring_plan.md` |
| Integration reviewer | Check cross-boundary coherence and issue pass, fix, or redo status | `.agents/skills/data-pipeline-integration-reviewer/SKILL.md` | `_workspace/06_integration_review.md` |

## Phase Order

### Phase 0: Intake
- Inputs: user request, repository context, known source or destination docs.
- Actions: summarize scope, assumptions, constraints, and final deliverable.
- Output: `_workspace/00_input/request-summary.md`.
- Completion criteria: all known constraints and open questions are visible.

### Phase 1: Work Plan
- Inputs: request summary and repository context.
- Actions: choose required specialists and any safe parallelism.
- Output: `_workspace/01_pipeline_workplan.md`.
- Completion criteria: phase order and artifact names are fixed.

### Phase 2: Schema Design
- Inputs: request summary and work plan.
- Actions: define datasets, event contracts, keys, field semantics, and evolution.
- Output: `_workspace/02_schema_design.md`.
- Completion criteria: every dataset has grain, owner, key policy, and field dictionary.

### Phase 3: ETL Logic
- Inputs: request summary, work plan, schema design.
- Actions: map sources to targets and define transforms, checkpoints, and recovery.
- Output: `_workspace/03_etl_logic.md`.
- Completion criteria: every target dataset has a producing flow and recovery policy.

### Phase 4: Validation Rules
- Inputs: schema design and ETL logic.
- Actions: define testable data quality rules, severity, thresholds, and remediation.
- Output: `_workspace/04_validation_rules.md`.
- Completion criteria: critical keys, freshness, completeness, and reconciliation
  checks have actionable outcomes.

### Phase 5: Monitoring Plan
- Inputs: ETL logic and validation rules.
- Actions: define metrics, dashboards, alerts, SLOs, and runbook hooks.
- Output: `_workspace/05_monitoring_plan.md`.
- Completion criteria: critical failures and severe validation rules map to
  human-readable observability signals.

### Phase 6: Integration Review
- Inputs: all specialist artifacts.
- Actions: compare schema-to-ETL, ETL-to-validation, and validation-to-monitoring
  boundaries.
- Output: `_workspace/06_integration_review.md`.
- Completion criteria: review status is `pass`, `fix`, or `redo` with evidence.

### Phase 7: Final Synthesis
- Inputs: specialist artifacts and integration review.
- Actions: merge approved design into a concise final artifact.
- Output: `_workspace/final/data-pipeline-design.md`.
- Completion criteria: final design names assumptions, decisions, validation
  coverage, monitoring coverage, and next implementation steps.

## Handoff Files
| From | To | File | Purpose |
| --- | --- | --- | --- |
| Orchestrator | All roles | `_workspace/00_input/request-summary.md` | Shared request snapshot |
| Orchestrator | All roles | `_workspace/01_pipeline_workplan.md` | Phase order and artifact contract |
| Schema designer | ETL designer | `_workspace/02_schema_design.md` | Target contracts and field semantics |
| ETL designer | Validation designer | `_workspace/03_etl_logic.md` | Transform assumptions and failure modes |
| Validation designer | Monitoring designer | `_workspace/04_validation_rules.md` | Rule signals, severities, and metrics |
| All specialists | Integration reviewer | `_workspace/02_schema_design.md` through `_workspace/05_monitoring_plan.md` | Boundary evidence |
| Integration reviewer | Orchestrator | `_workspace/06_integration_review.md` | Approval and required revisions |

## Failure Policy
- If source details are missing but ecommerce defaults are reasonable, continue
  with explicit assumptions and open questions.
- If a target dataset lacks an ETL producer, return to Phase 3.
- If a severe validation rule lacks monitoring coverage, return to Phase 5.
- If integration review returns `fix`, run targeted revisions once before final
  synthesis.
- If integration review returns `redo`, rewrite the failing phase artifact and
  rerun downstream phases that consumed it.

## Artifact Naming Convention
- Use two-digit phase prefixes for durable handoffs.
- Use role names in filenames only when two roles can write in the same phase.
- Keep final user-facing output under `_workspace/final/`.
- Preserve all intermediate artifacts for auditability.

## Validation Checklist
- [ ] Every generated skill starts with YAML frontmatter.
- [ ] Team spec and orchestrator use the same handoff filenames.
- [ ] Schema fields trace to ETL outputs.
- [ ] ETL failure modes trace to validation rules or monitoring signals.
- [ ] Severe validation rules trace to alerts or explicit non-alerting rationale.
- [ ] Human-facing dashboards and alerts use readable labels.
- [ ] Integration review status is resolved before final synthesis.

## Test Scenarios

### Normal Flow
- Request: Design a daily ecommerce orders pipeline from checkout events to an
  analytics warehouse with data quality checks and monitoring.
- Expected outputs: all phase artifacts, integration status `pass`, and final
  design with source-to-target mapping, validation coverage, and alert plan.

### Failure Flow
- Failure point: ETL artifact produces `customer_id` but schema defines
  `buyer_id`, and validation rules reference both.
- Expected behavior: integration review returns `fix`, cites the mismatch, and
  orchestrator revises schema or ETL naming before final synthesis.

## Removable Model-Specific Logic
- Temporary retries are limited to one targeted revision after `fix`.
- Delete this retry cap if future runtime tooling provides deterministic
  structured review and revision control.
