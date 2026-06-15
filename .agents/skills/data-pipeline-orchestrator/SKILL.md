---
name: data-pipeline-orchestrator
description: Coordinate hierarchical ecommerce data pipeline design across schema, ETL, validation, monitoring, and integration review specialists.
---

# Data Pipeline Orchestrator

## When to Use
- Use this skill when a request asks for a data pipeline design or redesign.
- Use it when schema design, ETL logic, data validation, and monitoring must be
  coordinated as a connected system.
- Do not use it for a single isolated SQL query, one-off metric explanation, or
  local bug fix unless the request also needs reusable pipeline design artifacts.

## Required Inputs
- business goal, entities, events, or source systems
- expected destinations such as warehouse, lakehouse, streaming topic, or BI layer
- freshness, latency, volume, retention, and compliance constraints if known
- preferred orchestration, storage, or monitoring tools if the repository defines them
- final deliverable format and audience if the user specifies one

If inputs are missing, inspect the repository first and make the narrowest
reasonable ecommerce-data assumption. Record assumptions in the intake artifact.

## Delegation Hierarchy
| Layer | Role | Skill | Owns |
| --- | --- | --- | --- |
| Top | Pipeline orchestrator | this skill | request framing, phase order, final synthesis |
| Domain | Schema designer | `.agents/skills/data-schema-designer/SKILL.md` | contracts, keys, schema evolution |
| Domain | ETL logic designer | `.agents/skills/etl-logic-designer/SKILL.md` | extraction, transforms, loads, recovery |
| Domain | Validation designer | `.agents/skills/data-validation-designer/SKILL.md` | rules, severity, testable quality gates |
| Domain | Monitoring designer | `.agents/skills/pipeline-monitoring-designer/SKILL.md` | metrics, alerts, dashboards, runbooks |
| Review | Integration reviewer | `.agents/skills/data-pipeline-integration-reviewer/SKILL.md` | cross-boundary coherence and final quality gate |

## Workflow
1. Intake and scope
   - Write `_workspace/00_input/request-summary.md`.
   - Capture goals, assumptions, constraints, target audience, and unresolved questions.
2. Work plan
   - Write `_workspace/01_pipeline_workplan.md`.
   - Define the expected final artifact and which specialists must run.
3. Schema design
   - Delegate to the schema designer.
   - Required output: `_workspace/02_schema_design.md`.
4. ETL design
   - Delegate to the ETL logic designer after the schema artifact exists.
   - Required output: `_workspace/03_etl_logic.md`.
5. Validation design
   - Delegate to the validation designer after schema and ETL artifacts exist.
   - Required output: `_workspace/04_validation_rules.md`.
6. Monitoring design
   - Delegate to the monitoring designer after ETL and validation artifacts exist.
   - Required output: `_workspace/05_monitoring_plan.md`.
7. Integration review
   - Delegate to the integration reviewer.
   - Required output: `_workspace/06_integration_review.md`.
8. Final synthesis
   - Write `_workspace/final/data-pipeline-design.md`.
   - Include decisions, diagrams or tables when useful, open questions, and next
     implementation steps.

## Handoff Rules
- Every phase must read the original request summary and the immediately prior
  relevant artifacts.
- Specialists must list assumptions and unresolved questions instead of hiding
  them in prose.
- Downstream specialists may request upstream revisions, but the orchestrator
  owns whether to revise or document the tradeoff.
- Preserve all `_workspace/` files for auditability.

## Failure Policy
- Missing source details: continue with explicit assumptions if the design can
  remain useful; otherwise stop with the exact missing input.
- Schema and ETL mismatch: revise the schema artifact before writing validation
  rules.
- Validation and monitoring mismatch: revise metrics or rule names so alerts can
  point to actionable quality failures.
- Conflicting specialist outputs: the integration reviewer records the conflict,
  then the orchestrator chooses one path or leaves a decision log entry.

## Validation
- Confirm each generated `SKILL.md` has YAML frontmatter with `name` and
  `description`.
- Confirm the team spec and all skills use the same `_workspace/` artifact names.
- Confirm the final design traces schema fields to ETL steps, validation rules,
  and monitoring signals.
- Confirm alert and dashboard labels are human-readable, not only internal IDs.

## References
- Team topology: `docs/harness/data-pipeline/team-spec.md`
