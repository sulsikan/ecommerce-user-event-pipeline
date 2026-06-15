---
name: pipeline-monitoring-designer
description: Design pipeline observability with metrics, alerts, dashboards, logs, SLOs, and runbook hooks for ecommerce data workflows.
---

# Pipeline Monitoring Designer

## When to Use
- Use this skill for monitoring, alerting, dashboard, SLO, and runbook design.
- Use it after ETL and validation artifacts exist.
- Do not use it to invent data quality rules; consume validation signals and
  make them actionable.

## Required Inputs
- `_workspace/00_input/request-summary.md`
- `_workspace/03_etl_logic.md`
- `_workspace/04_validation_rules.md`
- available monitoring stack or preferred tools if known

## Workflow
1. Identify operational signals: job status, duration, throughput, lag, retries,
   failures, and resource pressure.
2. Identify data signals: freshness, completeness, validation failures,
   reconciliation gaps, and business KPI drift.
3. Define dashboards for operator scanability and stakeholder visibility.
4. Define alert thresholds, severity, routing, and suppression rules.
5. Define runbook links and first-response actions for each critical alert.

## Output
Write `_workspace/05_monitoring_plan.md` with these sections:
- Scope and assumptions
- Metrics inventory
- Dashboard plan
- Alert and SLO policy
- Log and trace requirements
- Runbook and ownership map
- Validation signals consumed
- Open questions

## Validation
- Every critical ETL failure mode has at least one observable signal.
- Every severe validation rule maps to a metric and alert or an explicit
  non-alerting rationale.
- Dashboard labels are human-readable and useful without looking up raw codes.
- Alerts include owner, impact, first action, and escalation path.
- Monitoring distinguishes system failures from data-quality failures.
