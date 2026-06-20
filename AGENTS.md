# Repository Agents Guide

Keep this file short and repo-wide. Put workflow-specific depth in repo-local
skills and `docs/harness/`.

## What
- This repository stores portable agent harnesses for ecommerce data work.
- Canonical harness files live under `.agents/skills/` and `docs/harness/`.
- Intermediate handoffs should be written under `_workspace/` when a harness is run.

## Why
- Data pipeline design has boundary-heavy work: schemas, ETL logic, validation
  rules, and monitoring must agree with each other.
- File-based handoffs make those boundaries reviewable and reusable across
  future runs.

## How
- Use `.agents/skills/harness/SKILL.md` when creating or revising harnesses.
- Use `.agents/skills/data-pipeline-orchestrator/SKILL.md` for hierarchical
  ecommerce data pipeline design.
- Keep generated skills lean and move detailed, conditional guidance into
  `docs/harness/` or skill `references/` files.
- No project-wide build command is defined yet. Validate harness changes with
  path checks and by reading generated `SKILL.md` frontmatter.

## Work Log
- Record meaningful project work in `docs/project-log.md` after completing it.
- Log entries should include the user request, work performed, files changed,
  verification, and next step when relevant.
- Do not log prompts that only ask for status, confirmation, explanation, or a
  plan when no repository files, commands, configuration, or implementation work
  changed.
