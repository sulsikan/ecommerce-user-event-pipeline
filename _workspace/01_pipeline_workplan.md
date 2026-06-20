# Pipeline Workplan

## Architecture Pattern
- Top-level pattern: hierarchical delegation.
- Execution pattern: sequential pipeline with integration review.
- Runtime architecture: CSV replay producer -> Kafka -> Spark Structured
  Streaming -> storage and metrics -> Grafana.

## Specialist Outputs
| Phase | Specialist | Output |
| --- | --- | --- |
| 2 | Schema designer | `_workspace/02_schema_design.md` |
| 3 | ETL logic designer | `_workspace/03_etl_logic.md` |
| 4 | Validation designer | `_workspace/04_validation_rules.md` |
| 5 | Monitoring designer | `_workspace/05_monitoring_plan.md` |
| 6 | Integration reviewer | `_workspace/06_integration_review.md` |
| 7 | Orchestrator | `_workspace/final/data-pipeline-design.md` |

## Proposed Component Stack
| Layer | Component | Reason |
| --- | --- | --- |
| Replay | Python or JVM CSV replay producer | Converts historical CSV into controlled Kafka events. |
| Broker | Kafka with 3 core topics | Required streaming backbone and replay buffer. |
| Processing | Spark Structured Streaming | Required streaming computation and stateful aggregations. |
| Durable lake | Delta/Parquet on local disk or MinIO | Keeps raw and validated event history replayable. |
| MVP metrics store | Prometheus | Stores pipeline health plus pre-aggregated business metrics for Grafana. |
| Optional OLAP store | ClickHouse | Added after MVP to compare SQL OLAP serving performance. |
| Dashboard | Grafana | Required dashboard UI for business and operational views. |

## Delivery Phases
| Phase | Runtime Path | Goal |
| --- | --- | --- |
| 1 | Kafka -> Spark Streaming -> Prometheus -> Grafana | Build the simplest real-time dashboard and measure baseline throughput, latency, and dashboard freshness. |
| 2 | Kafka -> Spark Streaming -> ClickHouse + Prometheus -> Grafana | Add OLAP serving for richer category/brand/time-window analysis and compare against the baseline. |

## Main Deliverables
- Project architecture with component responsibilities.
- Kafka topic design.
- Canonical event schema and aggregate datasets.
- Spark streaming flow and checkpoint strategy.
- Data quality and quarantine rules.
- Grafana dashboard and monitoring plan.
- Performance comparison plan for Prometheus-only versus ClickHouse-backed dashboards.
- Integration review confirming traceability from source CSV to dashboard.

## Non-Goals for This Pass
- Writing application code.
- Choosing exact Docker image versions.
- Producing complete Grafana dashboard JSON.
- Tuning Spark executor sizing beyond initial architecture-level guidance.

## Acceptance Criteria
- The design uses Kafka, Spark Streaming, and Grafana directly.
- Phase 1 dashboard metrics trace back to Spark outputs exported to Prometheus.
- Phase 2 dashboard metrics trace back to Spark outputs written to ClickHouse.
- Severe data quality failures have monitoring signals.
- The design explains how a static CSV becomes a real-time stream.
