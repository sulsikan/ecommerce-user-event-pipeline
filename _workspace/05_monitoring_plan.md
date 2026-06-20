# Monitoring Plan

## Scope and Assumptions
- Grafana is the dashboard UI.
- Phase 1 uses Prometheus as the only Grafana datasource for both operational
  metrics and pre-aggregated business metrics.
- Phase 2 adds ClickHouse as an OLAP serving layer for richer SQL-backed
  category, brand, and time-window analysis.
- Dashboard labels should be readable without external lookup.

## Metrics Inventory

### Phase 1 Business Metrics in Prometheus
Prometheus metrics should avoid high-cardinality labels. Do not label metrics by
`product_id`, `user_id`, or `user_session`. Use low-cardinality dimensions and
pre-computed top-N series where category or brand drilldown is needed.

| Metric | Dimensions | Purpose |
| --- | --- | --- |
| Events per minute | `event_type` | Live traffic volume. |
| Active users per minute | minute window | Audience activity. |
| Active sessions per minute | minute window | Session activity. |
| View/cart/purchase funnel | `category_label`, 5m window | Conversion monitoring. |
| Purchase count | bounded `category_label`, bounded `brand_label` | Sales activity. |
| Revenue | bounded `category_label`, bounded `brand_label` | Revenue trend. |
| Average purchase price | bounded `category_label`, bounded `brand_label` | Basket/product signal. |

### Phase 2 Business Tables in ClickHouse
| Table | Dimensions | Purpose |
| --- | --- | --- |
| `event_metrics_1m` | `event_type`, minute window | SQL-backed traffic analysis. |
| `funnel_metrics_5m` | `category_label`, 5m window | Category-level funnel analysis. |
| `revenue_metrics_1m` | `category_label`, `brand_label`, minute window | Revenue and top-N analysis. |

### Operational Metrics in Prometheus
| Metric | Labels | Purpose |
| --- | --- | --- |
| `producer_events_sent_total` | `source_file`, `topic` | CSV replay progress. |
| `producer_replay_lag_seconds` | `source_file`, `topic` | Replay delay. |
| `kafka_consumer_lag` | `group`, `topic`, `partition` | Spark backlog. |
| `spark_batch_duration_seconds` | `query` | Processing latency. |
| `spark_input_rows_per_second` | `query` | Ingestion rate. |
| `spark_processed_rows_per_second` | `query` | Processing rate. |
| `spark_query_failures_total` | `query` | Job stability. |
| `aggregate_freshness_seconds` | `aggregate_name` | Dashboard freshness. |
| `clickhouse_write_latency_seconds` | `table` | Phase 2 sink health. |

### Data Quality Metrics in Prometheus
| Metric | Labels | Purpose |
| --- | --- | --- |
| `dq_rule_failures_total` | `rule_id`, `rule_name`, `severity` | Rule failure volume. |
| `dq_rule_failure_rate` | `rule_id`, `rule_name`, `severity` | Alert threshold input. |
| `stream_records_quarantined_total` | `rule_id`, `rule_name` | Quarantine volume. |

## Dashboard Plan

### Grafana Dashboard 1: Live Ecommerce Overview
- Events per minute by `event_type`.
- Purchases per minute.
- Revenue per minute.
- Active users and sessions.
- Top categories by purchases.
- Top brands by revenue.

### Grafana Dashboard 2: Funnel and Category Performance
- View to cart to purchase funnel by `category_label`.
- Conversion rate by category.
- Revenue by category and brand.
- Unknown category and unknown brand rates.

### Grafana Dashboard 3: Streaming Pipeline Health
- Kafka consumer lag by partition.
- Spark micro-batch duration.
- Spark input and processed rows per second.
- Producer replay progress.
- Aggregate freshness.
- Prometheus scrape freshness.
- ClickHouse write latency in Phase 2.

### Grafana Dashboard 4: Data Quality
- Rule failures by readable `rule_name`.
- Quarantined records over time.
- Critical versus warning failures.
- Duplicate event rate.
- Unknown category and brand rates.

## Alert and SLO Policy
| Alert | Severity | Condition | First Action |
| --- | --- | --- | --- |
| Kafka lag high | high | consumer lag grows for 5m | check Spark query health and Kafka partitions |
| Spark query stopped | critical | query failure count increases | restart Spark app from checkpoint |
| Aggregate stale | critical | freshness > 5m during replay | check Spark metrics export in Phase 1 or Spark-to-ClickHouse sink in Phase 2 |
| Critical DQ failures | high | any critical rule spikes for 5m | inspect quarantine records by rule name |
| Unknown category rate high | warning | unknown category rate > 20% for 10m | inspect source category fields |
| Producer stalled | high | no sent events for 2m during active replay | check producer checkpoint and Kafka availability |

## Log and Trace Requirements
- Producer logs current source line, events sent, and publish errors.
- Spark logs query name, batch ID, input rows, processed rows, and sink result.
- Quarantine logs include source line and rule ID.
- Grafana panels link to runbook sections and quarantine queries where useful.

## Runbook and Ownership Map
| Surface | Owner | Runbook Note |
| --- | --- | --- |
| CSV replay producer | data engineering | resume from producer checkpoint |
| Kafka topics | platform/data engineering | inspect partitions, retention, and lag |
| Spark streaming app | data engineering | restart from checkpoint, inspect failed query |
| Prometheus metrics path | data engineering | check exporter health, scrape status, and metric freshness |
| ClickHouse aggregate store | data engineering | Phase 2 only; check write latency and table health |
| Grafana dashboards | analytics/data engineering | verify datasource and panel queries |
| Validation rules | analytics/data engineering | inspect quarantine by `rule_name` |

## Validation Signals Consumed
- `dq_rule_failures_total`
- `dq_rule_failure_rate`
- `stream_records_quarantined_total`
- `aggregate_freshness_seconds`
- purchase reconciliation between event counts and revenue aggregates

## Open Questions
- Whether alerts should go to Slack, email, or only Grafana annotations.
- Whether dashboard users need row-level drilldown from aggregates to events.
- Whether Prometheus metrics should be pushed by Spark or scraped from exporters.
- Whether Phase 2 ClickHouse improves dashboard latency enough to justify the
  extra component.
