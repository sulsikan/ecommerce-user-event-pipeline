# Real-Time Ecommerce Streaming Dashboard Architecture

## Summary
Use the Kaggle `data/2019-Oct.csv` file as a historical event source and replay
it into Kafka to simulate a live ecommerce event stream. Spark Structured
Streaming reads Kafka, validates and enriches events, and writes durable event
history to Delta/Parquet. Phase 1 exports pre-aggregated business, operational,
and data-quality metrics to Prometheus so Grafana can run with the smallest
serving stack. Phase 2 adds ClickHouse as an OLAP serving layer for richer
SQL-backed category, brand, and time-window analysis, then compares its
performance against the Prometheus-only baseline.

## Observed Source Data
- File: `data/2019-Oct.csv`
- Size: 5.3 GB
- Rows: 42,448,765 including header
- Time range: `2019-10-01 00:00:00 UTC` to `2019-10-31 23:59:59 UTC`
- Event distribution:
  - `view`: 40,779,399
  - `cart`: 926,516
  - `purchase`: 742,849
- Columns:
  - `event_time`, `event_type`, `product_id`, `category_id`,
    `category_code`, `brand`, `price`, `user_id`, `user_session`

## Proposed Architecture

### Phase 1: Prometheus-Only MVP

```text
data/2019-Oct.csv
  |
  v
CSV replay producer
  |
  v
Kafka topic: ecommerce.events.raw.v1
  |
  v
Spark Structured Streaming
  |-- bronze raw Delta/Parquet
  |-- silver validated events Delta/Parquet
  |-- invalid events quarantine + Kafka DLQ
  |-- business, pipeline, and quality metrics to Prometheus
  |
  v
Grafana
  |-- Prometheus datasource for business, pipeline, and quality metrics
```


### Phase 2: ClickHouse OLAP Extension

```text
data/2019-Oct.csv
  |
  v
CSV replay producer
  |
  v
Kafka topic: ecommerce.events.raw.v1
  |
  v
Spark Structured Streaming
  |-- bronze raw Delta/Parquet
  |-- silver validated events Delta/Parquet
  |-- invalid events quarantine + Kafka DLQ
  |-- business aggregates to ClickHouse
  |-- pipeline and quality metrics to Prometheus
  |
  v
Grafana
  |-- ClickHouse datasource for business analytics
  |-- Prometheus datasource for pipeline and quality metrics
```

## Component Responsibilities
| Component | Responsibility |
| --- | --- |
| CSV replay producer | Reads the 2019-Oct CSV and publishes events to Kafka at a controlled rate. |
| Kafka | Buffers raw events, decouples ingestion from processing, supports replay from offsets. |
| Spark Structured Streaming | Parses, validates, enriches, aggregates, and writes streaming outputs. |
| Delta/Parquet storage | Stores bronze raw events and silver validated events for audit and replay. |
| Prometheus | Phase 1 serving layer for pre-aggregated business, operational, and data quality metrics. |
| ClickHouse | Phase 2 OLAP serving layer for richer SQL-backed aggregate queries in Grafana. |
| Grafana | Displays live ecommerce, funnel, revenue, pipeline health, and data quality dashboards. |

## Kafka Design
| Topic | Producer | Consumer | Purpose |
| --- | --- | --- | --- |
| `ecommerce.events.raw.v1` | CSV replay producer | Spark bronze/silver streams | Raw ecommerce event stream. |
| `ecommerce.events.invalid.v1` | Spark validation | optional debug consumer | Invalid event DLQ. |
| `ecommerce.pipeline.audit.v1` | producer and Spark | optional debug consumer | Replay checkpoints, batch summaries, and audit events. |

Recommended raw topic settings:
- Partitions: 6 to 12 for local development.
- Message key: `user_session` if present, else `user_id`.
- Value format: JSON for first implementation; Avro/Protobuf can be added later
  if schema registry becomes useful.
- Retention: at least long enough to replay failed Spark windows during demos.

## Spark Streaming Design
| Stream | Input | Output | Notes |
| --- | --- | --- | --- |
| Bronze stream | Kafka raw topic | raw Delta/Parquet | Preserve raw payload and Kafka metadata. |
| Silver stream | Kafka raw topic or bronze | validated events and invalid quarantine | Parse, normalize, validate, derive labels. |
| Traffic aggregation | validated events | Phase 1 Prometheus metrics; Phase 2 ClickHouse `event_metrics_1m` | Event counts by minute and type. |
| Funnel aggregation | validated events | Phase 1 Prometheus metrics; Phase 2 ClickHouse `funnel_metrics_5m` | View/cart/purchase counts by category. |
| Revenue aggregation | purchase events | Phase 1 Prometheus metrics; Phase 2 ClickHouse `revenue_metrics_1m` | Purchases and revenue by category/brand. |
| Quality metrics | validation results | Prometheus | Rule failures by readable rule name. |

Use event-time watermarks because the source has an original `event_time`.
During replay, ingestion time and event time are intentionally different.

Phase 1 Prometheus metrics must keep label cardinality controlled. Do not use
`product_id`, `user_id`, or `user_session` as metric labels. Use low-cardinality
labels such as `event_type`, `category_l1`, `category_label` after top-N
filtering, `rule_name`, and `severity`. Phase 2 ClickHouse is where richer
brand/category drilldown and SQL exploration should live.

## Canonical Event Model
Required fields:
- `event_id`
- `event_time`
- `ingest_time`
- `event_type`
- `product_id`
- `category_id`
- `category_code`
- `category_l1`
- `category_l2`
- `category_l3`
- `category_label`
- `brand`
- `brand_label`
- `price`
- `user_id`
- `user_session`
- `source_file`
- `source_line`
- Kafka topic, partition, and offset

Important design choice: use `category_label` and `brand_label` for dashboards.
When source values are missing, use `unknown` so Grafana panels remain readable.

## Data Quality Rules
Critical rules quarantine records:
- `event_time` must parse as UTC.
- `event_type` must be one of `view`, `cart`, `purchase`.
- `product_id`, `user_id`, and `user_session` must be present.
- `price` must be numeric and non-negative.

Warning/info rules keep records but emit metrics:
- unknown category label rate
- unknown brand label rate
- duplicate event ID rate
- replay or processing lag

Validation metrics should include both machine-readable IDs and readable labels:
- `rule_id`
- `rule_name`
- `severity`

## Grafana Dashboard Design

### Live Ecommerce Overview
- Events per minute by event type.
- Purchases per minute.
- Revenue per minute.
- Active users per minute.
- Active sessions per minute.
- Top categories by purchase count.
- Top brands by revenue.

### Funnel and Category Performance
- View to cart to purchase funnel by category.
- Cart conversion rate.
- Purchase conversion rate.
- Revenue by category and brand.
- Unknown category and brand rates.

### Streaming Pipeline Health
- Kafka consumer lag.
- Spark batch duration.
- Spark input rows per second.
- Spark processed rows per second.
- Producer replay progress.
- Aggregate freshness.
- Prometheus scrape freshness.
- ClickHouse write latency in Phase 2.

### Data Quality
- Rule failures by readable rule name.
- Quarantined records over time.
- Critical versus warning failures.
- Duplicate event rate.
- Unknown category and brand rates.

## Storage and Query Layout
| Storage | Data | Use |
| --- | --- | --- |
| `storage/bronze/events` | raw Kafka payload and metadata | audit and replay |
| `storage/silver/events` | canonical valid events | debugging and backfill |
| `storage/quarantine/events` | invalid records with rule evidence | data quality investigation |
| Prometheus | Phase 1 business aggregates, runtime metrics, and quality metrics | Grafana MVP dashboards |
| ClickHouse `event_metrics_1m` | Phase 2 traffic aggregates | Grafana overview and OLAP comparison |
| ClickHouse `funnel_metrics_5m` | Phase 2 funnel aggregates | Grafana conversion panels and OLAP comparison |
| ClickHouse `revenue_metrics_1m` | Phase 2 revenue aggregates | Grafana revenue panels and OLAP comparison |

## Initial Project Structure

```text
.
|-- data/
|   `-- 2019-Oct.csv
|-- docker-compose.yml
|-- configs/
|   |-- kafka/
|   |-- spark/
|   |-- grafana/
|   |   |-- dashboards/
|   |   `-- datasources/
|   |-- prometheus/
|   `-- clickhouse/        # Phase 2
|-- src/
|   |-- producer/
|   |   `-- csv_replay_producer.py
|   |-- streaming/
|   |   |-- bronze_stream.py
|   |   |-- silver_stream.py
|   |   `-- aggregates_stream.py
|   |-- quality/
|   |   `-- rules.py
|   `-- common/
|       `-- schema.py
|-- storage/
|   |-- bronze/
|   |-- silver/
|   |-- quarantine/
|   `-- checkpoints/
|-- tests/
|   |-- fixtures/
|   `-- test_quality_rules.py
`-- docs/
    `-- architecture.md
```

## Implementation Order
### Phase 1: Prometheus-Only MVP
1. Add Docker Compose for Kafka, Spark, Prometheus, and Grafana.
2. Implement CSV replay producer with checkpointed source-line progress.
3. Create Kafka topics and publish a small fixture stream.
4. Implement Spark schema parsing and bronze storage.
5. Implement validation and quarantine routing.
6. Implement silver event enrichment and label derivation.
7. Export Spark business aggregates and quality metrics to Prometheus.
8. Configure Prometheus scrape targets and Grafana Prometheus datasource.
9. Build MVP Grafana dashboards from Prometheus metrics.
10. Add fixture-based tests and a smoke test that replays a small CSV slice.

### Phase 2: ClickHouse OLAP Extension
1. Add ClickHouse to Docker Compose.
2. Create ClickHouse aggregate tables for event, funnel, and revenue metrics.
3. Add Spark sinks that write the same aggregate outputs to ClickHouse.
4. Add Grafana ClickHouse datasource and equivalent SQL-backed panels.
5. Replay the same benchmark workload used in Phase 1.
6. Compare dashboard latency, query flexibility, write cost, resource usage, and operational complexity.

## Acceptance Checks
- Producer can replay a small CSV fixture into Kafka.
- Spark can parse and validate all fixture event types.
- Invalid fixture rows land in quarantine with rule names.
- Phase 1 Prometheus metrics update while replay is active.
- Grafana shows business, Kafka, Spark, producer, and data-quality metrics from
  Prometheus.
- Phase 2 ClickHouse aggregate tables update while replay is active.
- Phase 2 Grafana SQL panels show equivalent business metrics from ClickHouse.
- Dashboard labels are readable without looking up raw category IDs or rule IDs.

## Performance Comparison Plan
| Metric | Phase 1 Baseline | Phase 2 Comparison | Why It Matters |
| --- | --- | --- | --- |
| End-to-end latency | event_time/replay timestamp to Grafana visibility via Prometheus | same workload through ClickHouse-backed panels | Measures real-time dashboard freshness. |
| Spark throughput | processed rows/sec while exporting Prometheus metrics | processed rows/sec while also writing ClickHouse | Shows sink overhead. |
| Dashboard query latency | Grafana panel render time from Prometheus | Grafana panel render time from ClickHouse | Shows user-facing responsiveness. |
| Query flexibility | fixed metric labels and PromQL | SQL over aggregate tables | Shows whether OLAP adds useful analysis power. |
| Resource usage | Kafka/Spark/Prometheus/Grafana CPU and memory | plus ClickHouse CPU, memory, disk, write latency | Shows operational cost. |
| Operational complexity | number of services and recovery paths | added ClickHouse service and sink recovery | Shows maintenance tradeoff. |

Use the same CSV slice, replay speed, Kafka partition count, Spark checkpoint
state, and dashboard panels where possible so the comparison is fair. Keep Phase
1 Prometheus labels intentionally bounded; otherwise Prometheus high-cardinality
overhead would dominate the comparison instead of representing a sensible MVP.

## Open Decisions
- Replay speed for local demos.
- Whether Phase 2 should store validated event-level data in ClickHouse for drilldown.
- Whether to use Avro/Protobuf and schema registry after the JSON prototype.
- Whether user IDs should be hashed before any dashboard drilldown.
