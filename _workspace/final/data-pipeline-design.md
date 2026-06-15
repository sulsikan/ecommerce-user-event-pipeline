# Real-Time Ecommerce Streaming Dashboard Architecture

## Summary
Use the Kaggle `data/2019-Oct.csv` file as a historical event source and replay
it into Kafka to simulate a live ecommerce event stream. Spark Structured
Streaming reads Kafka, validates and enriches events, writes durable event
history to Delta/Parquet, writes dashboard aggregates to ClickHouse, and exports
pipeline and data-quality metrics to Prometheus. Grafana reads ClickHouse for
business dashboards and Prometheus for pipeline health dashboards.

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
  |-- gold aggregates to ClickHouse
  |-- metrics to Prometheus
  |
  v
Grafana
  |-- ClickHouse datasource for business metrics
  |-- Prometheus datasource for pipeline and quality metrics
```

## Component Responsibilities
| Component | Responsibility |
| --- | --- |
| CSV replay producer | Reads the 2019-Oct CSV and publishes events to Kafka at a controlled rate. |
| Kafka | Buffers raw events, decouples ingestion from processing, supports replay from offsets. |
| Spark Structured Streaming | Parses, validates, enriches, aggregates, and writes streaming outputs. |
| Delta/Parquet storage | Stores bronze raw events and silver validated events for audit and replay. |
| ClickHouse | Serves low-latency aggregate queries to Grafana. |
| Prometheus | Stores operational and data quality metrics. |
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
| Traffic aggregation | validated events | ClickHouse `event_metrics_1m` | Event counts by minute and type. |
| Funnel aggregation | validated events | ClickHouse `funnel_metrics_5m` | View/cart/purchase counts by category. |
| Revenue aggregation | purchase events | ClickHouse `revenue_metrics_1m` | Purchases and revenue by category/brand. |
| Quality metrics | validation results | Prometheus and ClickHouse optional | Rule failures by readable rule name. |

Use event-time watermarks because the source has an original `event_time`.
During replay, ingestion time and event time are intentionally different.

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
- ClickHouse write latency.

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
| ClickHouse `event_metrics_1m` | traffic aggregates | Grafana overview |
| ClickHouse `funnel_metrics_5m` | funnel aggregates | Grafana conversion panels |
| ClickHouse `revenue_metrics_1m` | revenue aggregates | Grafana revenue panels |
| Prometheus | runtime and quality metrics | Grafana health and alerts |

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
|   `-- clickhouse/
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
1. Add Docker Compose for Kafka, Spark, ClickHouse, Prometheus, and Grafana.
2. Implement CSV replay producer with checkpointed source-line progress.
3. Create Kafka topics and publish a small fixture stream.
4. Implement Spark schema parsing and bronze storage.
5. Implement validation and quarantine routing.
6. Implement silver event enrichment and label derivation.
7. Implement ClickHouse aggregate tables and Spark sinks.
8. Configure Prometheus exporters and Grafana datasources.
9. Build Grafana dashboards.
10. Add fixture-based tests and a smoke test that replays a small CSV slice.

## Acceptance Checks
- Producer can replay a small CSV fixture into Kafka.
- Spark can parse and validate all fixture event types.
- Invalid fixture rows land in quarantine with rule names.
- ClickHouse aggregate tables update while replay is active.
- Grafana shows business metrics from ClickHouse.
- Grafana shows Kafka, Spark, producer, and data-quality metrics from
  Prometheus.
- Dashboard labels are readable without looking up raw category IDs or rule IDs.

## Open Decisions
- Replay speed for local demos.
- Whether to store validated event-level data in ClickHouse for drilldown.
- Whether to use Avro/Protobuf and schema registry after the JSON prototype.
- Whether user IDs should be hashed before any dashboard drilldown.
