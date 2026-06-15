# ETL Logic

## Scope and Assumptions
- The project simulates real-time ingestion by replaying `2019-Oct.csv`.
- Kafka is the durable streaming buffer.
- Spark Structured Streaming owns parsing, validation routing, enrichment,
  aggregation, and sink writes.

## Source-to-Target Mapping
| Source | Target | Transformation |
| --- | --- | --- |
| CSV row | Kafka `ecommerce.events.raw.v1` | Replay row as JSON with source metadata. |
| Kafka raw event | `raw_ecommerce_events` | Persist payload and Kafka metadata. |
| Kafka raw event | `ecommerce_events` | Parse, validate, normalize, derive labels. |
| Invalid raw event | `invalid_ecommerce_events` and Kafka DLQ | Store row, rule, reason, and source metadata. |
| Valid event | `event_metrics_1m` | Count events by minute and event type. |
| Valid event | `funnel_metrics_5m` | Count view/cart/purchase by category window. |
| Purchase event | `revenue_metrics_1m` | Sum price and count purchases by category/brand. |
| Validation results | `pipeline_quality_metrics` | Count rule failures by window and severity. |

## Phase-by-Phase ETL Flow
1. CSV replay producer
   - Reads `data/2019-Oct.csv` sequentially.
   - Publishes JSON messages to `ecommerce.events.raw.v1`.
   - Adds `source_file`, `source_line`, `replay_time`, and `schema_version`.
   - Supports `--speed-factor` or fixed `--events-per-second`.
2. Kafka ingestion
   - Topic: `ecommerce.events.raw.v1`.
   - Key: `user_session` if present, else `user_id`.
   - Retention: long enough for Spark recovery and local replay debugging.
3. Spark bronze stream
   - Reads Kafka offsets with checkpointing.
   - Writes raw payloads to Delta/Parquet bronze storage.
4. Spark silver stream
   - Parses source fields.
   - Normalizes empty strings to null.
   - Derives labels and category levels.
   - Routes invalid records to quarantine and DLQ.
5. Spark gold aggregations
   - Computes 1-minute traffic and revenue windows.
   - Computes 5-minute funnel windows.
   - Uses watermarking on `event_time`.
   - Writes queryable aggregates to ClickHouse.
6. Metrics export
   - Emits Spark job, Kafka lag, producer progress, validation failure, and
     aggregation freshness metrics to Prometheus.
7. Grafana
   - Reads business aggregates from ClickHouse.
   - Reads operational metrics from Prometheus.

## Transformation Rules
- `event_time`: parse as UTC timestamp.
- `event_type`: lowercase and validate against allowed values.
- `price`: parse decimal, reject negative or non-numeric values.
- `category_code`: split by `.`, derive `category_l1`, `category_l2`,
  `category_l3`.
- `category_label`: use `category_code`, else `unknown`.
- `brand_label`: use `brand`, else `unknown`.
- `event_id`: deterministic hash from source metadata and normalized payload.

## Incremental Processing and Checkpointing
- Producer checkpoint: last successfully published CSV line.
- Spark checkpoint paths:
  - `storage/checkpoints/bronze_events`
  - `storage/checkpoints/silver_events`
  - `storage/checkpoints/gold_event_metrics`
  - `storage/checkpoints/gold_funnel_metrics`
  - `storage/checkpoints/gold_revenue_metrics`
- Kafka offsets are committed through Spark checkpoints.
- Use event-time watermarks:
  - traffic metrics: 10 minutes
  - funnel metrics: 30 minutes
  - revenue metrics: 10 minutes

## Idempotency, Retry, and Recovery Policy
- Producer can resume from line checkpoint without republishing acknowledged rows.
- Spark writes are idempotent by `event_id` for event tables and by
  `(window_start, window_end, dimensions)` for aggregates.
- Retryable failures:
  - transient Kafka unavailability
  - transient ClickHouse write failure
  - Spark executor restart
- Data-quality failures:
  - invalid timestamp
  - unknown event type
  - missing required IDs
  - negative price
- Data-quality failures are quarantined, not retried as system failures.

## Performance and Scaling Notes
- 42.4M rows fits a meaningful local streaming replay workload.
- Start with 6 to 12 Kafka partitions for the raw topic.
- Spark should process micro-batches with backpressure enabled.
- ClickHouse is selected for aggregate query speed under high event volume.
- Use separate Spark queries or clearly separated sinks for bronze, silver, and
  gold outputs so failures are easier to isolate.

## Dependencies for Validation and Monitoring
- Validation consumes canonical schema expectations from
  `_workspace/02_schema_design.md`.
- Monitoring consumes:
  - producer progress
  - Kafka lag
  - Spark micro-batch duration
  - rows processed per second
  - validation failure counts
  - aggregate freshness
  - ClickHouse write latency

## Open Questions
- Exact replay speed for demos.
- Whether to use one Spark application with multiple queries or separate apps.
- Whether ClickHouse should store raw validated events or only aggregates.
