# Schema Design

## Scope and Assumptions
- Source is `data/2019-Oct.csv`, replayed as an ordered event stream.
- Source grain is one user behavior event.
- `category_code` and `brand` may be empty and must remain nullable.
- `category_id` is a technical identifier; dashboard labels should prefer
  `category_code`, derived `category_l1`, `category_l2`, or `unknown`.

## Dataset Inventory
| Dataset | Grain | Owner | Purpose |
| --- | --- | --- | --- |
| `raw_ecommerce_events` | one CSV row | replay producer | Preserve source row and ingestion metadata. |
| `ecommerce_events` | one valid behavior event | Spark streaming | Canonical validated event stream. |
| `invalid_ecommerce_events` | one rejected row | Spark validation | Quarantine malformed or invalid records. |
| `event_metrics_1m` | event_type x minute | Spark aggregation | Core live traffic dashboard. |
| `funnel_metrics_5m` | category x 5-minute window | Spark aggregation | View to cart to purchase conversion. |
| `revenue_metrics_1m` | category/brand x minute | Spark aggregation | Purchase count and revenue dashboard. |
| `pipeline_quality_metrics` | rule x window | Spark validation | Data quality monitoring in Grafana. |

## Entity and Event Contracts

### Canonical Event
| Field | Type | Nullable | Notes |
| --- | --- | --- | --- |
| `event_id` | string | no | Deterministic hash of source file, source line, and event payload. |
| `event_time` | timestamp | no | Parsed from source UTC string. |
| `ingest_time` | timestamp | no | Producer or Spark ingestion timestamp. |
| `event_type` | string | no | Allowed: `view`, `cart`, `purchase`. |
| `product_id` | long | no | Product identifier from source. |
| `category_id` | long | yes | Technical category identifier. |
| `category_code` | string | yes | Original hierarchical category text. |
| `category_l1` | string | yes | First token from `category_code`. |
| `category_l2` | string | yes | Second token from `category_code`. |
| `category_l3` | string | yes | Remaining category path token when present. |
| `category_label` | string | no | `category_code` or `unknown`. |
| `brand` | string | yes | Empty source values become null. |
| `brand_label` | string | no | `brand` or `unknown`. |
| `price` | decimal(12,2) | no | Non-negative item price. |
| `user_id` | long | no | User identifier from source. |
| `user_session` | string | no | Session identifier from source. |
| `source_file` | string | no | `2019-Oct.csv` for this dataset. |
| `source_line` | long | no | CSV row number, excluding or including header by implementation convention. |
| `kafka_topic` | string | no | Kafka topic that delivered the event. |
| `kafka_partition` | int | no | Kafka partition. |
| `kafka_offset` | long | no | Kafka offset. |

## Keys, Grain, Partitions, and Relationships
- Primary event key: `event_id`.
- Kafka message key: `user_session` when present, else `user_id`.
- Kafka partitioning goal: preserve per-session event order for funnel metrics.
- Lake partitions:
  - `event_date`
  - optionally `event_type`
- Optional ClickHouse aggregate ordering for Phase 2:
  - `window_start`
  - `event_type`
  - `category_label`
  - `brand_label`

## Evolution and Compatibility Policy
- Raw topic keeps original payload plus schema version.
- Canonical events use `schema_version = 1`.
- New nullable fields may be added without breaking consumers.
- Changes to field names, types, or event type semantics require a new versioned
  topic and compatibility note.

## Downstream Requirements
- ETL must derive `category_l1`, `category_l2`, `category_l3`,
  `category_label`, and `brand_label`.
- Validation must check event type domain, timestamp parseability, required IDs,
  and non-negative price.
- Monitoring must expose readable category and rule labels.

## Open Questions
- Whether `category_id` should be enriched from an external category dimension.
- Whether user identifiers need hashing before storage or display.
- Whether product price changes should be modeled as a product dimension or
  treated as event-level observed price.
