# Validation Rules

## Scope and Assumptions
- Validation happens in Spark after raw Kafka ingestion and before canonical
  events are written to silver/gold outputs.
- Invalid records are quarantined and counted for monitoring.
- Some source fields are legitimately nullable, especially `category_code` and
  `brand`.

## Rule Inventory
| Rule ID | Name | Severity | Threshold | Owner | Action |
| --- | --- | --- | --- | --- | --- |
| `DQ_EVENT_TIME_PARSE` | Event time parse failed | critical | any | data engineering | quarantine record |
| `DQ_EVENT_TYPE_DOMAIN` | Event type is unsupported | critical | any | data engineering | quarantine record |
| `DQ_PRODUCT_ID_REQUIRED` | Product ID is missing | critical | any | data engineering | quarantine record |
| `DQ_USER_ID_REQUIRED` | User ID is missing | critical | any | data engineering | quarantine record |
| `DQ_SESSION_REQUIRED` | User session is missing | high | any | data engineering | quarantine record |
| `DQ_PRICE_INVALID` | Price is negative or non-numeric | critical | any | data engineering | quarantine record |
| `DQ_CATEGORY_LABEL_UNKNOWN` | Category label is unknown | warning | rate > 20% for 10m | analytics | keep record and alert |
| `DQ_BRAND_LABEL_UNKNOWN` | Brand label is unknown | info | rate > 50% for 10m | analytics | keep record |
| `DQ_DUPLICATE_EVENT_ID` | Duplicate event ID detected | high | rate > 0.1% for 10m | data engineering | dedupe and alert |
| `DQ_REPLAY_LAG_HIGH` | Replay or processing lag is high | high | lag > 5m | platform | alert |

## Rule Definitions
- Required field checks apply to canonical event creation.
- Domain checks apply before aggregate updates.
- Warning rules may keep records if dashboard labels remain usable.
- Duplicate checks use `event_id` in the silver stream and sink merge logic.

## Freshness, Completeness, Validity, Uniqueness, Reconciliation
- Freshness:
  - latest processed `event_time` should be within 5 minutes of replay head.
  - ClickHouse aggregate max `window_end` should advance every minute during
    replay.
- Completeness:
  - raw Kafka consumed count should equal silver valid count plus quarantine
    count for each micro-batch.
- Validity:
  - `event_type` in `view`, `cart`, `purchase`.
  - `price >= 0`.
  - `event_time` parsed as UTC timestamp.
- Uniqueness:
  - duplicate `event_id` rate should stay below 0.1 percent over 10 minutes.
- Reconciliation:
  - purchase count in `event_metrics_1m` should equal purchase count in
    `revenue_metrics_1m` by matching windows after watermark closure.

## Quarantine and Remediation Policy
- Quarantined records store:
  - raw payload
  - rule ID
  - rule name
  - severity
  - source file and line
  - Kafka topic, partition, and offset
  - detected timestamp
- Critical rule failures do not update business aggregates.
- Warning and info rule failures may update aggregates with `unknown` labels.
- Quarantine records are queryable for debugging and alert evidence.

## Metrics Exposed to Monitoring
| Metric | Labels | Source |
| --- | --- | --- |
| `dq_rule_failures_total` | `rule_id`, `rule_name`, `severity` | Spark validation |
| `dq_rule_failure_rate` | `rule_id`, `rule_name`, `severity` | Spark validation |
| `stream_records_raw_total` | `topic` | Spark/Kafka |
| `stream_records_valid_total` | `topic` | Spark validation |
| `stream_records_quarantined_total` | `rule_id`, `rule_name` | Spark validation |
| `stream_replay_lag_seconds` | `producer`, `topic` | producer and Spark |
| `aggregate_freshness_seconds` | `aggregate_name` | Spark or ClickHouse probe |

## Test Data or Fixture Needs
- A small CSV fixture with:
  - one valid view event
  - one valid cart event
  - one valid purchase event
  - one invalid timestamp
  - one unknown event type
  - one negative price
  - one missing category code that should be kept with `unknown`

## Open Questions
- Exact thresholds for production alerts versus demo alerts.
- Whether unknown category rate should be benchmarked from the full dataset
  before setting a final threshold.
