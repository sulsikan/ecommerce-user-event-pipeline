# Request Summary

## User Request
Design a project architecture for a real-time ecommerce event streaming
dashboard.

## Source Data
- File: `./data/2019-Oct.csv`
- Origin: Kaggle ecommerce user behavior dataset
- Size observed locally: 5.3 GB
- Rows observed locally: 42,448,765 including header
- Event time range observed locally:
  - first event: `2019-10-01 00:00:00 UTC`
  - last event: `2019-10-31 23:59:59 UTC`
- Columns:
  - `event_time`
  - `event_type`
  - `product_id`
  - `category_id`
  - `category_code`
  - `brand`
  - `price`
  - `user_id`
  - `user_session`
- Event type counts observed locally:
  - `view`: 40,779,399
  - `cart`: 926,516
  - `purchase`: 742,849

## Minimum Requirements
- Kafka for event streaming.
- Spark Streaming for real-time processing.
- Grafana for dashboards.

## Design Assumptions
- The static CSV is replayed into Kafka to simulate a real-time event stream.
- Spark Structured Streaming is acceptable as the Spark Streaming interface.
- Additional supporting components may be introduced when they keep the
  architecture operable and observable.
- The dashboard should support both business metrics and pipeline health.
- Raw category IDs should not be the only labels shown to users; readable
  labels should be derived from `category_code` where possible.

## Target Audience
- Developer/operator building a local or containerized streaming analytics
  project.
- Dashboard consumer who wants to understand ecommerce traffic, funnel, and
  revenue in near real time.

## Open Questions
- Target runtime: local Docker Compose, Kubernetes, or managed services.
- Required dashboard latency SLA.
- Whether the project should preserve all raw events long term or only enough
  for dashboard replay.
- Whether the dashboard needs user-level drilldown, which may require stricter
  privacy handling.
