# Project Log

This log records meaningful project work. Skip prompts that only ask for status,
confirmation, explanation, or planning when no repository files, commands,
configuration, or implementation work changed.

## 2026-06-20 - Add Project Logging Rule

### User Request
Set a rule to record future project work as a log, including what was requested
and what work was done, while skipping prompts that only ask for status,
confirmation, explanation, or a plan without changing anything.

### Work Performed
- Added a repository-wide logging rule to `AGENTS.md`.
- Created this `docs/project-log.md` file as the durable work log.
- Defined the skip rule for no-op discussion prompts.

### Files Changed
- `AGENTS.md`
- `docs/project-log.md`

### Verification
- Inspected `AGENTS.md` and confirmed the Work Log rules were added.
- Inspected `docs/project-log.md` and confirmed the initial log entry exists.
- Ran `git status --short`; `AGENTS.md` is modified and `docs/project-log.md` is untracked.

### Next Step
Use this log for future implementation, configuration, documentation, testing,
and verification work.

## 2026-06-20 - Build Phase 1 Local Infrastructure Skeleton

### User Request
Implement the first Phase 1 setup task: create `docker-compose.yml`,
`configs/prometheus/`, `configs/grafana/datasources/`, and the minimum project
directory structure for the Kafka -> Spark Streaming -> Prometheus -> Grafana
MVP.

### Work Performed
- Added a Phase 1 `docker-compose.yml` with Kafka, Spark master, Spark worker,
  Prometheus, and Grafana services.
- Switched unavailable Bitnami image tags to available official images:
  `apache/kafka:3.8.0` and `apache/spark:3.5.5`.
- Added Prometheus scrape configuration for Prometheus, Spark master, Spark
  worker, and Spark applications.
- Added Grafana datasource provisioning for Prometheus.
- Added Spark Prometheus servlet metrics configuration.
- Created minimum `configs/`, `src/`, `tests/fixtures/`, and `storage/`
  directory skeletons.
- Updated `.gitignore` so runtime `storage/` outputs stay local while `.gitkeep`
  placeholders remain tracked.
- Started Docker Desktop because the daemon was initially unavailable, then
  started the Compose stack.

### Files Changed
- `docker-compose.yml`
- `.gitignore`
- `configs/prometheus/prometheus.yml`
- `configs/grafana/datasources/prometheus.yml`
- `configs/spark/metrics.properties`
- `configs/grafana/dashboards/.gitkeep`
- `configs/kafka/.gitkeep`
- `src/producer/.gitkeep`
- `src/streaming/.gitkeep`
- `src/quality/.gitkeep`
- `src/common/.gitkeep`
- `tests/fixtures/.gitkeep`
- `storage/bronze/.gitkeep`
- `storage/silver/.gitkeep`
- `storage/quarantine/.gitkeep`
- `storage/checkpoints/.gitkeep`

### Verification
- Ran `docker compose config` successfully.
- Ran `docker compose up -d` successfully after correcting unavailable image tags.
- Ran `docker compose ps`; all five services are up and Kafka is healthy.
- Verified Prometheus readiness from inside the Docker network.
- Verified Grafana health from inside the Docker network.
- Verified Spark master JSON reports one alive worker with 2 cores and 2 GB memory.
- Verified Prometheus active targets are `up` for Prometheus, Spark master, Spark
  worker, and Spark applications.
- Verified Grafana has the provisioned Prometheus datasource.

### Next Step
Create a small fixture CSV and implement the checkpointed CSV replay producer
that publishes events to Kafka topic `ecommerce.events.raw.v1`.

## 2026-06-20 - Add Fixture CSV and Checkpointed Kafka Replay Producer

### User Request
Continue with the next planned implementation step after the Phase 1 local
infrastructure setup.

### Work Performed
- Added `tests/fixtures/sample_events.csv` with a small ecommerce event sample
  that includes view, cart, purchase, nullable category/brand cases, and one
  intentionally unsupported event type for later validation testing.
- Added `src/producer/csv_replay_producer.py`, a standard-library Python replay
  producer that reads CSV rows, converts them to JSON payloads, and publishes
  them to Kafka through the Kafka CLI in the running Docker Kafka container.
- Added producer metadata fields: `schema_version`, `source_file`,
  `source_line`, and `replay_time`.
- Added producer checkpoint support so successful publishes can resume from the
  last source line.
- Added optional topic creation through `--create-topic`.
- Made the producer executable.
- Tightened `.gitignore` so runtime files under `storage/` stay local while
  `.gitkeep` placeholders remain trackable.

### Files Changed
- `.gitignore`
- `src/producer/csv_replay_producer.py`
- `tests/fixtures/sample_events.csv`
- `docs/project-log.md`

### Verification
- Compiled `src/producer/csv_replay_producer.py` with Python `compile()` without
  writing `__pycache__`.
- Ran the producer with `--publisher stdout --max-events 2` and confirmed CSV
  rows are converted to keyed JSON events.
- Confirmed Kafka was running and initially had no user topics.
- Ran the producer against Kafka with `--create-topic`, publishing 7 fixture
  events to `ecommerce.events.raw.v1`.
- Described the Kafka topic and confirmed it has 6 partitions and replication
  factor 1.
- Consumed 7 messages from `ecommerce.events.raw.v1` and confirmed the expected
  keyed JSON events were present.
- Inspected the runtime checkpoint and confirmed `last_source_line` is 8 after
  publishing the fixture.
- Verified `storage/checkpoints/fixture_replay_checkpoint.json` is ignored by
  Git.

### Next Step
Implement the Spark Structured Streaming bronze reader that consumes
`ecommerce.events.raw.v1`, parses Kafka key/value metadata, and writes raw event
records to the bronze storage path.

## 2026-06-21 - Add Spark Bronze Kafka Reader

### User Request
Continue with the next planned implementation step after the checkpointed Kafka
CSV replay producer, starting from step 2 because the old feature branch cleanup
was already handled by the user.

### Work Performed
- Added `src/streaming/bronze_stream.py`, a Spark Structured Streaming job that
  consumes raw ecommerce events from Kafka topic `ecommerce.events.raw.v1`.
- Preserved raw Kafka payloads and metadata as bronze parquet columns:
  `kafka_key`, `kafka_value`, `kafka_topic`, `kafka_partition`, `kafka_offset`,
  `kafka_timestamp`, `kafka_timestamp_type`, and `ingest_time`.
- Added configurable runtime options for Kafka bootstrap server, topic, output
  path, checkpoint path, starting offsets, query name, and trigger mode.
- Used `available-now` as the default trigger for local smoke tests, with
  `processing-time` available for long-running streaming mode.
- Recreated the Spark containers after discovering the already-running
  containers did not see the newly added bind-mounted source file.

### Files Changed
- `src/streaming/bronze_stream.py`
- `docs/project-log.md`

### Verification
- Compiled `src/streaming/bronze_stream.py` with Python `compile()` without
  writing `__pycache__`.
- Ran `spark-submit` in the Spark master container with the Spark Kafka
  connector package.
- Confirmed the job completed successfully with `--trigger available-now` and
  `--starting-offsets earliest`.
- Read the generated bronze parquet output with a temporary Spark inspection job
  and confirmed `count=7`.
- Confirmed the bronze output contains the expected Kafka topic, key, partition,
  offset, and raw JSON payload columns.
- Verified generated parquet and checkpoint files under `storage/` are ignored
  by Git.

### Next Step
Commit and push `feature/spark-bronze-reader`, then start the silver parsing and
data validation step that converts `kafka_value` JSON into typed event columns
and validation outcomes.

## 2026-06-21 - Add Silver Event Parser and Quarantine Routing

### User Request
Continue with the next implementation step after bronze storage, following the
planned Silver parsing and validation phase.

### Work Performed
- Created `feature/silver-event-parser` from `develop`.
- Added `src/streaming/silver_stream.py`, a Spark Structured Streaming job that
  reads bronze parquet records and parses `kafka_value` JSON into canonical event
  columns.
- Normalized blank strings to null and derived typed fields for event time,
  event type, product ID, category ID, price, user ID, and user session.
- Derived `category_l1`, `category_l2`, `category_l3`, `category_label`,
  `brand_label`, `event_id`, and `event_date`.
- Added critical validation routing for timestamp parsing, event type domain,
  required product/user/session fields, and invalid prices.
- Wrote valid records to `storage/silver/events` and invalid records to
  `storage/quarantine/events` with rule ID, rule name, severity, raw payload,
  source metadata, and Kafka metadata.
- Started Docker Desktop and the local Compose stack to run Spark verification.

### Files Changed
- `src/streaming/silver_stream.py`
- `docs/project-log.md`

### Verification
- Compiled `src/streaming/silver_stream.py` with Python `compile()` without
  writing `__pycache__`.
- Ran `spark-submit` in the Spark master container with `--trigger available-now`.
- Read the generated silver and quarantine parquet output with a temporary Spark
  inspection job.
- Confirmed the fixture produced `silver_count=6` and `quarantine_count=1`.
- Confirmed the quarantined fixture record failed with
  `DQ_EVENT_TYPE_DOMAIN:1` for unsupported event type `remove_from_cart`.
- Confirmed missing category and brand values are retained in silver with
  `unknown` labels instead of being quarantined.
- Verified generated silver, quarantine, and checkpoint files under `storage/`
  are ignored by Git.

### Next Step
Add fixture-based automated tests or a repeatable smoke-test script, then proceed
to gold aggregation and Prometheus metrics export for the Phase 1 dashboard.

## 2026-06-21 - Add Fixture Smoke Test Script

### User Request
Add a repeatable smoke test script for the fixture-based Kafka, bronze, silver,
and quarantine flow before moving on to gold aggregation and metrics export.

### Work Performed
- Created `feature/fixture-smoke-test` from `develop`.
- Added `scripts/smoke_test_fixture.sh` as an executable end-to-end smoke test.
- The script starts the local Docker Compose stack, waits for Kafka readiness,
  resets only the smoke Kafka topic `ecommerce.events.smoke.v1`, and resets only
  runtime files under `storage/smoke/`.
- The script publishes `tests/fixtures/sample_events.csv` through the existing
  CSV replay producer.
- The script runs the bronze Spark stream against the smoke topic and writes to
  smoke-only bronze storage.
- The script runs the silver Spark stream against smoke bronze output and writes
  to smoke-only silver and quarantine storage.
- The script inspects the generated parquet outputs with Spark and asserts the
  expected fixture counts and quarantine rule counts.
- Detailed Spark logs are written under `storage/smoke/logs/` while the terminal
  output stays focused on the final counts and pass/fail result.

### Files Changed
- `scripts/smoke_test_fixture.sh`
- `docs/project-log.md`

### Verification
- Ran `bash -n scripts/smoke_test_fixture.sh` successfully.
- Ran `./scripts/smoke_test_fixture.sh` successfully after initial creation.
- Cleaned up verbose Spark output by redirecting detailed logs to
  `storage/smoke/logs/`.
- Re-ran `./scripts/smoke_test_fixture.sh` successfully after the logging
  cleanup.
- Confirmed the smoke test output reports `bronze_count=7`, `silver_count=6`,
  `quarantine_count=1`, and `quarantine_rules=DQ_EVENT_TYPE_DOMAIN:1`.
- Verified `storage/smoke/` runtime outputs are ignored by Git.

### Next Step
Commit and push `feature/fixture-smoke-test`, merge it into `develop`, and then
start gold aggregation plus Prometheus metrics export for the Phase 1 dashboard.
