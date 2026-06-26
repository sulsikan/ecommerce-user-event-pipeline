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

## 2026-06-21 - Add Gold Metrics Export to Prometheus

### User Request
Proceed with the gold aggregation and Prometheus metrics export step for the
Phase 1 dashboard path.

### Work Performed
- Created `feature/gold-prometheus-metrics` from `develop`.
- Added a Pushgateway service to `docker-compose.yml` for local Spark-to-
  Prometheus metric handoff.
- Updated `configs/prometheus/prometheus.yml` so Prometheus scrapes
  `pushgateway:9091` with `honor_labels: true`.
- Added `src/streaming/gold_metrics_stream.py`, a Spark Structured Streaming
  job that reads silver and quarantine parquet outputs, aggregates business and
  data-quality metrics, and pushes Prometheus text-format metrics to
  Pushgateway.
- Exported fixture-verifiable business metrics for event type counts, purchase
  count, revenue, active users, active sessions, category purchases, brand
  revenue, and aggregate freshness.
- Exported quality metrics for quarantined records and DQ rule failures.
- Extended `scripts/smoke_test_fixture.sh` so the smoke test now covers producer,
  Kafka, bronze, silver, quarantine, gold metrics, Pushgateway, and Prometheus
  scrape verification.
- Added Prometheus reload handling to the smoke test so local config changes are
  picked up without manually restarting Prometheus.

### Files Changed
- `docker-compose.yml`
- `configs/prometheus/prometheus.yml`
- `src/streaming/gold_metrics_stream.py`
- `scripts/smoke_test_fixture.sh`
- `docs/project-log.md`

### Verification
- Ran `docker compose config` successfully.
- Compiled `src/streaming/gold_metrics_stream.py` with Python `compile()` without
  writing `__pycache__`.
- Ran `bash -n scripts/smoke_test_fixture.sh` successfully.
- Ran `./scripts/smoke_test_fixture.sh` successfully for the full fixture flow.
- Confirmed the smoke test output reports bronze, silver, and quarantine counts:
  `bronze_count=7`, `silver_count=6`, and `quarantine_count=1`.
- Confirmed Pushgateway metrics include view/cart/purchase counts, purchase
  total, revenue total, and `DQ_EVENT_TYPE_DOMAIN` quality failure count.
- Confirmed Prometheus reports the Pushgateway target as `up` and can query
  `ecommerce_purchase_total`.
- Verified generated `storage/smoke/` runtime outputs remain ignored by Git.

### Next Step
Build the initial Grafana dashboard provisioning for Phase 1 Prometheus metrics,
then run the fixture smoke test to confirm dashboard source metrics are present.

## 2026-06-22 - Add Grafana Phase 1 Dashboard Provisioning

### User Request
Proceed with the Grafana dashboard provisioning work according to the planned
Phase 1 dashboard step.

### Work Performed
- Created `feature/grafana-phase1-dashboard` from `develop`.
- Added a stable Grafana datasource UID `prometheus` to the existing Prometheus
  datasource provisioning file.
- Added `configs/grafana/dashboards/dashboard.yml` so Grafana provisions local
  dashboard JSON files under the Ecommerce Pipeline folder.
- Added `configs/grafana/dashboards/ecommerce-phase1-overview.json`, a Phase 1
  overview dashboard for ecommerce event metrics, purchases, revenue, active
  users, data-quality failures, and Pushgateway scrape health.
- Extended `scripts/smoke_test_fixture.sh` so the smoke test reloads Grafana
  provisioning and verifies the Prometheus datasource UID and dashboard UID via
  the Grafana API.

### Files Changed
- `configs/grafana/datasources/prometheus.yml`
- `configs/grafana/dashboards/dashboard.yml`
- `configs/grafana/dashboards/ecommerce-phase1-overview.json`
- `scripts/smoke_test_fixture.sh`
- `docs/project-log.md`

### Verification
- Validated `configs/grafana/dashboards/ecommerce-phase1-overview.json` with
  `python3 -m json.tool`.
- Ran `docker compose config` successfully.
- Ran `bash -n scripts/smoke_test_fixture.sh` successfully.
- Started Docker Desktop because the Docker daemon was initially unavailable.
- Ran `./scripts/smoke_test_fixture.sh` successfully for the full fixture flow.
- Confirmed the smoke test still reports `bronze_count=7`, `silver_count=6`, and
  `quarantine_count=1`.
- Confirmed Pushgateway and Prometheus metric checks still pass.
- Confirmed Grafana API reports `grafana_datasource_uid=prometheus` and
  `grafana_dashboard_uid=ecommerce-phase1-overview`.

### Next Step
Commit and push `feature/grafana-phase1-dashboard`, merge it into `develop`, and
then review the Phase 1 MVP completeness before deciding whether to improve the
Grafana dashboard panels or begin the Phase 2 ClickHouse comparison path.

## 2026-06-22 - Add Phase 1 MVP Readiness Check

### User Request
Implement the Phase 1 MVP completeness check after the Grafana dashboard
provisioning work was merged into `develop`.

### Work Performed
- Created `feature/phase1-mvp-readiness` from `develop`.
- Added `scripts/check_phase1_mvp_readiness.sh`, an executable readiness check
  that validates required project files, static configuration, ignore rules,
  Python source compilation, the full fixture smoke test, and runtime
  Prometheus/Grafana APIs.
- Reused `scripts/smoke_test_fixture.sh` as the end-to-end runtime verification
  for Kafka, Spark bronze, Spark silver/quarantine, gold metrics, Pushgateway,
  Prometheus, and Grafana.
- Added `docs/phase1-mvp-readiness.md`, a generated readiness report containing
  verified scope, passing checks, smoke-test evidence, remaining Phase 1
  improvements, and Phase 2 entry criteria.
- Fixed the readiness smoke log path so it is not deleted by the smoke test's
  `storage/smoke/` reset.
- Fixed report generation to safely print Markdown bullet lines in Bash.

### Files Changed
- `scripts/check_phase1_mvp_readiness.sh`
- `docs/phase1-mvp-readiness.md`
- `docs/project-log.md`

### Verification
- Ran `bash -n scripts/check_phase1_mvp_readiness.sh` successfully.
- Ran `./scripts/check_phase1_mvp_readiness.sh` successfully.
- Confirmed the readiness check runs the full fixture smoke test and reports
  `smoke_test=PASS`.
- Confirmed the readiness check verifies Prometheus can query
  `ecommerce_purchase_total`.
- Confirmed the readiness check verifies Grafana dashboard UID
  `ecommerce-phase1-overview`.
- Verified readiness runtime logs under `storage/readiness/` remain ignored by
  Git.

### Next Step
Commit and push `feature/phase1-mvp-readiness`, merge it into `develop`, and
then decide whether to polish the Phase 1 Grafana dashboard or begin the Phase 2
ClickHouse comparison path.

## 2026-06-22 - Run Initial Real CSV Streaming Test

### User Request
Start testing with the real Kaggle ecommerce CSV data.

### Work Performed
- Kept the test isolated from fixture/runtime outputs by using the Kafka topic
  `ecommerce.events.realtest.v1` and the `storage/realtest/` runtime path.
- Published the first 1,000 rows from `data/2019-Oct.csv` to Kafka with
  `src/producer/csv_replay_producer.py`.
- Ran the Spark Structured Streaming bronze job from Kafka to parquet.
- Ran the silver parsing and validation job against the bronze output.
- Ran the gold aggregation job and pushed metrics to Pushgateway under
  `ecommerce_realtest_business` and `ecommerce_realtest_quality`.
- Queried Prometheus to confirm it scraped the real-test metrics.

### Verification
- Confirmed the producer reported `published_events=1000`.
- Confirmed Spark output counts: `bronze_count=1000`, `silver_count=1000`, and
  `quarantine_count=0`.
- Confirmed event distribution: `view=987`, `cart=3`, and `purchase=10`.
- Confirmed purchase revenue aggregation: `revenue=2387.84`.
- Confirmed Prometheus query returned `ecommerce_purchase_total=10`.
- Confirmed Prometheus query returned `ecommerce_gold_last_batch_records=1000`.

### Next Step
Decide whether to scale the real-data replay to a larger sample size or update
the Grafana dashboard/job filters so the real-test metrics are visible in the
existing dashboard panels.

## 2026-06-22 - Add Grafana Metric Source Selector

### User Request
Add a Grafana job selector so the dashboard can switch between fixture metrics
and real-test metrics.

### Work Performed
- Added a `metric_source` custom variable to the Phase 1 Grafana dashboard.
- Configured the variable with `fixture` -> `gold` and `realtest` -> `realtest`.
- Updated business metric panels to query `ecommerce_${metric_source}_business`.
- Updated quality metric panels to query `ecommerce_${metric_source}_quality`.
- Extended the smoke test to verify that Grafana provisions the metric source
  variable and that dashboard panel queries reference it.
- Regenerated the Phase 1 readiness report so the new Grafana variable evidence
  is included.

### Files Changed
- `configs/grafana/dashboards/ecommerce-phase1-overview.json`
- `scripts/smoke_test_fixture.sh`
- `docs/phase1-mvp-readiness.md`
- `docs/project-log.md`

### Verification
- Ran `python3 -m json.tool configs/grafana/dashboards/ecommerce-phase1-overview.json` successfully.
- Ran `bash -n scripts/smoke_test_fixture.sh` successfully.
- Reloaded Grafana provisioning and confirmed the dashboard API reports
  `grafana_metric_source_variable=fixture,realtest`.
- Ran `./scripts/smoke_test_fixture.sh` successfully and confirmed
  `smoke_test=PASS`.
- Ran `./scripts/check_phase1_mvp_readiness.sh` successfully and confirmed
  `phase1_mvp_readiness=PASS`.

### Next Step
Open Grafana at `http://localhost:3000`, use the Metric Source dropdown, and
compare the fixture baseline with the 1,000-row real-test metrics before scaling
the replay size.

## 2026-06-22 - Stabilize Live Replay Runner

### User Request
Stabilize `scripts/run_live_replay.sh` so the pipeline can be observed as a slow
live stream in Grafana.

### Work Performed
- Added `scripts/run_live_replay.sh` as a live replay runner that starts Bronze,
  Silver, and Gold Spark Structured Streaming jobs in `processing-time` mode.
- Added producer throttling support through `LIVE_EVENTS_PER_SECOND` so the CSV
  replay can flow gradually instead of all at once.
- Increased the Spark worker capacity from 2 cores / 2 GB to 4 cores / 3 GB for
  three concurrent lightweight streaming jobs.
- Limited each live Spark application to 1 core and 512 MB executor/driver
  memory to avoid one job monopolizing the worker.
- Strengthened cleanup so Spark app IDs, Spark driver processes, and worker
  executors are stopped after a finite live smoke run.
- Fixed `gold_metrics_stream.py` Silver input schema ordering so partitioned
  `event_date` data is read correctly and `event_type` metrics remain
  `view/cart/purchase` instead of being shifted into product IDs.
- Updated the Phase 1 readiness check to include the live replay runner.

### Files Changed
- `.gitignore`
- `docker-compose.yml`
- `src/streaming/gold_metrics_stream.py`
- `scripts/run_live_replay.sh`
- `scripts/check_phase1_mvp_readiness.sh`
- `docs/phase1-mvp-readiness.md`
- `docs/project-log.md`

### Verification
- Ran `bash -n scripts/run_live_replay.sh` successfully.
- Ran `python3 -m py_compile src/streaming/gold_metrics_stream.py` successfully.
- Ran `docker compose config` successfully.
- Ran an isolated live smoke check with 30 real CSV events at 5 events/sec.
- Confirmed live smoke metrics included
  `ecommerce_events_total{event_type="view"}=30`.
- Confirmed live smoke cleanup left no Spark driver or executor processes.
- Ran `./scripts/smoke_test_fixture.sh` successfully and confirmed
  `smoke_test=PASS`.
- Ran `./scripts/check_phase1_mvp_readiness.sh` successfully and confirmed
  `phase1_mvp_readiness=PASS`.

### Next Step
Use `scripts/run_live_replay.sh` with `Metric Source=realtest` in Grafana for a
longer observation run, then decide whether to tune panel refresh intervals or
start Phase 2 ClickHouse comparison work.
