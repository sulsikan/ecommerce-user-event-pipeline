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

