#!/usr/bin/env bash
set -euo pipefail

TOPIC="${SMOKE_TOPIC:-ecommerce.events.smoke.v1}"
FIXTURE="${SMOKE_FIXTURE:-tests/fixtures/sample_events.csv}"
PRODUCER_CHECKPOINT="storage/smoke/checkpoints/producer_checkpoint.json"
BRONZE_PATH="storage/smoke/bronze/events"
SILVER_PATH="storage/smoke/silver/events"
QUARANTINE_PATH="storage/smoke/quarantine/events"
BRONZE_CHECKPOINT="storage/smoke/checkpoints/bronze_events"
SILVER_CHECKPOINT="storage/smoke/checkpoints/silver_events"
QUARANTINE_CHECKPOINT="storage/smoke/checkpoints/quarantine_events"
LOG_DIR="storage/smoke/logs"

SPARK_BRONZE_PATH="/opt/spark/storage/smoke/bronze/events"
SPARK_SILVER_PATH="/opt/spark/storage/smoke/silver/events"
SPARK_QUARANTINE_PATH="/opt/spark/storage/smoke/quarantine/events"
SPARK_BRONZE_CHECKPOINT="/opt/spark/storage/smoke/checkpoints/bronze_events"
SPARK_SILVER_CHECKPOINT="/opt/spark/storage/smoke/checkpoints/silver_events"
SPARK_QUARANTINE_CHECKPOINT="/opt/spark/storage/smoke/checkpoints/quarantine_events"

KAFKA_CONTAINER="ecommerce-kafka"
SPARK_CONTAINER="ecommerce-spark-master"
KAFKA_BOOTSTRAP="localhost:9092"
SPARK_MASTER="spark://spark-master:7077"
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"

log() {
  printf '[smoke] %s\n' "$*"
}

require_file() {
  if [[ ! -f "$1" ]]; then
    printf 'required file not found: %s\n' "$1" >&2
    exit 1
  fi
}

wait_for_kafka() {
  local attempt
  for attempt in {1..30}; do
    if docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "$KAFKA_BOOTSTRAP" --list >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  printf 'Kafka did not become ready in time.\n' >&2
  exit 1
}

reset_smoke_topic() {
  log "Resetting Kafka topic ${TOPIC}"
  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --delete \
    --if-exists \
    --topic "$TOPIC" >/dev/null 2>&1 || true

  sleep 2

  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --partitions 6 \
    --replication-factor 1 >/dev/null
}

reset_smoke_storage() {
  log "Resetting storage/smoke runtime outputs"
  rm -rf \
    "$PRODUCER_CHECKPOINT" \
    "$BRONZE_PATH" \
    "$SILVER_PATH" \
    "$QUARANTINE_PATH" \
    "$BRONZE_CHECKPOINT" \
    "$SILVER_CHECKPOINT" \
    "$QUARANTINE_CHECKPOINT" \
    "$LOG_DIR"
  mkdir -p "$LOG_DIR"
}

publish_fixture() {
  log "Publishing fixture rows to ${TOPIC}"
  python3 src/producer/csv_replay_producer.py \
    --input "$FIXTURE" \
    --topic "$TOPIC" \
    --checkpoint-file "$PRODUCER_CHECKPOINT" \
    --reset-checkpoint \
    --max-events 7 \
    --checkpoint-interval 1
}

run_bronze() {
  log "Running bronze stream"
  docker exec "$SPARK_CONTAINER" /opt/spark/bin/spark-submit \
    --master "$SPARK_MASTER" \
    --conf spark.ui.showConsoleProgress=false \
    --packages "$KAFKA_PACKAGE" \
    /opt/spark/app/streaming/bronze_stream.py \
    --topic "$TOPIC" \
    --output-path "$SPARK_BRONZE_PATH" \
    --checkpoint-path "$SPARK_BRONZE_CHECKPOINT" \
    --starting-offsets earliest \
    --trigger available-now >"$LOG_DIR/bronze_stream.log" 2>&1
}

run_silver() {
  log "Running silver stream"
  docker exec "$SPARK_CONTAINER" /opt/spark/bin/spark-submit \
    --master "$SPARK_MASTER" \
    --conf spark.ui.showConsoleProgress=false \
    /opt/spark/app/streaming/silver_stream.py \
    --input-path "$SPARK_BRONZE_PATH" \
    --silver-output-path "$SPARK_SILVER_PATH" \
    --quarantine-output-path "$SPARK_QUARANTINE_PATH" \
    --silver-checkpoint-path "$SPARK_SILVER_CHECKPOINT" \
    --quarantine-checkpoint-path "$SPARK_QUARANTINE_CHECKPOINT" \
    --trigger available-now >"$LOG_DIR/silver_stream.log" 2>&1
}

inspect_outputs() {
  local output

  log "Inspecting output counts"
  output=$(docker exec "$SPARK_CONTAINER" sh -c "cat > /tmp/inspect_smoke.py <<'PY'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('inspect-smoke').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

bronze = spark.read.parquet('${SPARK_BRONZE_PATH}')
silver = spark.read.parquet('${SPARK_SILVER_PATH}')
quarantine = spark.read.parquet('${SPARK_QUARANTINE_PATH}')

bronze_count = bronze.count()
silver_count = silver.count()
quarantine_count = quarantine.count()
rule_counts = {
    row['rule_id']: row['count']
    for row in quarantine.groupBy('rule_id').count().collect()
}

print(f'bronze_count={bronze_count}')
print(f'silver_count={silver_count}')
print(f'quarantine_count={quarantine_count}')
print(
    'quarantine_rules='
    + ','.join(f'{rule_id}:{rule_counts[rule_id]}' for rule_id in sorted(rule_counts))
)

assert bronze_count == 7, f'expected bronze_count=7, got {bronze_count}'
assert silver_count == 6, f'expected silver_count=6, got {silver_count}'
assert quarantine_count == 1, f'expected quarantine_count=1, got {quarantine_count}'
assert rule_counts == {'DQ_EVENT_TYPE_DOMAIN': 1}, f'unexpected quarantine rules: {rule_counts}'

spark.stop()
PY
/opt/spark/bin/spark-submit \
  --master local[1] \
  --conf spark.ui.showConsoleProgress=false \
  /tmp/inspect_smoke.py" 2>"$LOG_DIR/inspect_stderr.log")
  printf '%s\n' "$output"
}

main() {
  require_file "$FIXTURE"
  require_file "src/producer/csv_replay_producer.py"
  require_file "src/streaming/bronze_stream.py"
  require_file "src/streaming/silver_stream.py"

  log "Starting local Compose stack"
  docker compose up -d >/dev/null
  wait_for_kafka
  reset_smoke_topic
  reset_smoke_storage
  publish_fixture
  run_bronze
  run_silver
  inspect_outputs
  log "smoke_test=PASS"
  log "Detailed logs: ${LOG_DIR}"
}

main "$@"
