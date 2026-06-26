#!/usr/bin/env bash
set -euo pipefail

TOPIC="${LIVE_TOPIC:-ecommerce.events.realtest.v1}"
INPUT="${LIVE_INPUT:-data/2019-Oct.csv}"
MAX_EVENTS="${LIVE_MAX_EVENTS:-1000}"
EVENTS_PER_SECOND="${LIVE_EVENTS_PER_SECOND:-5}"
PROCESSING_TIME="${LIVE_PROCESSING_TIME:-10 seconds}"
CHECKPOINT_INTERVAL="${LIVE_CHECKPOINT_INTERVAL:-50}"
RESET="${LIVE_RESET:-1}"
KEEP_RUNNING="${LIVE_KEEP_RUNNING:-1}"
DRAIN_SECONDS="${LIVE_DRAIN_SECONDS:-20}"
RUN_ROOT="${LIVE_RUN_ROOT:-storage/live}"

PRODUCER_CHECKPOINT="${RUN_ROOT}/checkpoints/producer_checkpoint.json"
BRONZE_PATH="${RUN_ROOT}/bronze/events"
SILVER_PATH="${RUN_ROOT}/silver/events"
QUARANTINE_PATH="${RUN_ROOT}/quarantine/events"
BRONZE_CHECKPOINT="${RUN_ROOT}/checkpoints/bronze_events"
SILVER_CHECKPOINT="${RUN_ROOT}/checkpoints/silver_events"
QUARANTINE_CHECKPOINT="${RUN_ROOT}/checkpoints/quarantine_events"
GOLD_SILVER_CHECKPOINT="${RUN_ROOT}/checkpoints/gold_silver_metrics"
GOLD_QUARANTINE_CHECKPOINT="${RUN_ROOT}/checkpoints/gold_quarantine_metrics"
LOG_DIR="${RUN_ROOT}/logs"

SPARK_RUN_ROOT="/opt/spark/${RUN_ROOT}"
SPARK_BRONZE_PATH="${SPARK_RUN_ROOT}/bronze/events"
SPARK_SILVER_PATH="${SPARK_RUN_ROOT}/silver/events"
SPARK_QUARANTINE_PATH="${SPARK_RUN_ROOT}/quarantine/events"
SPARK_BRONZE_CHECKPOINT="${SPARK_RUN_ROOT}/checkpoints/bronze_events"
SPARK_SILVER_CHECKPOINT="${SPARK_RUN_ROOT}/checkpoints/silver_events"
SPARK_QUARANTINE_CHECKPOINT="${SPARK_RUN_ROOT}/checkpoints/quarantine_events"
SPARK_GOLD_SILVER_CHECKPOINT="${SPARK_RUN_ROOT}/checkpoints/gold_silver_metrics"
SPARK_GOLD_QUARANTINE_CHECKPOINT="${SPARK_RUN_ROOT}/checkpoints/gold_quarantine_metrics"

KAFKA_CONTAINER="ecommerce-kafka"
SPARK_CONTAINER="ecommerce-spark-master"
SPARK_WORKER_CONTAINER="ecommerce-spark-worker"
KAFKA_BOOTSTRAP="localhost:9092"
SPARK_MASTER="spark://spark-master:7077"
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"
PUSHGATEWAY_URL="http://localhost:9091"
PROMETHEUS_URL="http://localhost:9090"
GRAFANA_URL="http://localhost:3000"
SPARK_PUSHGATEWAY_URL="http://pushgateway:9091"
BUSINESS_JOB="${LIVE_BUSINESS_JOB:-ecommerce_realtest_business}"
QUALITY_JOB="${LIVE_QUALITY_JOB:-ecommerce_realtest_quality}"
SPARK_APP_CORES="${LIVE_SPARK_APP_CORES:-1}"
SPARK_EXECUTOR_CORES="${LIVE_SPARK_EXECUTOR_CORES:-1}"
SPARK_EXECUTOR_MEMORY="${LIVE_SPARK_EXECUTOR_MEMORY:-512m}"
SPARK_DRIVER_MEMORY="${LIVE_SPARK_DRIVER_MEMORY:-512m}"

BRONZE_PID=""
SILVER_PID=""
GOLD_PID=""

log() {
  printf '[live] %s\n' "$*"
}

require_file() {
  if [[ ! -f "$1" ]]; then
    printf 'required file not found: %s\n' "$1" >&2
    exit 1
  fi
}

wait_for_http() {
  local name="$1"
  local url="$2"
  local attempt
  for attempt in {1..30}; do
    if python3 - <<PY >/dev/null 2>&1
import urllib.request
urllib.request.urlopen('${url}', timeout=2).read()
PY
    then
      return 0
    fi
    sleep 2
  done
  printf '%s did not become ready in time.\n' "$name" >&2
  exit 1
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

reload_grafana_provisioning() {
  log "Reloading Grafana provisioning"
  python3 - <<PY
import base64
import urllib.error
import urllib.request

auth = base64.b64encode(b'admin:admin').decode()
for path in (
    '/api/admin/provisioning/datasources/reload',
    '/api/admin/provisioning/dashboards/reload',
):
    request = urllib.request.Request('${GRAFANA_URL}' + path, method='POST')
    request.add_header('Authorization', 'Basic ' + auth)
    try:
        urllib.request.urlopen(request, timeout=10).read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            continue
        raise
PY
}

reset_pushgateway_metrics() {
  log "Resetting Pushgateway jobs ${BUSINESS_JOB}, ${QUALITY_JOB}"
  python3 - <<PY
import urllib.error
import urllib.request

for job in ('${BUSINESS_JOB}', '${QUALITY_JOB}'):
    request = urllib.request.Request(
        '${PUSHGATEWAY_URL}/metrics/job/' + job,
        method='DELETE',
    )
    try:
        urllib.request.urlopen(request, timeout=5).read()
    except urllib.error.HTTPError as exc:
        if exc.code not in (202, 404):
            raise
PY
}

reset_topic() {
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

reset_storage() {
  log "Resetting ${RUN_ROOT} runtime outputs"
  rm -rf \
    "$PRODUCER_CHECKPOINT" \
    "$BRONZE_PATH" \
    "$SILVER_PATH" \
    "$QUARANTINE_PATH" \
    "$BRONZE_CHECKPOINT" \
    "$SILVER_CHECKPOINT" \
    "$QUARANTINE_CHECKPOINT" \
    "$GOLD_SILVER_CHECKPOINT" \
    "$GOLD_QUARANTINE_CHECKPOINT" \
    "$LOG_DIR"
}

prepare_storage() {
  mkdir -p \
    "$LOG_DIR" \
    "$(dirname "$PRODUCER_CHECKPOINT")" \
    "$BRONZE_PATH" \
    "$SILVER_PATH" \
    "$QUARANTINE_PATH"
}

spark_submit_base() {
  docker exec "$SPARK_CONTAINER" /opt/spark/bin/spark-submit \
    --master "$SPARK_MASTER" \
    --driver-memory "$SPARK_DRIVER_MEMORY" \
    --conf spark.ui.showConsoleProgress=false \
    --conf "spark.cores.max=${SPARK_APP_CORES}" \
    --conf "spark.executor.cores=${SPARK_EXECUTOR_CORES}" \
    --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY}" \
    "$@"
}

extract_app_ids() {
  local log_file="$1"
  if [[ -f "$log_file" ]]; then
    grep -Eo 'app-[0-9-]+' "$log_file" | sort -u || true
  fi
}

kill_spark_apps_from_log() {
  local log_file="$1"
  local app_id
  while IFS= read -r app_id; do
    [[ -z "$app_id" ]] && continue
    log "Stopping Spark app ${app_id}"
    docker exec "$SPARK_CONTAINER" /opt/spark/bin/spark-class \
      org.apache.spark.deploy.Client kill "$SPARK_MASTER" "$app_id" >/dev/null 2>&1 || true
  done < <(extract_app_ids "$log_file")
}

kill_matching_processes() {
  local container="$1"
  local first_marker="$2"
  local second_marker="$3"
  docker exec     -e FIRST_MARKER="$first_marker"     -e SECOND_MARKER="$second_marker"     "$container" sh -c '
find_pids() {
  ps -eo pid=,args=     | grep "$FIRST_MARKER"     | grep "$SECOND_MARKER"     | grep -v grep     | while read -r pid rest; do printf "%s\n" "$pid"; done
}
find_pids | while read -r pid; do
  [ -n "$pid" ] && kill -TERM "$pid" 2>/dev/null || true
done
sleep 2
find_pids | while read -r pid; do
  [ -n "$pid" ] && kill -KILL "$pid" 2>/dev/null || true
done
' >/dev/null 2>&1 || true
}


kill_worker_executors_from_log() {
  local log_file="$1"
  local app_id
  while IFS= read -r app_id; do
    [[ -z "$app_id" ]] && continue
    kill_matching_processes "$SPARK_WORKER_CONTAINER" "CoarseGrainedExecutorBackend" "$app_id"
  done < <(extract_app_ids "$log_file")
}

cleanup() {
  trap - EXIT INT TERM

  local pid log_file
  for log_file in \
    "$LOG_DIR/gold_metrics_stream.log" \
    "$LOG_DIR/silver_stream.log" \
    "$LOG_DIR/bronze_stream.log"; do
    kill_spark_apps_from_log "$log_file"
    kill_worker_executors_from_log "$log_file"
  done

  for pid in "$GOLD_PID" "$SILVER_PID" "$BRONZE_PID"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done

  kill_matching_processes "$SPARK_CONTAINER" "gold_metrics_stream.py" "$SPARK_RUN_ROOT"
  kill_matching_processes "$SPARK_CONTAINER" "silver_stream.py" "$SPARK_RUN_ROOT"
  kill_matching_processes "$SPARK_CONTAINER" "bronze_stream.py" "$TOPIC"

  wait >/dev/null 2>&1 || true
  log "Stopped live streaming jobs"
}

ensure_running() {
  local name="$1"
  local pid="$2"
  local log_file="$3"
  sleep 6
  if ! kill -0 "$pid" >/dev/null 2>&1; then
    printf '%s stream exited early. Last log lines:\n' "$name" >&2
    tail -80 "$log_file" >&2 || true
    exit 1
  fi
}

start_bronze() {
  log "Starting bronze stream (${PROCESSING_TIME})"
  spark_submit_base \
    --packages "$KAFKA_PACKAGE" \
    /opt/spark/app/streaming/bronze_stream.py \
    --topic "$TOPIC" \
    --output-path "$SPARK_BRONZE_PATH" \
    --checkpoint-path "$SPARK_BRONZE_CHECKPOINT" \
    --starting-offsets earliest \
    --trigger processing-time \
    --processing-time "$PROCESSING_TIME" >"$LOG_DIR/bronze_stream.log" 2>&1 &
  BRONZE_PID="$!"
  ensure_running "bronze" "$BRONZE_PID" "$LOG_DIR/bronze_stream.log"
}

start_silver() {
  log "Starting silver stream (${PROCESSING_TIME})"
  spark_submit_base \
    /opt/spark/app/streaming/silver_stream.py \
    --input-path "$SPARK_BRONZE_PATH" \
    --silver-output-path "$SPARK_SILVER_PATH" \
    --quarantine-output-path "$SPARK_QUARANTINE_PATH" \
    --silver-checkpoint-path "$SPARK_SILVER_CHECKPOINT" \
    --quarantine-checkpoint-path "$SPARK_QUARANTINE_CHECKPOINT" \
    --trigger processing-time \
    --processing-time "$PROCESSING_TIME" >"$LOG_DIR/silver_stream.log" 2>&1 &
  SILVER_PID="$!"
  ensure_running "silver" "$SILVER_PID" "$LOG_DIR/silver_stream.log"
}

start_gold() {
  log "Starting gold metrics stream (${PROCESSING_TIME})"
  spark_submit_base \
    /opt/spark/app/streaming/gold_metrics_stream.py \
    --silver-input-path "$SPARK_SILVER_PATH" \
    --quarantine-input-path "$SPARK_QUARANTINE_PATH" \
    --silver-checkpoint-path "$SPARK_GOLD_SILVER_CHECKPOINT" \
    --quarantine-checkpoint-path "$SPARK_GOLD_QUARANTINE_CHECKPOINT" \
    --pushgateway-url "$SPARK_PUSHGATEWAY_URL" \
    --business-job "$BUSINESS_JOB" \
    --quality-job "$QUALITY_JOB" \
    --trigger processing-time \
    --processing-time "$PROCESSING_TIME" >"$LOG_DIR/gold_metrics_stream.log" 2>&1 &
  GOLD_PID="$!"
  ensure_running "gold" "$GOLD_PID" "$LOG_DIR/gold_metrics_stream.log"
}

publish_events() {
  log "Publishing ${MAX_EVENTS} events from ${INPUT} at ${EVENTS_PER_SECOND} events/sec"
  python3 src/producer/csv_replay_producer.py \
    --input "$INPUT" \
    --topic "$TOPIC" \
    --checkpoint-file "$PRODUCER_CHECKPOINT" \
    --reset-checkpoint \
    --max-events "$MAX_EVENTS" \
    --events-per-second "$EVENTS_PER_SECOND" \
    --checkpoint-interval "$CHECKPOINT_INTERVAL" \
    --create-topic
}

print_dashboard_hint() {
  cat <<MSG

Live replay is running.
- Grafana: ${GRAFANA_URL}
- Dashboard: Ecommerce Phase 1 Overview
- Metric Source: realtest
- Topic: ${TOPIC}
- Business job: ${BUSINESS_JOB}
- Quality job: ${QUALITY_JOB}
- Logs: ${LOG_DIR}

Press Ctrl+C in this terminal to stop Bronze/Silver/Gold streaming jobs.
MSG
}

main() {
  require_file "$INPUT"
  require_file "src/producer/csv_replay_producer.py"
  require_file "src/streaming/bronze_stream.py"
  require_file "src/streaming/silver_stream.py"
  require_file "src/streaming/gold_metrics_stream.py"

  trap cleanup EXIT INT TERM

  log "Starting local Compose stack"
  docker compose up -d >/dev/null
  wait_for_kafka
  wait_for_http "Pushgateway" "${PUSHGATEWAY_URL}/-/healthy"
  wait_for_http "Prometheus" "${PROMETHEUS_URL}/-/ready"
  wait_for_http "Grafana" "${GRAFANA_URL}/api/health"
  reload_grafana_provisioning

  if [[ "$RESET" == "1" ]]; then
    reset_pushgateway_metrics
    reset_topic
    reset_storage
  fi
  prepare_storage

  start_bronze
  start_silver
  start_gold
  print_dashboard_hint
  publish_events

  log "Producer finished. Metrics may continue updating while Spark processes the final micro-batches."
  if [[ "$KEEP_RUNNING" == "1" ]]; then
    log "Keeping streams alive for Grafana observation. Press Ctrl+C to stop."
    while true; do
      sleep 60
    done
  fi

  if [[ "$DRAIN_SECONDS" != "0" ]]; then
    log "Waiting ${DRAIN_SECONDS}s for final streaming micro-batches"
    sleep "$DRAIN_SECONDS"
  fi
}

main "$@"
