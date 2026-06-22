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
GOLD_SILVER_CHECKPOINT="storage/smoke/checkpoints/gold_silver_metrics"
GOLD_QUARANTINE_CHECKPOINT="storage/smoke/checkpoints/gold_quarantine_metrics"
LOG_DIR="storage/smoke/logs"

SPARK_BRONZE_PATH="/opt/spark/storage/smoke/bronze/events"
SPARK_SILVER_PATH="/opt/spark/storage/smoke/silver/events"
SPARK_QUARANTINE_PATH="/opt/spark/storage/smoke/quarantine/events"
SPARK_BRONZE_CHECKPOINT="/opt/spark/storage/smoke/checkpoints/bronze_events"
SPARK_SILVER_CHECKPOINT="/opt/spark/storage/smoke/checkpoints/silver_events"
SPARK_QUARANTINE_CHECKPOINT="/opt/spark/storage/smoke/checkpoints/quarantine_events"
SPARK_GOLD_SILVER_CHECKPOINT="/opt/spark/storage/smoke/checkpoints/gold_silver_metrics"
SPARK_GOLD_QUARANTINE_CHECKPOINT="/opt/spark/storage/smoke/checkpoints/gold_quarantine_metrics"

KAFKA_CONTAINER="ecommerce-kafka"
SPARK_CONTAINER="ecommerce-spark-master"
KAFKA_BOOTSTRAP="localhost:9092"
SPARK_MASTER="spark://spark-master:7077"
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"
PUSHGATEWAY_URL="http://localhost:9091"
PROMETHEUS_URL="http://localhost:9090"
GRAFANA_URL="http://localhost:3000"
SPARK_PUSHGATEWAY_URL="http://pushgateway:9091"
BUSINESS_JOB="ecommerce_gold_business"
QUALITY_JOB="ecommerce_gold_quality"

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

wait_for_pushgateway() {
  local attempt
  for attempt in {1..30}; do
    if python3 - <<PY >/dev/null 2>&1
import urllib.request
urllib.request.urlopen('${PUSHGATEWAY_URL}/-/healthy', timeout=2).read()
PY
    then
      return 0
    fi
    sleep 2
  done
  printf 'Pushgateway did not become ready in time.\n' >&2
  exit 1
}

wait_for_prometheus() {
  local attempt
  for attempt in {1..30}; do
    if python3 - <<PY >/dev/null 2>&1
import urllib.request
urllib.request.urlopen('${PROMETHEUS_URL}/-/ready', timeout=2).read()
PY
    then
      return 0
    fi
    sleep 2
  done
  printf 'Prometheus did not become ready in time.\n' >&2
  exit 1
}

wait_for_grafana() {
  local attempt
  for attempt in {1..30}; do
    if python3 - <<PY >/dev/null 2>&1
import urllib.request
urllib.request.urlopen('${GRAFANA_URL}/api/health', timeout=2).read()
PY
    then
      return 0
    fi
    sleep 2
  done
  printf 'Grafana did not become ready in time.\n' >&2
  exit 1
}

reload_prometheus_config() {
  log "Reloading Prometheus config"
  python3 - <<PY
import urllib.request
request = urllib.request.Request('${PROMETHEUS_URL}/-/reload', method='POST')
urllib.request.urlopen(request, timeout=5).read()
PY
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
  log "Resetting Pushgateway smoke metrics"
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
    except urllib.error.URLError:
        raise
PY
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
    "$GOLD_SILVER_CHECKPOINT" \
    "$GOLD_QUARANTINE_CHECKPOINT" \
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

run_gold() {
  log "Running gold metrics stream"
  docker exec "$SPARK_CONTAINER" /opt/spark/bin/spark-submit \
    --master "$SPARK_MASTER" \
    --conf spark.ui.showConsoleProgress=false \
    /opt/spark/app/streaming/gold_metrics_stream.py \
    --silver-input-path "$SPARK_SILVER_PATH" \
    --quarantine-input-path "$SPARK_QUARANTINE_PATH" \
    --silver-checkpoint-path "$SPARK_GOLD_SILVER_CHECKPOINT" \
    --quarantine-checkpoint-path "$SPARK_GOLD_QUARANTINE_CHECKPOINT" \
    --pushgateway-url "$SPARK_PUSHGATEWAY_URL" \
    --business-job "$BUSINESS_JOB" \
    --quality-job "$QUALITY_JOB" \
    --trigger available-now >"$LOG_DIR/gold_metrics_stream.log" 2>&1
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

inspect_pushgateway_metrics() {
  log "Inspecting Pushgateway metrics"
  python3 - <<PY
import re
import urllib.request

metrics = urllib.request.urlopen('${PUSHGATEWAY_URL}/metrics', timeout=5).read().decode()

checks = {
    'events_view': r'ecommerce_events_total\{[^}]*event_type="view"[^}]*\}\s+4(?:\.0)?',
    'events_cart': r'ecommerce_events_total\{[^}]*event_type="cart"[^}]*\}\s+1(?:\.0)?',
    'events_purchase': r'ecommerce_events_total\{[^}]*event_type="purchase"[^}]*\}\s+1(?:\.0)?',
    'purchase_total': r'ecommerce_purchase_total(?:\{[^}]*\})?\s+1(?:\.0)?',
    'revenue_total': r'ecommerce_revenue_total(?:\{[^}]*\})?\s+1081\.98',
    'quality_rule': (
        r'dq_rule_failures_total\{[^}]*rule_id="DQ_EVENT_TYPE_DOMAIN"'
        r'[^}]*\}\s+1(?:\.0)?'
    ),
}
missing = [name for name, pattern in checks.items() if not re.search(pattern, metrics)]
if missing:
    raise AssertionError('missing expected Pushgateway metrics: ' + ', '.join(missing))

print('metric_events_view=4')
print('metric_events_cart=1')
print('metric_events_purchase=1')
print('metric_purchase_total=1')
print('metric_revenue_total=1081.98')
print('metric_quality_rule=DQ_EVENT_TYPE_DOMAIN:1')
PY
}

inspect_prometheus_metrics() {
  log "Inspecting Prometheus scrape result"
  python3 - <<PY
import json
import time
import urllib.parse
import urllib.request

for _ in range(20):
    targets = json.loads(
        urllib.request.urlopen('${PROMETHEUS_URL}/api/v1/targets', timeout=5).read()
    )
    pushgateway_targets = [
        target
        for target in targets['data']['activeTargets']
        if target.get('labels', {}).get('job') == 'pushgateway'
    ]
    query = urllib.parse.urlencode({'query': 'ecommerce_purchase_total'})
    result = json.loads(
        urllib.request.urlopen(
            f'${PROMETHEUS_URL}/api/v1/query?{query}',
            timeout=5,
        ).read()
    )
    if (
        pushgateway_targets
        and pushgateway_targets[0].get('health') == 'up'
        and result['data']['result']
    ):
        print('prometheus_pushgateway_target=up')
        print('prometheus_metric_ecommerce_purchase_total=present')
        break
    time.sleep(2)
else:
    raise AssertionError('Prometheus did not scrape Pushgateway metrics in time')
PY
}

inspect_grafana_provisioning() {
  log "Inspecting Grafana provisioning"
  python3 - <<PY
import base64
import json
import time
import urllib.request

auth = base64.b64encode(b'admin:admin').decode()

def get(path):
    request = urllib.request.Request('${GRAFANA_URL}' + path)
    request.add_header('Authorization', 'Basic ' + auth)
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode())

for _ in range(30):
    datasource = get('/api/datasources/uid/prometheus')
    dashboard = get('/api/dashboards/uid/ecommerce-phase1-overview')
    if (
        datasource.get('name') == 'Prometheus'
        and dashboard.get('dashboard', {}).get('title') == 'Ecommerce Phase 1 Overview'
    ):
        print('grafana_datasource_uid=prometheus')
        print('grafana_dashboard_uid=ecommerce-phase1-overview')
        break
    time.sleep(2)
else:
    raise AssertionError('Grafana provisioning was not visible in time')
PY
}

main() {
  require_file "$FIXTURE"
  require_file "src/producer/csv_replay_producer.py"
  require_file "src/streaming/bronze_stream.py"
  require_file "src/streaming/silver_stream.py"
  require_file "src/streaming/gold_metrics_stream.py"

  log "Starting local Compose stack"
  docker compose up -d >/dev/null
  wait_for_kafka
  wait_for_pushgateway
  wait_for_prometheus
  wait_for_grafana
  reload_prometheus_config
  reload_grafana_provisioning
  reset_pushgateway_metrics
  reset_smoke_topic
  reset_smoke_storage
  publish_fixture
  run_bronze
  run_silver
  inspect_outputs
  run_gold
  inspect_pushgateway_metrics
  inspect_prometheus_metrics
  inspect_grafana_provisioning
  log "smoke_test=PASS"
  log "Detailed logs: ${LOG_DIR}"
}

main "$@"
