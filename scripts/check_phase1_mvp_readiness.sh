#!/usr/bin/env bash
set -euo pipefail

REPORT_PATH="${PHASE1_READINESS_REPORT:-docs/phase1-mvp-readiness.md}"
SMOKE_LOG="storage/readiness/phase1_readiness_smoke.log"
PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"
GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"

PASSED_CHECKS=()
WARNINGS=()

log() {
  printf '[readiness] %s\n' "$*"
}

pass() {
  PASSED_CHECKS+=("$1")
  printf '[pass] %s\n' "$1"
}

warn() {
  WARNINGS+=("$1")
  printf '[warn] %s\n' "$1"
}

require_file() {
  if [[ ! -f "$1" ]]; then
    printf 'required file not found: %s\n' "$1" >&2
    exit 1
  fi
  pass "required file exists: $1"
}

check_required_files() {
  log "Checking required Phase 1 files"
  local files=(
    "docker-compose.yml"
    "configs/prometheus/prometheus.yml"
    "configs/grafana/datasources/prometheus.yml"
    "configs/grafana/dashboards/dashboard.yml"
    "configs/grafana/dashboards/ecommerce-phase1-overview.json"
    "src/producer/csv_replay_producer.py"
    "src/streaming/bronze_stream.py"
    "src/streaming/silver_stream.py"
    "src/streaming/gold_metrics_stream.py"
    "scripts/smoke_test_fixture.sh"
    "scripts/run_live_replay.sh"
    "tests/fixtures/sample_events.csv"
    "data/README.md"
  )
  for file in "${files[@]}"; do
    require_file "$file"
  done
}

check_static_config() {
  log "Checking static configuration"
  docker compose config >/tmp/phase1-compose-config.txt
  pass "docker compose config"

  python3 -m json.tool configs/grafana/dashboards/ecommerce-phase1-overview.json \
    >/tmp/phase1-dashboard-json-check.json
  pass "Grafana dashboard JSON parses"

  bash -n scripts/smoke_test_fixture.sh
  pass "smoke test shell syntax"

  bash -n scripts/run_live_replay.sh
  pass "live replay shell syntax"

  python3 - <<'PY'
from pathlib import Path
for source in (
    'src/producer/csv_replay_producer.py',
    'src/streaming/bronze_stream.py',
    'src/streaming/silver_stream.py',
    'src/streaming/gold_metrics_stream.py',
):
    path = Path(source)
    compile(path.read_text(), str(path), 'exec')
PY
  pass "Python pipeline sources compile"
}

check_ignore_rules() {
  log "Checking local dataset and runtime ignore rules"
  git check-ignore -q data/2019-Oct.csv
  pass "large Kaggle CSV is ignored"

  git check-ignore -q storage/readiness/phase1_readiness_smoke.log
  pass "smoke runtime logs are ignored"
}

run_smoke_test() {
  log "Running full fixture smoke test"
  mkdir -p "$(dirname "$SMOKE_LOG")"
  ./scripts/smoke_test_fixture.sh | tee "$SMOKE_LOG"

  grep -q 'smoke_test=PASS' "$SMOKE_LOG"
  pass "fixture smoke test passes"

  grep -q 'prometheus_pushgateway_target=up' "$SMOKE_LOG"
  pass "Prometheus scrapes Pushgateway"

  grep -q 'grafana_dashboard_uid=ecommerce-phase1-overview' "$SMOKE_LOG"
  pass "Grafana dashboard is provisioned"
}

check_runtime_apis() {
  log "Checking runtime APIs after smoke test"
  python3 - <<PY
import base64
import json
import urllib.parse
import urllib.request

prometheus_url = '${PROMETHEUS_URL}'
grafana_url = '${GRAFANA_URL}'

query = urllib.parse.urlencode({'query': 'ecommerce_purchase_total'})
prometheus_result = json.loads(
    urllib.request.urlopen(f'{prometheus_url}/api/v1/query?{query}', timeout=5).read()
)
if not prometheus_result['data']['result']:
    raise SystemExit('missing ecommerce_purchase_total in Prometheus')

auth = base64.b64encode(b'admin:admin').decode()
request = urllib.request.Request(grafana_url + '/api/dashboards/uid/ecommerce-phase1-overview')
request.add_header('Authorization', 'Basic ' + auth)
dashboard = json.loads(urllib.request.urlopen(request, timeout=10).read())
if dashboard.get('dashboard', {}).get('title') != 'Ecommerce Phase 1 Overview':
    raise SystemExit('Grafana dashboard title mismatch')
PY
  pass "Prometheus and Grafana API checks"
}

collect_git_summary() {
  git rev-parse --abbrev-ref HEAD
  git rev-parse --short HEAD
}

write_report() {
  log "Writing ${REPORT_PATH}"
  mkdir -p "$(dirname "$REPORT_PATH")"

  local branch commit generated_at
  branch=$(git rev-parse --abbrev-ref HEAD)
  commit=$(git rev-parse --short HEAD)
  generated_at=$(date -u '+%Y-%m-%d %H:%M:%S UTC')

  {
    printf '# Phase 1 MVP Readiness\n\n'
    printf '%s\n' "- Generated at: \`$generated_at\`"
    printf '%s\n' "- Git branch: \`$branch\`"
    printf '%s\n' "- Git commit: \`$commit\`"
    printf '%s\n\n' '- Status: `READY_FOR_PHASE_1_REVIEW`'

    printf '## Verified Scope\n\n'
    printf '%s\n' '- CSV fixture replay into Kafka.'
    printf '%s\n' '- Spark bronze raw parquet storage.'
    printf '%s\n' '- Spark silver parsing, enrichment, validation, and quarantine routing.'
    printf '%s\n' '- Spark gold business and quality metric export to Pushgateway.'
    printf '%s\n' '- Prometheus scrape of Pushgateway metrics.'
    printf '%s\n\n' '- Grafana Prometheus datasource and Phase 1 dashboard provisioning.'

    printf '## Passing Checks\n\n'
    local check
    for check in "${PASSED_CHECKS[@]}"; do
      printf '%s\n' "- $check"
    done
    printf '\n'

    printf '## Smoke Test Evidence\n\n'
    printf 'Expected fixture results are verified by `scripts/smoke_test_fixture.sh`:\n\n'
    printf '```text\n'
    local evidence_pattern
    evidence_pattern='bronze_count=|silver_count=|quarantine_count=|metric_events_'
    evidence_pattern+='|metric_purchase_total=|metric_revenue_total=|metric_quality_rule='
    evidence_pattern+='|prometheus_|grafana_|smoke_test=PASS'
    grep -E "$evidence_pattern" "$SMOKE_LOG" || true
    printf '```\n\n'

    printf '## Remaining Phase 1 Improvements\n\n'
    printf '%s\n' \
      '- Review Grafana panel layout and thresholds in browser.'
    printf '%s\n' '- Add benchmark runs against a larger CSV slice for throughput and freshness numbers.'
    printf '%s\n' '- Decide whether long-running streaming mode needs a separate operational runbook.'
    printf '%s\n\n' '- Keep ClickHouse out of Phase 1; use it in Phase 2 for OLAP comparison.'

    printf '## Phase 2 Entry Criteria\n\n'
    printf '%s\n' '- Phase 1 smoke test remains green.'
    printf '%s\n' '- Grafana dashboard can be opened locally and shows Prometheus-backed panels.'
    printf '%s\n' \
      '- Baseline metric names are stable enough to compare against ClickHouse-backed panels.'
  } >"$REPORT_PATH"

  pass "readiness report written: ${REPORT_PATH}"
}

main() {
  check_required_files
  check_static_config
  check_ignore_rules
  run_smoke_test
  check_runtime_apis
  write_report
  log "phase1_mvp_readiness=PASS"
}

main "$@"
