#!/usr/bin/env bash
set -euo pipefail

# End-to-end accelerated 1-day simulation against LocalStack (006-T004, SC-001).
DEMO_RECORDS="${DEMO_RECORDS:-1000}"
DEMO_PROVIDERS="${DEMO_PROVIDERS:-20}"
CLOCK_SPEEDUP="${CLOCK_SPEEDUP:-1440}"
SHARDS="${SHARDS:-4}"
AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:4566}"
MOCKAPI_ADDR="${MOCKAPI_ADDR:-127.0.0.1:18090}"

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL

TMPDIR_BIN="$(mktemp -d)"
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill -TERM "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$TMPDIR_BIN"
}
trap cleanup EXIT

echo "==> [1/6] Building binaries (seed, slotter, mockapi, dispatcher, worker, reconciler)..."
go build -o "$TMPDIR_BIN/seed" ./cmd/seed
go build -o "$TMPDIR_BIN/slotter" ./cmd/slotter
go build -o "$TMPDIR_BIN/mockapi" ./cmd/mockapi
go build -o "$TMPDIR_BIN/dispatcher" ./cmd/dispatcher
go build -o "$TMPDIR_BIN/worker" ./cmd/worker
go build -o "$TMPDIR_BIN/reconciler" ./cmd/reconciler

echo "==> [2/6] Seeding ${DEMO_RECORDS} synthetic requests across ${DEMO_PROVIDERS} Zipf providers..."
"$TMPDIR_BIN/seed" \
  --records "$DEMO_RECORDS" \
  --providers "$DEMO_PROVIDERS" \
  --workers 8 \
  --endpoint "$AWS_ENDPOINT_URL"

echo "==> [3/6] Assigning uniform slots across 1440 minutes (cmd/slotter)..."
"$TMPDIR_BIN/slotter" \
  --shards "$SHARDS" \
  --workers 8 \
  --wcu-limit 2000 \
  --endpoint "$AWS_ENDPOINT_URL"

echo "==> [4/6] Starting mockapi (${MOCKAPI_ADDR}), worker, and accelerated dispatcher (${CLOCK_SPEEDUP}x)..."
ADDR="$MOCKAPI_ADDR" DEFAULT_LATENCY_MS=1 "$TMPDIR_BIN/mockapi" >/dev/null 2>&1 &
PIDS+=($!)

for _ in $(seq 1 30); do
  if curl -sf "http://${MOCKAPI_ADDR}/healthz" >/dev/null; then
    break
  fi
  sleep 0.2
done

PROVIDER_API_BASE_URL="http://${MOCKAPI_ADDR}" \
  JOBS_QUEUE_NAME="tapi-jobs.fifo" \
  HEALTH_ADDR="127.0.0.1:18081" \
  INFLIGHT_PER_TASK=32 \
  MAX_TASKS=4 \
  LAMBDA_BUDGET=1000 \
  RECEIVERS=2 \
  "$TMPDIR_BIN/worker" >/dev/null 2>&1 &
PIDS+=($!)

CLOCK_SPEEDUP="$CLOCK_SPEEDUP" \
  JOBS_QUEUE_NAME="tapi-jobs.fifo" \
  HEALTH_ADDR="127.0.0.1:18080" \
  SHARDS="$SHARDS" \
  "$TMPDIR_BIN/dispatcher" >/dev/null 2>&1 &
PIDS+=($!)

echo "==> [5/6] Waiting for simulated 24h day (${DEMO_RECORDS} executions) to complete..."
DEADLINE=$((SECONDS + 240))
TOTAL_CALLS=0
MAX_INFLIGHT=0
DUP_KEYS=0

while [ "$SECONDS" -lt "$DEADLINE" ]; do
  STATS="$(curl -sf "http://${MOCKAPI_ADDR}/stats" || true)"
  if [ -n "$STATS" ]; then
    TOTAL_CALLS="$(printf '%s' "$STATS" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("total_calls",0))')"
    MAX_INFLIGHT="$(printf '%s' "$STATS" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("max_provider_inflight",0))')"
    DUP_KEYS="$(printf '%s' "$STATS" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("duplicate_idem_keys",0))')"
    if [ "$TOTAL_CALLS" -ge "$DEMO_RECORDS" ]; then
      break
    fi
  fi
  sleep 2
done

echo "==> [6/6] Running daily reconciler & checking invariants..."
TODAY="$(date -u +%Y-%m-%d)"
"$TMPDIR_BIN/reconciler" \
  --date "$TODAY" \
  --shards "$SHARDS" \
  --parallelism 32 \
  --redispatch=false \
  --endpoint "$AWS_ENDPOINT_URL" >/dev/null

echo ""
echo "================ DEMO SUMMARY ================"
echo "Seeded records         : ${DEMO_RECORDS}"
echo "Executed API calls     : ${TOTAL_CALLS}"
echo "Max inflight / provider: ${MAX_INFLIGHT} (invariant IV.1 requires <= 1)"
echo "Duplicate idem keys    : ${DUP_KEYS} (invariant IV.2 requires 0)"
echo "=============================================="

if [ "$TOTAL_CALLS" -lt "$DEMO_RECORDS" ]; then
  echo "ERROR: Demo did not complete all ${DEMO_RECORDS} records (got ${TOTAL_CALLS})" >&2
  exit 1
fi
if [ "$MAX_INFLIGHT" -ne 1 ]; then
  echo "ERROR: Invariant IV.1 violated! max_provider_inflight=${MAX_INFLIGHT}" >&2
  exit 1
fi
if [ "$DUP_KEYS" -ne 0 ]; then
  echo "ERROR: Invariant IV.2 violated! duplicate_idem_keys=${DUP_KEYS}" >&2
  exit 1
fi

echo "DEMO PASSED: 100% executed, max_inflight/provider=1, 0 duplicate calls!"
