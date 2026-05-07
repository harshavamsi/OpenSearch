#!/usr/bin/env bash
# Full-suite 2-way A/B (baseline vs streaming+columnar) on the streaming_workload.
# Runs each query in its own OSB invocation with a cache-clear + fleet-wide GC between
# queries, so heap is always near-clean at query start. Captures per-query latency,
# errors, and pre/post heap snapshots.
set -uo pipefail

ENDPOINT=https://search-omnissa-streaming-cswekj5xnwys7a6abq7qixaysy.eu-west-1.es-staging.amazonaws.com
TARGET_HOST=search-omnissa-streaming-cswekj5xnwys7a6abq7qixaysy.eu-west-1.es-staging.amazonaws.com:443
WORKLOAD_DIR=/home/hvamsi/code/opensearch-benchmark-workloads/streaming_workload
OSB=/home/hvamsi/.venvs/osb-sdg/bin/opensearch-benchmark
INSTANCES_FILE=${INSTANCES_FILE:-/tmp/instance_ids.txt}
SETTINGS_IID=${SETTINGS_IID:-i-0dd889a195c698458}

TS=$(date +%s)
OUT=${OUT:-/local/home/hvamsi/code/OpenSearch/.claude/worktrees/omnissa-streaming/benchmarks/omnissa/results/full_2way_$TS}
mkdir -p "$OUT"
echo "OUT=$OUT"

# Query list in cheap-to-expensive order. Keep this in sync with the test procedure so a
# later reduce doesn't need to join by both sources.
QUERIES=(
  cbp_nested_user_card
  cbp_tcc_urls_over_5
  cbp_fm_urls_over_5
  cbp_multi_terms_deferred
  cb_q08_advengine_terms
  cb_q13_phrase_terms
  cb_q16_top_users
  cb_q09_region_users
  cb_q11_mobile_users
  cb_q_heavy_nested_urls
  om_nested_low_card_tcc
  om_nested_high_card_tcc
  om_nested_high_card_fm
  om_multi_term_high_card_deferred
  om_multi_term_low_card
  om_multi_term_low_card_max
  om_multi_term_high_card
  om_multi_term_high_card_max
)

auth="Admin:Admin@123"

apply_settings() {
  local payload="$1"
  local b64
  b64=$(echo -n "$payload" | base64 -w0)
  yes y | timeout 120 tumbler --bypass dub-beta aes domain dp node shell \
    -D 779035457181:omnissa-streaming --instance-id "$SETTINGS_IID" \
    -b su -a "root -c \"echo $b64 | base64 -d > /tmp/s.json && curl -s -XPUT http://localhost:9200/_cluster/settings -H 'content-type: application/json' -d @/tmp/s.json\"" 2>/dev/null \
    | tail -1
}

fanout_cache_clear() {
  for iid in $(cat "$INSTANCES_FILE"); do
    (timeout 90 yes y | timeout 120 tumbler --bypass dub-beta aes domain dp node shell \
      -D 779035457181:omnissa-streaming --instance-id "$iid" \
      -b su -a "root -c \"curl -s -XPOST 'http://localhost:9200/_cache/clear?fielddata=true&query=true&request=true' > /dev/null\"" > /dev/null 2>&1) &
  done
  wait
}

# Fleet-wide GC via jcmd (one round). Drops most transient heap; we do two rounds for
# full effect since some references survive a single young-gen cycle.
fanout_gc() {
  for round in 1 2; do
    for iid in $(cat "$INSTANCES_FILE"); do
      (timeout 90 yes y | timeout 120 tumbler --bypass dub-beta aes domain dp node shell \
        -D 779035457181:omnissa-streaming --instance-id "$iid" \
        -b su -a "root -c \"BASE=\\\$(ls -d /apollo/_env/swift-eu-west-1-staging-OS_3_5AMI-ES2-p*_OS_35 2>/dev/null | head -1); PID=\\\$(pgrep -f \\\"\\\$BASE/jdk-21/bin/java\\\" | head -1); [ -z \\\"\\\$PID\\\" ] || \\\$BASE/jdk-21/bin/jcmd \\\$PID GC.run\"" > /dev/null 2>&1) &
    done
    wait
  done
}

# Per-node background sampler: once per second for 900s max, log GC.heap_info to
# /tmp/heap_info.out on each data node. Started before each query, stopped after.
# Output shows per-sample committed/used/capacity for young+old, so the peak during
# the query run is recoverable even if it fell by the post-heap snapshot.
start_sampler() {
  for iid in $(cat "$INSTANCES_FILE"); do
    (timeout 90 yes y | timeout 120 tumbler --bypass dub-beta aes domain dp node shell \
      -D 779035457181:omnissa-streaming --instance-id "$iid" \
      -b su -a "root -c \"BASE=\\\$(ls -d /apollo/_env/swift-eu-west-1-staging-OS_3_5AMI-ES2-p*_OS_35 2>/dev/null | head -1); PID=\\\$(pgrep -f \\\"\\\$BASE/jdk-21/bin/java\\\" | head -1); rm -f /tmp/heap_info.out; nohup bash -c 'for i in \\\$(seq 1 900); do echo === \\\$(date +%s) ===; \\\$0 \\\$1 GC.heap_info 2>&1; sleep 1; done' \\\$BASE/jdk-21/bin/jcmd \\\$PID > /tmp/heap_info.out 2>&1 &\"" > /dev/null 2>&1) &
  done
  wait
}

stop_sampler() {
  for iid in $(cat "$INSTANCES_FILE"); do
    (timeout 90 yes y | timeout 120 tumbler --bypass dub-beta aes domain dp node shell \
      -D 779035457181:omnissa-streaming --instance-id "$iid" \
      -b su -a "root -c \"pkill -f 'GC.heap_info' 2>/dev/null; pkill -f 'for i in \\\$(seq 1 900)' 2>/dev/null; true\"" > /dev/null 2>&1) &
  done
  wait
}

pull_samples() {
  local target="$1"
  mkdir -p "$target"
  for iid in $(cat "$INSTANCES_FILE"); do
    (timeout 90 yes y | timeout 120 tumbler --bypass dub-beta aes domain dp node shell \
      -D 779035457181:omnissa-streaming --instance-id "$iid" \
      -b su -a "root -c \"cat /tmp/heap_info.out\"" 2>/dev/null > "$target/$iid.log") &
  done
  wait
}

heap_snapshot() {
  curl -sku "$auth" "$ENDPOINT/_nodes/stats/jvm?filter_path=nodes.*.name,nodes.*.roles,nodes.*.jvm.mem.heap_used_in_bytes,nodes.*.jvm.mem.heap_used_percent" 2>&1 > "$1"
}

data_heap_gb() {
  python3 -c "
import json
d = json.load(open('$1'))
t = sum(n['jvm']['mem']['heap_used_in_bytes'] for n in d['nodes'].values() if 'data' in n.get('roles',[]))
print(f'{t/1e9:.1f}')
"
}

run_one_query() {
  local cfg="$1"
  local q="$2"
  local cfg_out="$OUT/$cfg"
  mkdir -p "$cfg_out/$q"

  echo "  [$(date -u +%H:%M:%S)] $q — cache clear + 2x GC..."
  fanout_cache_clear
  fanout_gc
  sleep 3

  heap_snapshot "$cfg_out/$q/heap_pre.json"
  local pre
  pre=$(data_heap_gb "$cfg_out/$q/heap_pre.json")
  echo "  [$(date -u +%H:%M:%S)] $q — pre-heap: ${pre} GB; starting sampler..."
  start_sampler

  echo "  [$(date -u +%H:%M:%S)] $q — running OSB..."
  local t0=$SECONDS
  "$OSB" run \
    --pipeline=benchmark-only \
    --target-hosts="$TARGET_HOST" \
    --workload-path="$WORKLOAD_DIR" \
    --test-procedure=all \
    --include-tasks="$q" \
    --client-options='use_ssl:true,verify_certs:false,basic_auth_user:Admin,basic_auth_password:Admin@123,timeout:600' \
    --kill-running-processes \
    --user-tag="config:$cfg,q:$q" \
    --results-file="$cfg_out/$q/osb_results.txt" \
    > "$cfg_out/$q/osb.log" 2>&1 || true
  local dt=$((SECONDS - t0))

  local tid
  tid=$(grep -oE '\[Test Run ID\]: [0-9a-f-]+' "$cfg_out/$q/osb.log" | awk '{print $NF}' | head -1)
  if [ -n "$tid" ]; then
    # OSB writes test_run.json async after actor shutdown; poll briefly.
    for i in $(seq 1 45); do
      if [ -f "/home/hvamsi/.benchmark/benchmarks/test-runs/$tid/test_run.json" ]; then
        n=$(python3 -c "import json; d=json.load(open('/home/hvamsi/.benchmark/benchmarks/test-runs/$tid/test_run.json')); print(len(d.get('results',{}).get('op_metrics',[])))" 2>/dev/null || echo 0)
        if [ "$n" -gt 0 ]; then break; fi
      fi
      sleep 1
    done
    cp -r "/home/hvamsi/.benchmark/benchmarks/test-runs/$tid" "$cfg_out/$q/test_run" 2>/dev/null || true
  fi

  # Pull sampler output before the buffer gets overwritten on next query's start_sampler.
  pull_samples "$cfg_out/$q/heap_info"
  stop_sampler

  heap_snapshot "$cfg_out/$q/heap_post.json"
  local post
  post=$(data_heap_gb "$cfg_out/$q/heap_post.json")

  # Peak data-node heap from sampler logs. G1GC output line looks like:
  #   "garbage-first heap   total 31981568K, used 617287K [...]"
  # Per timestamp we sum 'used' across all data-node sampler logs, then take the max.
  local peak
  peak=$(python3 -c "
import re, glob
buckets = {}
for f in glob.glob('$cfg_out/$q/heap_info/*.log'):
    ts = None
    for line in open(f):
        m = re.match(r'=== (\d+) ===', line)
        if m:
            ts = int(m.group(1))
            continue
        mm = re.search(r'garbage-first heap\s+total\s+\d+K,\s+used\s+(\d+)K', line)
        if mm and ts is not None:
            buckets[ts] = buckets.get(ts, 0) + int(mm.group(1)) * 1024
peak = max(buckets.values()) if buckets else 0
print(f'{peak/1e9:.1f}')
" 2>/dev/null || echo "?")

  echo "  [$(date -u +%H:%M:%S)] $q — done in ${dt}s; pre=${pre}GB post=${post}GB peak=${peak}GB; tid=$tid"
}

run_config() {
  local cfg="$1"
  local settings="$2"
  echo ""
  echo "=========================================="
  echo "CONFIG: $cfg"
  echo "=========================================="
  mkdir -p "$OUT/$cfg"
  echo "[$(date -u +%H:%M:%S)] settings: $settings"
  apply_settings "$settings" | tee "$OUT/$cfg/settings_applied.json"
  sleep 2

  for q in "${QUERIES[@]}"; do
    run_one_query "$cfg" "$q"
  done
}

if [ ! -f "$INSTANCES_FILE" ]; then echo "ERROR: $INSTANCES_FILE not found"; exit 1; fi

run_config baseline '{"persistent":{"stream.search.enabled":false}}'
run_config columnar '{"persistent":{"stream.search.enabled":true,"search.aggregations.streaming.arrow_columnar.enabled":true}}'

echo ""
echo "=========================================="
echo "All done: $OUT"
echo "=========================================="
