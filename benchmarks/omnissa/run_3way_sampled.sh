#!/usr/bin/env bash
# 3-way sampled benchmark: baseline / streaming (varbinary) / columnar.
# Each config: apply settings, clear caches cluster-wide, full-cluster GC,
# start per-node 1 s jcmd heap_info sampler, run OSB, pull samples, stop sampler.
set -uo pipefail

ENDPOINT=https://search-omnissa-streaming-cswekj5xnwys7a6abq7qixaysy.eu-west-1.es-staging.amazonaws.com
TARGET_HOST=search-omnissa-streaming-cswekj5xnwys7a6abq7qixaysy.eu-west-1.es-staging.amazonaws.com:443
WORKLOAD_DIR=/home/hvamsi/code/opensearch-benchmark-workloads/streaming_workload
OSB=/home/hvamsi/.venvs/osb-sdg/bin/opensearch-benchmark
INSTANCES_FILE=${INSTANCES_FILE:-/tmp/instance_ids.txt}
SETTINGS_IID=${SETTINGS_IID:-i-0dd889a195c698458}

TS=$(date +%s)
OUT=${OUT:-/local/home/hvamsi/code/OpenSearch/.claude/worktrees/omnissa-streaming/benchmarks/omnissa/results/threeway_sampled_$TS}
mkdir -p "$OUT"
echo "OUT=$OUT"

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

fanout_gc() {
  for iid in $(cat "$INSTANCES_FILE"); do
    (timeout 90 yes y | timeout 120 tumbler --bypass dub-beta aes domain dp node shell \
      -D 779035457181:omnissa-streaming --instance-id "$iid" \
      -b su -a "root -c \"BASE=\\\$(ls -d /apollo/_env/swift-eu-west-1-staging-OS_3_5AMI-ES2-p*_OS_35 2>/dev/null | head -1); PID=\\\$(pgrep -f \\\"\\\$BASE/jdk-21/bin/java\\\" | head -1); [ -z \\\"\\\$PID\\\" ] || \\\$BASE/jdk-21/bin/jcmd \\\$PID GC.run\"" > /dev/null 2>&1) &
  done
  wait
}

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
  curl -sku "$auth" "$ENDPOINT/_nodes/stats/jvm,indices,breaker?filter_path=nodes.*.name,nodes.*.roles,nodes.*.jvm.mem.heap_used_in_bytes,nodes.*.jvm.mem.heap_used_percent,nodes.*.indices.fielddata.memory_size_in_bytes,nodes.*.breakers.parent.estimated_size_in_bytes" 2>&1 > "$1"
}

run_config() {
  local name="$1"
  local settings="$2"
  echo ""
  echo "=========================================="
  echo "CONFIG: $name"
  echo "=========================================="
  local cfg_out="$OUT/$name"
  mkdir -p "$cfg_out"

  echo "[$(date -u +%H:%M:%S)] settings..."
  apply_settings "$settings" | tee "$cfg_out/settings_applied.json"

  echo "[$(date -u +%H:%M:%S)] cache clear + GC..."
  fanout_cache_clear
  fanout_gc
  sleep 3

  heap_snapshot "$cfg_out/heap_pre.json"
  echo "pre-run: $(python3 -c "import json;d=json.load(open('$cfg_out/heap_pre.json'));t=sum(n['jvm']['mem']['heap_used_in_bytes'] for n in d['nodes'].values() if 'data' in n.get('roles',[]));print(f'{t/1e9:.1f} GB')")"

  echo "[$(date -u +%H:%M:%S)] starting sampler..."
  start_sampler

  echo "[$(date -u +%H:%M:%S)] running OSB..."
  local t0=$SECONDS
  "$OSB" run \
    --pipeline=benchmark-only \
    --target-hosts="$TARGET_HOST" \
    --workload-path="$WORKLOAD_DIR" \
    --test-procedure=all \
    --client-options='use_ssl:true,verify_certs:false,basic_auth_user:Admin,basic_auth_password:Admin@123,timeout:300' \
    --kill-running-processes \
    --user-tag="config:$name" \
    --results-file="$cfg_out/osb_results.txt" \
    > "$cfg_out/osb.log" 2>&1 || true
  local dt=$((SECONDS - t0))
  echo "[$(date -u +%H:%M:%S)] OSB done in ${dt}s"

  local tid
  tid=$(grep -oE '\[Test Run ID\]: [0-9a-f-]+' "$cfg_out/osb.log" | awk '{print $NF}' | head -1)
  echo "test_run_id=$tid"
  # OSB 2.x writes test_run.json asynchronously after the actor system shuts down. Poll until
  # op_metrics is populated (or bail after 60 s).
  if [ -n "$tid" ]; then
    for i in $(seq 1 60); do
      if [ -f "/home/hvamsi/.benchmark/benchmarks/test-runs/$tid/test_run.json" ]; then
        n=$(python3 -c "import json; d=json.load(open('/home/hvamsi/.benchmark/benchmarks/test-runs/$tid/test_run.json')); print(len(d.get('results',{}).get('op_metrics',[])))" 2>/dev/null || echo 0)
        if [ "$n" -gt 0 ]; then break; fi
      fi
      sleep 1
    done
    cp -r "/home/hvamsi/.benchmark/benchmarks/test-runs/$tid" "$cfg_out/test_run" 2>/dev/null || true
  fi

  echo "[$(date -u +%H:%M:%S)] pulling samples..."
  pull_samples "$cfg_out/heap_info"
  stop_sampler

  heap_snapshot "$cfg_out/heap_post.json"
  echo "post-run: $(python3 -c "import json;d=json.load(open('$cfg_out/heap_post.json'));t=sum(n['jvm']['mem']['heap_used_in_bytes'] for n in d['nodes'].values() if 'data' in n.get('roles',[]));print(f'{t/1e9:.1f} GB')")"
}

if [ ! -f "$INSTANCES_FILE" ]; then echo "ERROR: $INSTANCES_FILE not found"; exit 1; fi

run_config baseline  '{"persistent":{"stream.search.enabled":false}}'
run_config streaming '{"persistent":{"stream.search.enabled":true,"search.aggregations.streaming.arrow_columnar.enabled":false}}'
run_config columnar  '{"persistent":{"stream.search.enabled":true,"search.aggregations.streaming.arrow_columnar.enabled":true}}'

echo ""
echo "=========================================="
echo "All configs done: $OUT"
echo "=========================================="
