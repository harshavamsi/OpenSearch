#!/usr/bin/env bash
# Runs streaming_workload under three configurations, with full-cluster GC + heap
# capture between configs.
#
# Configs:
#   baseline   - stream.search.enabled=false
#   streaming  - stream.search.enabled=true, arrow_columnar=false
#   columnar   - stream.search.enabled=true, arrow_columnar=true
#
# Per config:
#   1. Apply cluster settings via SSM->localhost on one master (curl can't hit _cluster/settings through gateway).
#   2. Fan out jcmd GC.run to every data node in parallel (see CLAUDE.md).
#   3. Snapshot _nodes/stats/jvm heap for each data node (pre-run).
#   4. Run OSB `run --test-procedure=all`, save test-execution.json path.
#   5. Snapshot heap again (post-run).
#
# Output: results/streaming_workload_<ts>/{baseline,streaming,columnar}/{ osb.log, heap_pre.json, heap_post.json }
set -uo pipefail
# Don't `set -e` — tumbler / curl exit non-zero on transient conditions; we want
# the script to keep going and record whatever it saw.

ENDPOINT=https://search-omnissa-streaming-cswekj5xnwys7a6abq7qixaysy.eu-west-1.es-staging.amazonaws.com
TARGET_HOST="search-omnissa-streaming-cswekj5xnwys7a6abq7qixaysy.eu-west-1.es-staging.amazonaws.com:443"
WORKLOAD_DIR=/home/hvamsi/code/opensearch-benchmark-workloads/streaming_workload
OSB=/home/hvamsi/.venvs/osb-sdg/bin/opensearch-benchmark
TUMBLER_INSTANCE=${TUMBLER_INSTANCE:-i-0dd889a195c698458}  # any data node — used only for the localhost settings PUT
INSTANCES_FILE=${INSTANCES_FILE:-/tmp/instance_ids.txt}

TS=$(date +%s)
OUT=${OUT:-/local/home/hvamsi/code/OpenSearch/.claude/worktrees/omnissa-streaming/benchmarks/omnissa/results/streaming_workload_$TS}
mkdir -p "$OUT"

auth="Admin:Admin@123"

apply_settings() {
  local payload="$1"
  local b64
  b64=$(echo -n "$payload" | base64 -w0)
  yes y | tumbler --bypass dub-beta aes domain dp node shell \
    -D 779035457181:omnissa-streaming --instance-id "$TUMBLER_INSTANCE" \
    -b su -a "root -c \"echo $b64 | base64 -d > /tmp/s.json && curl -s -XPUT http://localhost:9200/_cluster/settings -H 'content-type: application/json' -d @/tmp/s.json\"" 2>/dev/null \
    | tail -1
}

full_cluster_gc() {
  # Fan out jcmd GC.run in parallel to every data node (CLAUDE.md recipe).
  local instances=()
  while IFS= read -r iid; do instances+=("$iid"); done < "$INSTANCES_FILE"
  for iid in "${instances[@]}"; do
    (yes y | tumbler --bypass dub-beta aes domain dp node shell \
      -D 779035457181:omnissa-streaming --instance-id "$iid" \
      -b su -a "root -c \"BASE=\\\$(ls -d /apollo/_env/swift-eu-west-1-staging-OS_3_5AMI-ES2-p*_OS_35 2>/dev/null | head -1); PID=\\\$(pgrep -f \\\"\\\$BASE/jdk-21/bin/java\\\" | head -1); [ -z \\\"\\\$PID\\\" ] || \\\$BASE/jdk-21/bin/jcmd \\\$PID GC.run\"" > /tmp/gc_$iid.log 2>&1) &
  done
  wait
}

heap_snapshot() {
  local outfile="$1"
  curl -sku "$auth" "$ENDPOINT/_nodes/stats/jvm?filter_path=nodes.*.name,nodes.*.jvm.mem.heap_used_in_bytes,nodes.*.jvm.mem.heap_used_percent,nodes.*.roles" 2>&1 > "$outfile"
}

run_config() {
  local name="$1"
  local settings="$2"
  echo "==============================================="
  echo "CONFIG: $name"
  echo "settings: $settings"
  echo "==============================================="
  local cfg_out="$OUT/$name"
  mkdir -p "$cfg_out"

  echo "[$(date -u +%H:%M:%S)] Applying settings..."
  apply_settings "$settings" | tee "$cfg_out/settings_applied.json"

  echo "[$(date -u +%H:%M:%S)] Fan-out GC.run on all data nodes..."
  full_cluster_gc
  # Quick settle
  sleep 3

  echo "[$(date -u +%H:%M:%S)] Heap snapshot pre-run..."
  heap_snapshot "$cfg_out/heap_pre.json"

  local test_exec_id
  echo "[$(date -u +%H:%M:%S)] Running OSB..."
  local osb_start=$SECONDS
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
  local osb_elapsed=$((SECONDS - osb_start))
  echo "[$(date -u +%H:%M:%S)] OSB finished in ${osb_elapsed}s"

  # Grab the test-execution uuid from the log and copy its JSON
  test_exec_id=$(grep -oE '\[Test Run ID\]: [0-9a-f-]+' "$cfg_out/osb.log" | awk '{print $NF}' | head -1)
  echo "test_exec_id=$test_exec_id"
  if [ -n "$test_exec_id" ]; then
    cp -r "/home/hvamsi/.osb/benchmarks/test_executions/$test_exec_id" "$cfg_out/test_execution" 2>/dev/null || true
  fi

  echo "[$(date -u +%H:%M:%S)] Heap snapshot post-run..."
  heap_snapshot "$cfg_out/heap_post.json"

  echo "[$(date -u +%H:%M:%S)] $name done -> $cfg_out"
  echo ""
}

echo "Output dir: $OUT"
echo ""

# Make sure we have data-node instance IDs only (exclude masters).
if [ ! -f "$INSTANCES_FILE" ]; then
  echo "ERROR: $INSTANCES_FILE not found"; exit 1
fi

run_config baseline   '{"persistent":{"stream.search.enabled":false}}'
run_config streaming  '{"persistent":{"stream.search.enabled":true,"search.aggregations.streaming.arrow_columnar.enabled":false}}'
run_config columnar   '{"persistent":{"stream.search.enabled":true,"search.aggregations.streaming.arrow_columnar.enabled":true}}'

echo "==============================================="
echo "All configs done."
echo "Results: $OUT"
echo "==============================================="
