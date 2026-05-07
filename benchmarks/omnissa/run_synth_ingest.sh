#!/usr/bin/env bash
# Loop: generate a ~240 GB NDJSON slice of clickbench_plus, ingest into the
# omnissa-streaming cluster, delete the slice, repeat until the target on-disk
# size is reached (or MAX_ITERS).
set -euo pipefail

TOTAL_GB=${TOTAL_GB:-1920}
SLICE_GB=${SLICE_GB:-240}
MAX_ITERS=${MAX_ITERS:-30}

ENDPOINT=https://search-omnissa-streaming-cswekj5xnwys7a6abq7qixaysy.eu-west-1.es-staging.amazonaws.com
TARGET_HOST="search-omnissa-streaming-cswekj5xnwys7a6abq7qixaysy.eu-west-1.es-staging.amazonaws.com:443"
WORKLOAD_DIR=/home/hvamsi/code/opensearch-benchmark-workloads/clickbench_plus
MODULE=/local/home/hvamsi/code/OpenSearch/.claude/worktrees/omnissa-streaming/benchmarks/omnissa/clickbench_plus_generator.py
CONFIG=/local/home/hvamsi/code/OpenSearch/.claude/worktrees/omnissa-streaming/benchmarks/omnissa/sdg-clickbench_plus.yml
SLICE_ROOT=${SLICE_ROOT:-/tmp/sdg_slice}
OSB=/home/hvamsi/.venvs/osb-sdg/bin/opensearch-benchmark

LOG_DIR=${LOG_DIR:-/local/home/hvamsi/code/OpenSearch/.claude/worktrees/omnissa-streaming/benchmarks/omnissa/results/synth_ingest}
mkdir -p "$LOG_DIR"

for i in $(seq 1 "$MAX_ITERS"); do
  ITER_LOG="$LOG_DIR/iter_$(printf '%02d' "$i").log"
  echo "=== iter $i: generating ${SLICE_GB} GB NDJSON → $SLICE_ROOT ===" | tee -a "$ITER_LOG"
  rm -rf "$SLICE_ROOT"
  mkdir -p "$SLICE_ROOT"
  "$OSB" generate-data \
    --custom-module="$MODULE" \
    --custom-config="$CONFIG" \
    --total-size="$SLICE_GB" \
    --index-name=clickbench_plus \
    --output-path="$SLICE_ROOT" 2>&1 | tee -a "$ITER_LOG"

  echo "=== iter $i: ingesting slice ===" | tee -a "$ITER_LOG"
  # SDG writes clickbench_plus_0.json, clickbench_plus_1.json, etc. and a
  # clickbench_plus_record.json metadata file (skip the record file).
  for FILE in "$SLICE_ROOT"/clickbench_plus_*.json; do
    [ -f "$FILE" ] || continue
    case "$FILE" in *_record.json) continue ;; esac
    DOCS=$(wc -l < "$FILE")
    BYTES=$(stat -c %s "$FILE")
    echo "--- ingesting $FILE ($DOCS docs, $BYTES bytes) ---" | tee -a "$ITER_LOG"

    PARAMS=$(cat <<EOF
{
  "number_of_shards": 64,
  "number_of_replicas": 0,
  "index_name": "clickbench_plus",
  "bulk_indexing_clients": 14,
  "bulk_size": 5000,
  "source_file": "$FILE",
  "document_count": $DOCS,
  "uncompressed_bytes": $BYTES
}
EOF
)

    "$OSB" run \
      --pipeline=benchmark-only \
      --target-hosts="$TARGET_HOST" \
      --workload-path="$WORKLOAD_DIR" \
      --test-procedure=append-only \
      --workload-params="$PARAMS" \
      --client-options='use_ssl:true,verify_certs:false,basic_auth_user:Admin,basic_auth_password:Admin@123' \
      --kill-running-processes 2>&1 | tee -a "$ITER_LOG"
  done

  rm -rf "$SLICE_ROOT"

  SIZE_RAW=$(curl -sku Admin:Admin@123 "$ENDPOINT/_cat/indices/clickbench_plus?h=pri.store.size&bytes=gb" | tr -d '[:space:]')
  echo "=== iter $i: clickbench_plus pri.store.size = ${SIZE_RAW} GB ===" | tee -a "$ITER_LOG"
  if [[ "$SIZE_RAW" =~ ^[0-9]+$ ]] && [ "$SIZE_RAW" -ge "$TOTAL_GB" ]; then
    echo "Target ${TOTAL_GB} GB reached; exiting loop."
    break
  fi
done
