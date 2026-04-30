#!/usr/bin/env bash
# Omnissa streaming baseline — runs the 9 queries against the omnissa-streaming domain
# with streaming forced on, captures took_ms + wire bytes + circuit-breaker state.
#
# Prereqs: AWS_PROFILE=test-account-admin (IAM Admin in 779035457181), awscurl on PATH.
# Output: benchmarks/omnissa/results/<query>.{json,timing} per query.

set -u

ROOT=/local/home/hvamsi/code/OpenSearch/.claude/worktrees/omnissa-streaming
ENDPOINT=${ENDPOINT:-https://search-omnissa-streaming-cswekj5xnwys7a6abq7qixaysy.eu-west-1.es-staging.amazonaws.com}
INDEX=${INDEX:-pinot_applicaiton_2025_03_v3}
OUT=$ROOT/benchmarks/omnissa/results
WARMUP_RUNS=${WARMUP_RUNS:-1}
TIMED_RUNS=${TIMED_RUNS:-3}
PROFILE=${PROFILE:-test-account-admin}
export AWS_PROFILE="$PROFILE"

mkdir -p "$OUT"

queries=(
  device_count
  nested_agg_low_cardinality
  nested_agg_low_cardinality_max_agg
  nested_agg_high_cardinality
  nested_agg_high_cardinality_max_agg
  multi_term_low_cardinality
  multi_term_low_cardinality_max_agg
  multi_term_high_cardinality
  multi_term_high_cardinality_max_agg
)

echo "=== Cluster preflight ==="
AWS_PROFILE=$PROFILE awscurl --service es --region eu-west-1 -X GET \
  "$ENDPOINT/_cluster/health/$INDEX?pretty" 2>/dev/null | tee "$OUT/_health.json"
status=$(grep -oE '"status" : "[a-z]+"' "$OUT/_health.json" | head -1 | cut -d'"' -f4)
if [[ "$status" == "red" ]]; then
  echo "FATAL: index $INDEX is red" >&2
  exit 1
fi
echo "Index status: $status"
echo

echo "=== Streaming settings ==="
AWS_PROFILE=$PROFILE awscurl --service es --region eu-west-1 -X GET \
  "$ENDPOINT/_cluster/settings?flat_settings=true&include_defaults=true" 2>/dev/null \
  | grep -oE '"(stream\.search\.enabled|search\.aggregations\.streaming\.[a-z_]+)" : "[^"]*"' \
  | sort -u | tee "$OUT/_settings.txt"
echo

max_heap_gb () {
  awscurl --service es --region eu-west-1 -X GET \
    "$ENDPOINT/_nodes/stats/breaker?filter_path=nodes.*.breakers.parent.estimated_size_in_bytes" 2>/dev/null \
    | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
    sizes=[n['breakers']['parent']['estimated_size_in_bytes'] for n in (d.get('nodes') or {}).values() if 'breakers' in n]
    print(f'{max(sizes)/1e9:.2f}' if sizes else 'NA')
except Exception:
    print('NA')
"
}

settle_until_clear () {
  local max_gb=$1 max_waits=${2:-6}
  for ((i=0; i<max_waits; i++)); do
    local cur; cur=$(max_heap_gb)
    if [[ "$cur" != "NA" ]] && awk -v a="$cur" -v b="$max_gb" 'BEGIN{exit !(a<b)}'; then
      return 0
    fi
    sleep 10
  done
  return 1
}

run_query () {
  local name=$1 label=$2
  local body="$ROOT/$name.json"
  local out="$OUT/${name}__${label}.json"
  local tim="$OUT/${name}__${label}.timing"

  [[ -f $body ]] || { echo "SKIP $name (body missing)"; return; }

  local heap_before heap_after
  heap_before=$(max_heap_gb)

  local t0 t1
  t0=$(date +%s%3N)
  awscurl --service es --region eu-west-1 \
    -X POST "$ENDPOINT/$INDEX/_search?request_cache=false" \
    -H 'content-type: application/json' \
    -d "@$body" > "$out" 2> "$tim.err"
  t1=$(date +%s%3N)

  heap_after=$(max_heap_gb)

  local took shardtot shardfail
  took=$(grep -oE '"took":[0-9]+' "$out" | head -1 | cut -d: -f2)
  shardtot=$(grep -oE '"total":[0-9]+' "$out" | head -1 | cut -d: -f2)
  shardfail=$(grep -oE '"failed":[0-9]+' "$out" | head -1 | cut -d: -f2)
  local wall=$((t1 - t0))
  local size; size=$(wc -c < "$out")
  local status="OK"
  grep -q '"error"' "$out" && status="ERR"

  {
    echo "name=$name label=$label"
    echo "wall_ms=$wall"
    echo "took_ms=${took:-NA}"
    echo "shards_total=${shardtot:-NA}"
    echo "shards_failed=${shardfail:-NA}"
    echo "response_bytes=$size"
    echo "heap_before_gb=$heap_before"
    echo "heap_after_gb=$heap_after"
    echo "status=$status"
  } > "$tim"

  printf "  %-45s %s wall=%sms took=%sms fail=%s size=%s heap=%s->%sGB %s\n" \
    "$name" "[$label]" "$wall" "${took:-NA}" "${shardfail:-NA}" "$size" \
    "$heap_before" "$heap_after" "$status"
}

SETTLE_GB=${SETTLE_GB:-15}

echo "=== Warmup ($WARMUP_RUNS run per query), settle<${SETTLE_GB}GB between queries ==="
for name in "${queries[@]}"; do
  settle_until_clear "$SETTLE_GB" || echo "    (heap still >${SETTLE_GB}GB; continuing anyway)"
  for ((w = 0; w < WARMUP_RUNS; w++)); do run_query "$name" "warmup$w"; done
done
echo

echo "=== Timed runs ($TIMED_RUNS per query), settle<${SETTLE_GB}GB between queries ==="
for name in "${queries[@]}"; do
  settle_until_clear "$SETTLE_GB" || echo "    (heap still >${SETTLE_GB}GB; continuing anyway)"
  for ((r = 0; r < TIMED_RUNS; r++)); do run_query "$name" "run$r"; done
done
echo

echo "=== Summary ==="
median () { awk '{a[NR]=$1} END{n=asort(a); print (n%2==1)?a[(n+1)/2]:a[n/2]}'; }

summarize () {
  local name=$1
  local timings=("$OUT"/${name}__run*.timing)
  if [[ ! -f "${timings[0]}" ]]; then printf "%-45s %10s\n" "$name" "NO_DATA"; return; fi
  local tookmed wallmed failany sizemed
  tookmed=$(grep -h ^took_ms= "${timings[@]}" | cut -d= -f2 | median)
  wallmed=$(grep -h ^wall_ms= "${timings[@]}" | cut -d= -f2 | median)
  failany=$(grep -h ^shards_failed= "${timings[@]}" | cut -d= -f2 | sort -n | tail -1)
  sizemed=$(grep -h ^response_bytes= "${timings[@]}" | cut -d= -f2 | median)
  printf "%-45s %10s %10s %10s %10s\n" "$name" "${tookmed:-NA}" "${wallmed:-NA}" "${failany:-NA}" "${sizemed:-NA}"
}

{
  printf "%-45s %10s %10s %10s %10s\n" "query" "took_med" "wall_med" "fail_any" "size_med"
  for name in "${queries[@]}"; do summarize "$name"; done
} | tee "$OUT/_summary.txt"
