#!/usr/bin/env bash
# 4-way FSST A/B: {pinot classic, pinot fsst, classic+streaming; cbp classic, cbp fsst, classic+streaming}
# Uses the 12 workload queries, templated to swap index names.
set -uo pipefail
ENDPOINT=https://search-omnissa-streaming-cswekj5xnwys7a6abq7qixaysy.eu-west-1.es-staging.amazonaws.com
INSTANCES_FILE=${INSTANCES_FILE:-/tmp/instance_ids.txt}
SETTINGS_IID=${SETTINGS_IID:-i-0dd889a195c698458}

TS=$(date +%s)
OUT=/local/home/hvamsi/code/OpenSearch/.claude/worktrees/omnissa-streaming/benchmarks/omnissa/results/fsst_ab_${TS}
mkdir -p "$OUT/bodies"
echo "OUT=$OUT"

apply_settings() {
  local b64=$(echo -n "$1" | base64 -w0)
  yes y | timeout 120 tumbler --bypass dub-beta aes domain dp node shell \
    -D 779035457181:omnissa-streaming --instance-id "$SETTINGS_IID" \
    -b su -a "root -c \"echo $b64 | base64 -d > /tmp/s.json && curl -s -XPUT http://localhost:9200/_cluster/settings -H 'content-type: application/json' -d @/tmp/s.json\"" 2>/dev/null | tail -1
}

fanout_gc() {
  for round in 1 2; do
    for iid in $(cat "$INSTANCES_FILE"); do
      (yes y | timeout 120 tumbler --bypass dub-beta aes domain dp node shell \
        -D 779035457181:omnissa-streaming --instance-id "$iid" \
        -b su -a "root -c \"BASE=\\\$(ls -d /apollo/_env/swift-eu-west-1-staging-OS_3_5AMI-ES2-p*_OS_35 2>/dev/null | head -1); PID=\\\$(pgrep -f \\\"\\\$BASE/jdk-21/bin/java\\\" | head -1); [ -z \\\"\\\$PID\\\" ] || \\\$BASE/jdk-21/bin/jcmd \\\$PID GC.run\"" > /dev/null 2>&1) &
    done
    wait
  done
}

# ============================================================================
# Query bodies — per-index-family, parameterized on index name placeholder __IDX__
# The "family" prefix (om_ or cbp_) determines which index var to substitute.
# ============================================================================

# Omnissa family: filter on org_id
cat > $OUT/bodies/om_multi_term_low_card.json <<'J'
{"size":0,"query":{"bool":{"filter":[{"terms":{"org_id":["538f619e-2db4-4f07-974b-efb3e5326116"]}}]}},"aggs":{"all_buckets":{"multi_terms":{"terms":[{"field":"app_package_id"},{"field":"device_platform"}],"order":[{"app_count":"desc"}],"size":500},"aggs":{"app_count":{"cardinality":{"field":"device_guid__app_package_id","execution_hint":"deferred_ordinals"}}}}}}
J
cat > $OUT/bodies/om_multi_term_high_card.json <<'J'
{"size":0,"query":{"bool":{"filter":[{"terms":{"org_id":["538f619e-2db4-4f07-974b-efb3e5326116"]}}]}},"aggs":{"all_buckets":{"multi_terms":{"terms":[{"field":"device_guid"},{"field":"device_platform"}],"order":[{"app_count":"desc"}],"size":500},"aggs":{"app_count":{"cardinality":{"field":"device_guid__app_package_id","execution_hint":"deferred_ordinals"}}}}}}
J
cat > $OUT/bodies/om_multi_term_low_card_max.json <<'J'
{"size":0,"query":{"bool":{"filter":[{"terms":{"org_id":["538f619e-2db4-4f07-974b-efb3e5326116"]}}]}},"aggs":{"all_buckets":{"multi_terms":{"terms":[{"field":"app_package_id"},{"field":"device_platform"}],"order":[{"max_date":"desc"}],"size":500},"aggs":{"max_date":{"max":{"field":"app_creation_date"}}}}}}
J
cat > $OUT/bodies/om_multi_term_high_card_max.json <<'J'
{"size":0,"query":{"bool":{"filter":[{"terms":{"org_id":["538f619e-2db4-4f07-974b-efb3e5326116"]}}]}},"aggs":{"all_buckets":{"multi_terms":{"terms":[{"field":"device_guid"},{"field":"device_platform"}],"order":[{"max_date":"desc"}],"size":500},"aggs":{"max_date":{"max":{"field":"app_creation_date"}}}}}}
J
cat > $OUT/bodies/om_fm_high_card_cardinality.json <<'J'
{"size":0,"query":{"term":{"org_id":{"value":"538f619e-2db4-4f07-974b-efb3e5326116"}}},"aggregations":{"by_platform":{"terms":{"field":"device_platform","size":10},"aggs":{"devices_over_10":{"filtered_metric":{"buckets":{"terms":{"field":"device_guid"}},"metric":{"cardinality":{"field":"device_guid__app_package_id"}},"filter":{"gt":10},"execution_hint":"dfs","shard_min_doc_count":5}}}}}
J
cat > $OUT/bodies/om_fm_low_card_cardinality.json <<'J'
{"size":0,"query":{"term":{"org_id":{"value":"538f619e-2db4-4f07-974b-efb3e5326116"}}},"aggregations":{"by_platform":{"terms":{"field":"device_platform","size":10},"aggs":{"apps_over_0":{"filtered_metric":{"buckets":{"terms":{"field":"app_package_id"}},"metric":{"cardinality":{"field":"device_guid__app_package_id"}},"filter":{"gt":0},"execution_hint":"dfs"}}}}}
J

# Clickbench_plus family
cat > $OUT/bodies/cbp_heavy_nested_default.json <<'J'
{"size":0,"timeout":"5m","aggregations":{"urls":{"terms":{"field":"URL","size":10000},"aggregations":{"distinct_referers":{"cardinality":{"field":"Referer"}}}}}}
J
cat > $OUT/bodies/cbp_q23_title_search_cardinality.json <<'J'
{"size":0,"query":{"exists":{"field":"SearchPhrase"}},"aggregations":{"t":{"terms":{"field":"Title","size":10},"aggregations":{"u":{"cardinality":{"field":"UserID"}},"c":{"value_count":{"field":"_index"}}}}}}
J
cat > $OUT/bodies/cbp_nested_user_card.json <<'J'
{"size":0,"aggregations":{"by_counter":{"terms":{"field":"CounterID","size":1000},"aggs":{"unique_users":{"cardinality":{"field":"UserID"}}}}}}
J
cat > $OUT/bodies/cbp_multi_terms_deferred.json <<'J'
{"size":0,"aggregations":{"combos":{"multi_terms":{"terms":[{"field":"CounterID"},{"field":"SearchEngineID"}],"size":200,"order":[{"unique_users":"desc"}]},"aggregations":{"unique_users":{"cardinality":{"field":"UserID","execution_hint":"deferred_ordinals"}}}}}}
J
cat > $OUT/bodies/cbp_q33_watch_client_all.json <<'J'
{"size":0,"aggregations":{"k":{"multi_terms":{"terms":[{"field":"WatchID"},{"field":"ClientIP"}],"size":10,"order":[{"c":"desc"},{"_key":"asc"}]},"aggregations":{"s":{"sum":{"field":"IsRefresh"}},"a":{"avg":{"field":"ResolutionWidth"}},"c":{"value_count":{"field":"_index"}}}}}}
J
cat > $OUT/bodies/cbp_fm_urls_over_5.json <<'J'
{"size":0,"aggregations":{"by_engine":{"terms":{"field":"SearchEngineID","size":50},"aggregations":{"over":{"filtered_metric":{"buckets":{"terms":{"field":"URL"}},"metric":{"cardinality":{"field":"Referer"}},"filter":{"gt":5},"execution_hint":"dfs"}}}}}
J

# ============================================================================
# Index map per family × variant
# ============================================================================
declare -A INDEX
INDEX[om.lz4]=pinot_applicaiton_2025_03_v3
INDEX[om.fsst]=pinot_applicaiton_2025_03_fsst
INDEX[cbp.lz4]=clickbench_plus
INDEX[cbp.fsst]=clickbench_plus_fsst

run_one() {
  local cfg=$1 q=$2 variant=$3
  local family=${q%%_*}
  local idx_key="${family}.${variant}"
  local idx="${INDEX[$idx_key]}"
  if [ -z "$idx" ]; then echo "ERR no index for $idx_key"; return; fi
  mkdir -p "$OUT/$cfg"
  local t0=$(date +%s%3N)
  curl -sku Admin:Admin@123 --max-time 600 -XPOST "$ENDPOINT/$idx/_search?request_cache=false" \
    -H 'content-type: application/json' -d @"$OUT/bodies/$q.json" > "$OUT/$cfg/${q}_${variant}_resp.json" 2>&1
  local t1=$(date +%s%3N)
  local wall=$((t1-t0))
  python3 <<PY
import json
try:
  d = json.load(open("$OUT/$cfg/${q}_${variant}_resp.json"))
  sh = d.get('_shards',{})
  err = 'error' in d
  took = d.get('took','?')
  print(f"$cfg/${q}/${variant}: wall=${wall}ms took={took}ms shards=tot={sh.get('total')} succ={sh.get('successful')} fail={sh.get('failed')} err={err}")
except Exception as e:
  print(f"$cfg/${q}/${variant}: PARSE ERROR {e}")
PY
}

[ -f "$INSTANCES_FILE" ] || { echo "ERROR no instance file"; exit 1; }

OM_QUERIES="om_multi_term_low_card om_multi_term_high_card om_multi_term_low_card_max om_multi_term_high_card_max om_fm_high_card_cardinality om_fm_low_card_cardinality"
CBP_QUERIES="cbp_nested_user_card cbp_q23_title_search_cardinality cbp_heavy_nested_default cbp_multi_terms_deferred cbp_q33_watch_client_all cbp_fm_urls_over_5"

echo "[$(date -u +%H:%M:%S)] fleet GC before phase 1..."
fanout_gc
apply_settings '{"persistent":{"stream.search.enabled":false}}' > $OUT/settings_classic.json
sleep 3

echo "==== PHASE 1: classic, LZ4 (baseline) ===="
for q in $OM_QUERIES $CBP_QUERIES; do
  run_one classic_lz4 "$q" lz4
done

echo "[$(date -u +%H:%M:%S)] fleet GC before phase 2..."
fanout_gc
echo "==== PHASE 2: classic, FSST (Omnissa only — cbp_fsst not built due to FSST+ merge bug) ===="
for q in $OM_QUERIES; do
  run_one classic_fsst "$q" fsst
done

echo "[$(date -u +%H:%M:%S)] fleet GC before phase 3..."
fanout_gc
apply_settings '{"persistent":{"stream.search.enabled":true,"search.aggregations.streaming.arrow_columnar.enabled":true}}' > $OUT/settings_streaming.json
sleep 3

echo "==== PHASE 3: streaming+columnar, LZ4 ===="
for q in $OM_QUERIES $CBP_QUERIES; do
  run_one streaming_lz4 "$q" lz4
done

echo "[$(date -u +%H:%M:%S)] fleet GC before phase 4..."
fanout_gc
echo "==== PHASE 4: streaming+columnar, FSST (Omnissa only) ===="
for q in $OM_QUERIES; do
  run_one streaming_fsst "$q" fsst
done

echo "ALL DONE: $OUT"
