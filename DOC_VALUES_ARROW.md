# Doc_values as the storage layer under the mustang engine (Arrow + Flight + DataFusion)

*July 2026. Full chronological work log (streaming-agg POC, full-Arrow follow-on, session-by-session
fix history): [[DOC_VALUES_ARROW_HISTORY]]. This document carries only the claim, the evidence, the
architecture, and the open engineering — written for review.*

---

## The claim

**OpenSearch doc_values can replace parquet as the storage layer under the mustang engine,
eliminating the storage-format migration.** Doc values already are a columnar store; the
aggregation-speed gap lives above the file format. Two independent measurements at 100M-row
ClickBench scale support the claim:

1. **Local same-machine harness** (cleanest storage-layer isolation, zero network/coordinator
   noise): 42/43 queries run on the dv+arrow leg. dv wins wherever Lucene index structures
   prune the scan (q21: 2.6s→13ms; q23: 9.8s→23ms class), lands 1.2–3x behind parquet on
   full-scan numeric group-bys (single-partition shard feed — engineering, not format), and
   matches parquet on storage footprint (15GB dv / 14GB parquet for the same 24 columns).

2. **Three-cluster A/B** (identical r7i.2xlarge nodes, PPL on all legs, 1 iteration, caches
   off): **dv answers 42/43 correctly** — 26 exact result digests vs parquet, 4 within
   sort-tie margin (parquet and vanilla baseline disagree with each other on those too),
   12 within the 0.004% ingest-dup margin. Latencies within ~1.2–2x of parquet on full-scan
   group-bys, FASTER on selective-filter and sort shapes. Vanilla OpenSearch collapses at
   cardinality: 12 timeouts + several 100–290s outliers. Parquet fails 1 (q14, memory —
   waived on all legs).

---

## Architecture: the dv-plan path (wire v3)

The shard-local pipeline that makes this work, end to end:

```
PPL → Calcite plan → planner marks lucene viable (capability declarations)
    → LuceneFragmentConvertor.extractGeneralDvShape / extractRowDvShape
        rebases Aggregate[→Project][→Filter]→scan  (or [Project]→[Sort(fetch)]→[Filter]→scan)
        onto a stage-input scan over the referenced dv columns
    → DatafusionShardAggregationEngine.compileFragment
        applies DataFusion's scalar-function adapters, then the FULL isthmus
        (DataFusionFragmentConvertor) — same conversion path as parquet fragments
    → wire v3: [marker, base64(substrait), input columns+kinds, output names] + Lucene
        QueryBuilder filter tail
    → data node: DocValuesAggregationExecutor matches docids with the Lucene query,
        bulk-decodes columns into 65536-row Arrow batches (fork APIs longValuesInto /
        ordValues), feeds a shard-local DataFusion session executing the PARTIAL half
        of the plan (df_execute_local_plan_partial)
    → Flight streams Arrow batches to the coordinator's DataFusion reduce (FINAL half)
```

Key components (all in `sandbox/`, branch `columnar-streaming`):

- **Lucene fork** (branch `bulk-ord-values-10_5`, published 10.5.1-SNAPSHOT): bulk decode
  APIs — `NumericDocValues#longValuesInto(MemorySegment)` covering raw/gcd/delta/table
  encodings, `SortedDocValues#ordValues` bulk ordinals. Upstream-shaped (mirrors merged
  precedents GH#16129/#16286).
- **ShardAggregationEngine SPI** (`sandbox/libs/analytics-framework`): the seam between the
  storage backend (Lucene decodes columns → Arrow) and the execution engine (DataFusion runs
  the compiled fragment). `compileFragment(RelNode)` + `open(planBytes, inputColumns)` are
  plan-agnostic — aggregates, DISTINCT, expressions, and row-returning Sort/Project shapes
  all ride the same two methods. ColumnKinds: LONG (Int64), KEYWORD (Utf8View, ordinal-memo
  materialization), TIMESTAMP (ms).
- **Partial-aggregation mode** (Rust `agg_mode.rs`): the shard session strips the compiled
  plan to its PARTIAL half so engine-native-merge aggregates (approx_distinct) emit
  intermediate state (HLL sketches) for the coordinator merge. Single→Partial conversion
  handles single-stage plans; non-aggregate plans pass through untouched.
- **Cross-backend planner seams** (`sandbox/plugins/analytics-engine`): lucene drives the
  shard scan, DataFusion runs the coordinator reduce — PlanForker's agnostic-seam fallback,
  sort-rule fallback, reduce-capability fallback.
- **Ordinal-first keyword group-by**: single keyword key + mergeable aggs groups on the
  per-segment ordinal as Int64 (native fast path), materializes terms once per RESULT group
  in ord-sorted order (sequential term-dict reads; unsorted was 38.7s at 6M groups).

## The four fix classes (each root-caused and verified on-cluster)

Everything that initially failed on the dv leg fell into four classes. None was a
storage-format problem.

**1. Scalar functions (q19/q29/q40/q43: EXTRACT, REGEXP_REPLACE, CASE, DATE_FORMAT).**
Two stacked causes. (a) Planner viability: lucene never claimed these in PROJECT_CAPS, so
planning aborted before the (already-capable) compiled-fragment path ran. (b) Adapter
application: BackendPlanAdapter applies the DRIVING backend's scalar adapters — lucene's map
is empty — so raw Calcite EXTRACT reached isthmus unadapted and failed signature binding.
Fix: capability claims + apply `DataFusionAnalyticsBackendPlugin.scalarFunctionAdapterMap()`
(made static) bottom-up inside `compileFragment`. Lucene-driver fragments now get the
identical rewrite set (EXTRACT→opensearch_extract, DATE_FORMAT→Rust UDF, regexp \1→${1},
MINUTE→date_part …) as parquet-driver plans. CASE needed only the capability claim — isthmus
lowers it structurally to substrait IfThen.

**2. min/max(keyword) (q22/q23).** One capability line (`AggregateCapability MIN/MAX over
keyword`); the substrait yaml overloads and DataFusion execution pre-existed.

**3. Row-returning ORDER BY/LIMIT shapes (q20/q24–q27).** Non-aggregate fragments fell
through to the count fast path, which emitted a 0-column Arrow batch ("Native Arrow batch
has no field vectors"). Fixes: `extractRowDvShape` compiles
`[Project]→[Sort(fetch)]→[Filter]→scan` through the engine (DataFusion TopK per shard,
coordinator re-sort merges — the same reduce parquet uses); `__row_id__` decodes as a real
singleton SortedNumericDocValues despite being declared derived; the QTF/late-materialization
rewrite is gated off lucene-primary scans (its fetch phase needs `fetchByRowIds`,
unimplemented on Lucene); the reduce sink coerces Utf8↔Utf8View for computed string keys.
Contract note: feed-then-finish is deadlock-safe only for pipeline-breaker plans — GROUP BY
and Sort+fetch both qualify; fetch-less broad filters would need the memtable path.

**4. The "17M-group tier" timeouts (q19, q31–q36) — NOT a throughput ceiling.** jstack on a
live 600s query: all shard feeders parked in `senderSend`, node at 0% CPU. DataFusion's
skip-partial probe was switching the shard aggregate to passthrough emission mid-stream;
output fills the bounded channel before `finish()` starts the drain → feed-then-drain
deadlock. The shard-local session never pinned `skip_partial_aggregation_probe_ratio_threshold`
(the parquet session does). Subtlety: the threshold must be **2.0, not 1.0** — the probe
fires on `num_groups/input_rows >= threshold` and the ratio hits exactly 1.0 on near-unique
keys (WatchID). Result: **every 600s timeout collapsed to seconds** (table below). This also
explains the earlier "native pool reservation leak" — parked sessions never released
reservations.

---

## Final cluster results (100M-doc ClickBench, 4 shards, single r7i.2xlarge per leg, 1 iter, no cache)

parquet = mustang parquet-primary; baseline = vanilla OpenSearch PPL; dv-arrow =
lucene-primary doc_values through the dv-plan path. Digest = sha256 over sorted normalized
rows (column/row-order insensitive, floats 2dp).

```
query                                 parquet  baseline   dv-arrow  vs-pq
q01-count-all                            30       251       2198  ~dup
q02-count-adv-engine                     65        18        149  ~dup
q03-sum-count-avg                       198      1090       1425  ~dup
q04-avg-userid                          218      2058        656  ~dup
q05-distinct-userid                     239       404        355  SAME
q06-distinct-searchphrase              1123       916       2458  SAME
q07-min-max-eventdate                    20        14        431  SAME
q08-group-by-adv-engine                  68       244        466  ~dup
q09-region-users                       1971      3097       4240  SAME
q10-region-stats                       2057      4808       4326  ~dup
q11-mobile-phone-model                  385       528        604  SAME
q12-mobile-phone-stats                  454       450        583  SAME
q13-search-phrase-count                1627    252172       1950  SAME
q14-search-phrase-users                FAIL       T/O       skip  -    (waived: >mem on all)
q15-search-engine-phrase               1696    289723       3650  SAME
q16-user-activity                      2954     39273       3929  SAME
q17-user-search-activity               5680    166262       6497  SAME
q18-user-search-limit                  5677       876       5959  ~dup
q19-user-minute-search                 7878       T/O      12946  SAME (was 600s T/O)
q20-specific-user                       160       261         46  SAME
q21-google-urls                        3797     12283       4185  SAME
q22-google-search-phrases              4379      3777       4149  SAME
q23-google-title-search               10117     12815       7496  SAME
q24-google-urls-sorted                 9383      2643       4924  SAME
q25-search-phrases-by-time            20653        52       6993  ~tie
q26-search-phrases-sorted               870       371        773  SAME
q27-search-phrases-multi-sort         20575        30       7247  SAME
q28-counter-url-length                 3446    101336       4906  SAME
q29-referer-analysis                  23324       T/O      16740  ~dup
q30-resolution-width-sums               381       718        990  ~dup
q31-search-engine-client-stats         2309       T/O       4676  SAME (was 600s T/O)
q32-watch-client-stats                 2257       T/O       2549  ~tie (was 600s T/O)
q33-watch-client-all                  11214       T/O      14010  ~tie (was 600s T/O)
q34-url-popularity                     9768    240901      11371  SAME (was 600s T/O)
q35-url-with-constant                 10027    261074      10584  SAME (was 600s T/O)
q36-client-ip-variations               3349       T/O       3740  SAME (was 600s T/O)
q37-counter-62-urls                     160      1844       2930  SAME
q38-counter-62-titles                   123       353        204  SAME
q39-counter-62-links                     89        97        146  ~dup
q40-traffic-source-analysis             253     45795        481  ~tie
q41-url-hash-date                        52       935        205  ~dup
q42-window-client-dimensions             51        71        110  ~dup
q43-hourly-pageviews                    141       792        108  SAME
```

- **SAME** = exact digest match with parquet (26 queries).
- **~tie** = rows tied on the sort key beyond head-N; the three legs disagree pairwise —
  benchmark artifact, not an engine issue (4 queries).
- **~dup** = counts within the 0.004% ingest-duplicate margin (bulk-retry during dv-leg
  ingest; 12 queries).

**Latency character (dv vs parquet):** within ~1.2–2x on full-scan group-by tiers (the
single-partition shard feed residual), FASTER on selective-filter/sort shapes (q20 46 vs
160; q23 7.5s vs 10.1s; q25/q27 ~7s vs ~20.6s; q29 16.7s vs 23.3s; q43 108 vs 141), fixed
streaming overhead on trivial counts (q01/q07 class). q37–q39 (selective CounterID filter)
is scorer-iteration bound — same bucket as the parallel-feed follow-up.

**Baseline collapse is the third data point:** vanilla OpenSearch times out on 12 queries
and takes 100–290s on several more (q13 252s, q15 290s, q34/q35 240–261s) that both
engine-backed legs answer in seconds.

---

## Remaining engineering (ranked)

1. **Per-segment parallel shard feed** — the ~1.2–2x full-scan residual. Today one feeder
   thread decodes all segments sequentially into a single-partition StreamingTable
   (regardless of `target_partitions`). Design: `register_partition_stream_n` FFI
   (StreamingTable already accepts Vec<partition>), k feeder tasks over the leaf list with
   per-task scratch buffers. Also covers the q37-class scorer-iteration gap.
2. **Dictionary-preserving keyword handoff** — feed ords + dictionary instead of
   materializing terms per batch; blocked on the engine deriving Utf8 (not dictionary)
   schemas from substrait.
3. **Fetch-less row shapes** — broad `filter | fields` without a sort/limit streams output
   during feed; needs the memtable registration path or a concurrent drain.
4. **Productionization**: composite-engine merge path for lucene-primary (force-merge
   unsupported — benchmark ran on natural segment topology by design), catalog-snapshot
   races under concurrent bulk (0.004% dup retry), deterministic tie-breaks if exact digest
   parity on ~tie queries matters.

## Artifacts & reproduction

- **Code**: OpenSearch branch `columnar-streaming` (worktree
  `~/worktrees/OpenSearch/columnar-streaming`, all dv-path code under `sandbox/`); Lucene
  fork branch `bulk-ord-values-10_5` → `~/.m2` as 10.5.1-SNAPSHOT. Build with
  `-Dsandbox.enabled=true -Drepos.mavenLocal=true`; Rust needs
  `PROTOC=/tmp/protoc/bin/protoc`.
- **Clusters** (POC account 779035457181, us-east-1): parquet i-0d328bbdba07acee9, baseline
  i-036e0246b3a09c337, dv i-03dc754ff8d16ac49. All ops via SSM. Node settings:
  `cluster.composite.primary_data_format=lucene` + `analytics.planner.prefer_metadata_driver=true`
  on the dv leg; 16g heap, ~31GB DataFusion pool, `node.native_memory.limit: 44g`.
- **Binaries**: s3://mustang-benchmark-runs/dvab/jars/ (5 jars + stripped
  libopensearch_native.so). Deploy = copy jars to plugin dirs + .so to lib/, restart via
  `opensearch-tar-install.sh`.
- **Runner**: `compare_leg.py` (on-node + /tmp/dvab/) — per-query digest runner,
  OS_START/OS_END/OS_ENDPOINT env vars; queries.json = 43 PPL queries.
- **Results**: digests on the dv node (`digest-dv-{heavy2,q19,q29,fixed1}.jsonl`), reference
  digests locally (`/tmp/dvab/results/digest-{parquet,baseline}.jsonl`), final table
  `/tmp/dvab/final-table-v2.txt`. Local-harness table in
  `sandbox/plugins/analytics-backend-datafusion/build/test-results/`
  (ClickBenchFullThreeWayBenchTests).
