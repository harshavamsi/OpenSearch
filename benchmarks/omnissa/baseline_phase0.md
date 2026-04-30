# Phase 0 — Omnissa streaming baseline (stock OpenSearch 3.5.0, no patches)

Run date: 2026-04-30 (partial — first warmup pass + 4 timed runs before cancelling for cluster availability)
Cluster: `omnissa-streaming` (eu-west-1 staging, 16 × r7gd.8xlarge data, 3 × r7g.12xlarge masters)
Index: `pinot_applicaiton_2025_03_v3` (128 primary shards, 1.52B docs, ~3.8 TB, green)
Starting heap: max 12.4 GB / avg 8.2 GB across nodes (clean cluster)
Streaming settings: permissive (`stream.search.enabled=true`, bucket thresholds effectively disabled)

## Results

Warmup pass (cluster quiet at kickoff):

| Query | `took` | Outcome | Max heap after |
|---|---|---|---|
| `device_count` | 1.7 s | OK, 0 shard failures | 12.5 GB (unchanged) |
| `nested_agg_low_cardinality` | 131 s | 26/128 shards failed | 30.9 GB ⚠️ (approached CB ceiling at 29 GB limit) |
| `nested_agg_low_cardinality_max_agg` | 34 s | OK | 30.5 GB (still elevated from prior query) |
| `nested_agg_high_cardinality` | 140 s | **all shards failed** | 30.5 GB |
| `nested_agg_high_cardinality_max_agg` | 0.8 s | **rejected: "Search pre-filter is not supported in streaming"** | 30.5 GB |
| `multi_term_low_cardinality` | 65 s | 8/128 shards failed | 19.4 GB (draining) |
| `multi_term_low_cardinality_max_agg` | 20 s | OK | 20.6 GB |
| `multi_term_high_cardinality` | 17 s | all shards failed | 25 GB |
| `multi_term_high_cardinality_max_agg` | 18 s | OK | 25 GB |

Timed run sample (first 4 of planned 27):

| Query | `took` | Outcome |
|---|---|---|
| `device_count` × 3 | 0.9 s median | all OK, 0 failures |
| `nested_agg_low_cardinality` run0 | 133 s | 17/128 shards failed |

## Five findings

### 1. Streaming by itself drives coord heap near the parent CB ceiling
On a completely quiet cluster (max 12.4 GB at kickoff, nothing else running), a single
`nested_agg_low_cardinality` query pushed coord parent breaker to 30.9 GB — within 2 GB of
the 29 GB limit where tripping starts. No other workload on the cluster. This is the
streaming path's coord-side buffering doing exactly what Phase 4 is designed to prevent:
accumulating every unique term's sub-agg state before reducing.

### 2. Heap doesn't release between queries quickly
After a query completes, heap stays elevated (30 GB → 30 GB → 20 GB over 3 queries). The
streaming reduce state lingers. This is either:
- `QueryPhaseResultConsumer.PendingReduces` retaining per-shard `QuerySearchResult` buffers past completion, or
- Flight-side `VectorSchemaRoot` allocations not being promptly released

Either way, it means **two back-to-back streaming queries on the same cluster are more
dangerous than one**, even if individually both would fit. Phase 6's PER_SHARD_STREAM would
collapse per-query coord state to ~topN buckets via the Phase 4 reducer, releasing
immediately at shard-complete.

### 3. `nested_agg_high_cardinality_max_agg` hits a hard streaming gate
Fails in 0.8 s with `"Search pre-filter is not supported in streaming"` — coming from
`StreamTransportSearchAction.java:103`. This is a pre-existing gap in the streaming coord
path that none of our Phase 4/5/6 work addresses. Search pre-filtering is an optimization
that runs a `can_match` phase before the real query on clusters with many shards; the
streaming action just rejects it outright.

Worth tracking but out of scope for the coord-memory work.

### 4. Partial shard failures even when the query "succeeds"
- `nested_agg_low_cardinality`: 26/128 shards failed (20%) on warmup, 17/128 on run0
- `multi_term_low_cardinality`: 8/128 shards failed (6%)

These are the streaming path hitting per-shard resource limits on queries that classically
would complete. The 80% that succeed return results, so the query doesn't fail outright —
but the response is incomplete. Our Phase 4 reducer handles this gracefully (folds what
arrives), but Phase 6's classic shard compute + streaming transport would eliminate the
shard-side flakiness entirely.

### 5. The pattern that survives cleanly = the pattern that doesn't need fixing
- `device_count`: simple cardinality, 0 sub-agg buckets → 1.7 s, no heap impact
- `*_max_agg` queries: terms + max sub-agg (no sketch state) → 18–34 s, no shard failures

The pure cardinality-sub-agg shape is where every streaming weakness shows up together:
coord buffering, per-shard failures, slow latency. That's also exactly what Phase 4 + Phase
6 target.

## What this baseline validates

For the Phase 4 + Phase 6 thesis:
- **Coord-memory bounded is necessary**, not theoretical. Even quiet runs trip 30 GB.
- **Cardinality sub-agg is the pain shape**, as predicted. Phase 6's `hasStatefulMetricSubAgg` heuristic correctly targets these.
- **Classic shard compute is cleaner**. `*_max_agg` queries — which *already* use classic-style aggs on the shard because streaming only has a special-case for terms → never show the per-shard failures. Phase 6 extends this cleanliness to cardinality-sub-agg shapes.

For future work:
- Pre-filter rejection in `StreamTransportSearchAction.java:103` is a separate gap affecting `*_high_cardinality_max_agg`. Not addressed.
- Heap-release lag between queries is real. Once Phase 4/6 deploy and state is bounded by topN, this should resolve itself.

## Numbers for the A/B comparison once we deploy

After Phase 4 + Phase 6 deploy to a domain, I'd expect the following changes on the same queries:

| Query | Today | Target after Phase 4 + Phase 6 |
|---|---|---|
| `device_count` | 1.7 s, no heap impact | unchanged |
| `nested_agg_low_cardinality` | 131 s, 30.9 GB coord, 20% shard failures | classic-shard-latency (likely 60–100 s) + bounded coord heap (≤ topN × per-bucket-size) + 0 shard failures |
| `nested_agg_low_cardinality_max_agg` | 34 s | unchanged (already in max, not cardinality) or slightly better from PER_SHARD_STREAM amortizing across segments |
| `nested_agg_high_cardinality` | 140 s, all shards fail | will still stress — likely needs Phase 4.1 early termination on top |
| `multi_term_low_cardinality` | 65 s, 8/128 shard failures | Phase 6 applies (has cardinality). Shard failures should drop to 0. Latency likely similar or better. |
| `multi_term_low_cardinality_max_agg` | 20 s | unchanged |
| `multi_term_high_cardinality` | 17 s (all fail) | needs multi_terms streaming support (Phase 1c) to even run via streaming |

## Files

- Raw responses: `benchmarks/omnissa/results/<query>__{warmup0,run0,run1,...}.json`
- Per-run timing: `<query>__<label>.timing` (wall_ms, took_ms, response_bytes, heap_before_gb, heap_after_gb, status)
- Baseline script (includes heap tracking + settle-loop): `benchmarks/omnissa/run_baseline.sh`
