# Phase 4 — Bounded-PQ streaming reduce + HLL merge-as-you-go

## Problem restatement

On Omnissa's `terms(device_platform) → terms(device_guid, size=3M) → cardinality(...)` shape:
- Each shard emits ~100K inner buckets × ~8-16 KB HLL sketch = ~800 MB – 1.6 GB per shard-stream.
- Coord-side `StreamQueryPhaseResultConsumer.consumeStreamResult` hands each result to
  `PendingReduces.consume` which **buffers** the `QuerySearchResult` in memory (line 448 of
  `QueryPhaseResultConsumer.java`) until `batchReduceSize` is reached.
- For streaming, `StreamQueryPhaseResultConsumer.getBatchReduceSize` multiplies the non-streaming
  threshold by 10 — so partial reduces fire *less often* for streaming, causing memory to
  accumulate further.
- When partial reduce eventually runs (`partialReduce:216-281`), it calls
  `InternalAggregations.topLevelReduce(aggsList, ...)` which delegates to `InternalTerms.reduce`
  which builds a full `reducedBuckets` list containing every unique bucket across all shards
  seen so far, then prunes to topN only at the very end (line 466-489).
- Result: coord heap grows linearly with `unique_terms × sketch_size` until CB trips.

## What we actually need

Two invariants at the coord:
1. **Bucket count is bounded by `topN_size`**, not by `unique_terms_across_shards`.
2. **Sketches for the same term are merged in-place**, not buffered as N separate sketches.

`HyperLogLogPlusPlus.merge` is already in-place (`HyperLogLogPlusPlus.java:192-204`), and
`InternalCardinality.reduce` already merges N sketches into one (line 104-121). So invariant 2
is *already free* at reduce-time — the issue is that it only happens when `partialReduce` fires.

If we can drive a reduce **every time a shard batch arrives** instead of every `batchReduceSize`
batches, and keep the running reduced state as a persistent bounded PQ, both invariants hold.

## Design: parallel path, not surgery on existing reduce

`InternalTerms.reduce` is shared with non-streaming. Touching it risks a regression on the
millions of non-streaming queries per day across the fleet. Instead:

### New class: `StreamingTermsReducer`

Lives at `server/src/main/java/org/opensearch/search/aggregations/bucket/terms/StreamingTermsReducer.java`.

Responsibilities:
- Maintain a persistent top-N `BucketPriorityQueue` keyed by `term` (so we can look up existing
  buckets on incoming batches).
- Maintain an **auxiliary HashMap<bytes, B>** for O(1) lookup — the PQ alone is O(N) to find an
  existing term.
- On each `accept(InternalTerms<A, B> shardBatch)`:
  1. For each incoming bucket `b`:
     - If `map.containsKey(b.key)`: merge — add `docCount`, call
       `InternalAggregations.topLevelReduce([existing.aggs, b.aggs], ...)` to merge sub-aggs
       (including HLL in-place).
     - Else if `pq.size() < topN`: insert into PQ + map.
     - Else if `b.docCount > pq.bottom.docCount`: evict bottom from pq + map, insert `b`.
  2. Track `otherDocCount` (sum of incoming doc counts that didn't make the cut).
- On `finalize()`: emit an `InternalTerms` with the top-N buckets, sorted by final order.

### Integration: `StreamingPartialReducer` in the consumer

New class at `server/.../action/search/StreamingPartialReducer.java`. One instance per
`StreamQueryPhaseResultConsumer`. Holds a `StreamingTermsReducer` per top-level terms agg.

`StreamQueryPhaseResultConsumer.consumeStreamResult` calls `streamingReducer.accept(result)`
**directly** instead of going through the buffering `PendingReduces.consume` path. The existing
`PendingReduces` handles top-docs and non-terms aggs; we delegate only the terms tree.

On final reduce, `streamingReducer.finalize()` produces the merged `InternalAggregations`, which
we hand back to `SearchPhaseController.reducedQueryPhase` as the single pre-merged aggregation.

### Early termination (optional, bolt-on)

Once PQ floor's `docCount` exceeds every remaining shard's `StreamingCostMetrics.topNSize × maxPossiblePerBucketContribution`,
we know none of the un-received shards can promote a new bucket into the top-N. We can then
call `cancel()` on the Flight stream for those remaining shards.

This is a nice-to-have. Skipping for v1 — the bounded-PQ alone should fix the CB problem.

## Scope boundaries for v1 (must-ship)

- Top-level terms agg: `StreamStringTermsAggregator` + `StreamNumericTermsAggregator` outputs.
- Sub-agg: single level of metric sub-aggs (cardinality, max, min, sum). Nested terms under
  streaming terms already required metric-only leaves per `FlushModeResolver.isStreamable` — no
  change in scope.
- Aggregation ordering: count-order and key-order. Agg-order pushed to Phase 5.
- Non-streaming path: unchanged, zero risk.

## Scope boundaries explicitly excluded (v2+)

- Multi-terms streaming (Phase 1c, separate work).
- Metric-order streaming (requires partial-reduce-as-you-go, not just PQ — saved for Phase 5).
- Early termination with Flight cancel (Phase 4.1).
- date_histogram / histogram streaming reduce (Phase 6).

## Files touched (v1)

| File | Change |
|---|---|
| `StreamingTermsReducer.java` | NEW — per-agg bounded-PQ reducer. |
| `StreamingPartialReducer.java` | NEW — per-request orchestration across streaming aggs. |
| `StreamQueryPhaseResultConsumer.java` | MODIFY — route terms tree through streaming reducer, keep non-terms on existing partial-reduce path. |
| `StreamingTermsReducerTests.java` | NEW — unit tests: 128 shards × 100K buckets × HLL sketches; assert coord-peak heap bounded. |

Non-streaming `InternalTerms.reduce`, `QueryPhaseResultConsumer.PendingReduces`, etc. — **untouched**.

## Correctness invariants to test

1. **Idempotence on bucket order**: accepting batches in any order produces the same top-N (docs
   agree with non-streaming reduce on the same inputs).
2. **HLL merge correctness**: for a term appearing in N shard batches, the final cardinality
   estimate matches what non-streaming reduce would compute for the same inputs.
3. **Memory bound**: coord heap stays at `O(topN × per-bucket-size)` regardless of shard count or
   unique-terms-across-shards.
4. **Doc count correctness**: `otherDocCount` + sum of top-N doc counts == sum of all incoming
   doc counts.

## Risk table

| Risk | Mitigation |
|---|---|
| Parallel code path drifts from non-streaming reduce behavior | v1 comparison test: run same input through both paths, diff output. |
| HLL merge semantics differ when merged-over-time vs merged-all-at-once | HLL merge is commutative + associative by construction. A randomized property test covers this. |
| Final ordering requires agg-sort and we deferred that to v2 | v1 rejects agg-sorted streaming terms at the planner level (Phase 0 logging will show the reason). |
| Pipeline aggs run post-reduce and expect the old `InternalAggregations` shape | Our `finalize` produces a plain `InternalAggregations` — same type and shape. |

## Timeline

- Day 1: write `StreamingTermsReducer` + unit tests for PQ invariants and HLL merge.
- Day 2: write `StreamingPartialReducer` + hook into `StreamQueryPhaseResultConsumer`, add
  parity test vs non-streaming reduce.
- Day 3: memory-bound tests, edge cases (empty shards, all-same-term, single-shard), spotless
  + integration-level asserts.
- Day 4–5: buffer for unknowns that surface during implementation (there will be some).

Starting day 1 now.
