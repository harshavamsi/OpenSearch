# Axis 1 investigation — where streaming actually wastes work on shards

## Q1: Does HLL preserve sparse state on the wire?

**Yes, already.** `AbstractHyperLogLogPlusPlus.writeTo` (lines 119-135) checks the algorithm
flag per bucket and serializes either `size × int32` (sparse) or `2^precision × byte` (dense).
Precision 14 → sparse threshold at 3072 distinct values (4096 capacity × 0.75 load factor).

Wire size per sketch:
- 100 distinct → 400 B sparse (vs 16 KB dense: **40× savings**)
- 1000 distinct → 4 KB sparse (vs 16 KB: **4× savings**)
- 3072+ distinct → 16 KB dense (no savings, but this is genuinely needed for accuracy)

For Omnissa's `device_guid__app_package_id` cardinality sub-agg, most inner buckets have a
small app count per (platform, device) — probably well under 100. So typical sketches are
**sub-1 KB, not 16 KB.** My earlier "800 MB per shard-stream" estimate was off by likely 10×.

**Implication:** wire-bytes-on-the-coord is **not the Omnissa bottleneck.** That kills my
earlier Phase 3 (columnar Arrow) enthusiasm as the Omnissa lever — good to know.

## Q2: What does DEPTH_FIRST actually cost in the Omnissa shape?

Shard-side streaming path inside `StreamStringTermsAggregator`:

1. Single-segment leaf collector fires `sub.collect(doc, bucketOrd)` for every doc in every
   candidate bucket (line 198). With DEPTH_FIRST, that propagates directly to the nested terms
   aggregator, which in turn fires `cardinality.collect(doc, innerBucketOrd)` for every doc.
2. HLL linear-counting hash + addEncoded runs on every call.
3. Only **after** all docs are collected does `buildAggregationsBatch` select the top-N
   buckets by doc count (line 227 `selectTopBuckets`).
4. `buildSubAggs` (line 236) then reads out sketches for **only the top-N survivors**, calling
   `buildSubAggsForAllBuckets` → `buildSubAggsForBuckets(survivorOrds)`.

The work between steps 1 and 3 for non-survivor buckets is **entirely wasted** — sketches
built in-heap, never serialized, never used.

**Concrete cost for `nested_agg_high_cardinality`:**
- 3M candidate `device_guid` values per shard; `segmentTopN` caps survivor selection at
  whatever `requiredSize` the query specified (3M in the Omnissa case, but the streaming
  min-segment-size clamp probably caps at tens or hundreds of thousands).
- Every doc (~12M per shard) touches HLL state for its own device_guid bucket.
- Ratio of useful HLL work: `topN / unique_candidates`. If topN=100K and unique=3M, ~3% of
  the HLL work is useful. **~97% of sketch maintenance is thrown away.**

For the low-cardinality nested shape (`size=10000` on 7K app_packages), it's
topN_useful/unique ≈ 1 so DEPTH_FIRST is fine. DEPTH_FIRST only burns on high-cardinality
inners.

## Q3: Is BREADTH_FIRST structurally possible in streaming?

**Yes, already.** `StreamStringTermsAggregator → AbstractStringTermsAggregator →
TermsAggregator → DeferableBucketAggregator`. The machinery is inherited. The blocker is
just a conservative hard-force at `TermsAggregatorFactory.pickSubAggCollectMode:386`:

```java
if (context.isStreamSearch() && (context.getFlushMode() == null || context.getFlushMode() == FlushMode.PER_SEGMENT)) {
    return SubAggCollectionMode.DEPTH_FIRST;
}
```

Git blame: this forcing was added in the original streaming PR (80c92e95818, "Streaming
Aggregation (#18874)") with no comment justifying the choice.

### What `BestBucketsDeferringCollector` does
Records `(doc, bucketOrd)` pairs via `PackedLongValues` during leaf collection (very compact —
likely 4-8 bytes per doc). Sub-aggs aren't invoked. After `prepareSelectedBuckets(survivors)`,
replays the packed records but only for `doc` values mapped to survivor buckets. Sub-agg
computation happens once, on a known-reduced set.

### What needs to change for streaming to use it
`StreamStringTermsAggregator.buildAggregationsBatch` runs per-segment, so the defer/replay
cycle has to fit inside one call:

1. During leaf collection: sub-aggs are recording-wrappers (already handled by
   `DeferableBucketAggregator.doPreCollection` when `shouldDefer` returns true).
2. After `selectTopBuckets` returns survivor ords for this owning-bucket-ord:
3. Call `recordingWrapper.prepareSelectedBuckets(survivorOrds)` to trigger the replay.
4. Then `buildSubAggs` reads out the now-correctly-populated survivor sketches.

Step 2→3 is the only new wiring. The existing `DeferableBucketAggregator.beforeBuildingBuckets`
hook does exactly this, called automatically from `buildSubAggsForBuckets`.

`StreamStringTermsAggregator` would need:
- Override `shouldDefer(Aggregator)` to return true for metric sub-aggs (cardinality/max/min/
  sum) when streaming.
- Let the existing `DeferableBucketAggregator` / `BestBucketsDeferringCollector` plumbing do
  the rest — no new collector classes.

### Risks to understand before flipping the switch
- **`scoreMode().needsScores()`** — if any sub-agg needs scores, we can't defer easily (the
  deferring collector doesn't replay scores by default in v1). For terms→cardinality/max/min/
  sum this isn't an issue — none need scores. Gate on `subAggsNeedScore()` before deferring.
- **Memory during recording** — `PackedLongValues` for 12M docs ≈ 24-48 MB per shard. Fits
  comfortably. Tradeoff vs DEPTH_FIRST's full sketch state: recording is smaller in all cases
  where survivor count < unique count.
- **Nested terms as a sub-agg** — the current streaming scope allows `terms → terms → metric`.
  If we defer the inner terms, we need the inner's sub-agg (the metric) to also cope with
  replay. BREADTH_FIRST through a terms agg works in non-streaming; should work in streaming
  with the same semantics.

## Recommendation: Phase 5 — allow BREADTH_FIRST in streaming for metric-only sub-agg trees

Scope:
1. Change `TermsAggregatorFactory.pickSubAggCollectMode:386` so the streaming branch returns
   BREADTH_FIRST when all sub-aggs are metric (no scores needed) and survivor count
   (`segmentTopN`) < expected unique ordinals.
2. Override `shouldDefer` in `StreamStringTermsAggregator` / `StreamNumericTermsAggregator`
   to return true for metric sub-aggs.
3. Verify the defer/replay flow works correctly within a single `buildAggregationsBatch`
   call — the existing `buildSubAggsForBuckets` path calls `beforeBuildingBuckets` which
   triggers the replay. Probably works out of the box.

Expected impact:
- **Shard CPU on nested_agg_high_cardinality: 10-30× reduction** in sub-agg compute (scales
  with unique_candidates / topN ratio).
- **Shard heap during streaming collection: smaller** because we're holding packed
  long-values instead of partial sketch state for discarded buckets.
- **Wire bytes: unchanged** (we were only sending survivor sketches before too).
- **Latency: likely lower** (less CPU burn during collection, same network time).

This is the lever I've been missing. It's orthogonal to Phase 4 (which bounds coord state)
and composes cleanly with it — shards do less work, coord holds less state.

## What this means for the broader plan

- **Phase 4 (Shipped)**: bounds coord state. Necessary.
- **Phase 5 (Proposed)**: bounds shard CPU. The other half of making streaming actually
  efficient on Omnissa-shaped workloads.
- **Phase 3 (Columnar wire)**: still useful for other workloads (terms→sum/max shapes where
  no sparse-sketch compression kicks in), but **not the Omnissa lever** — deprioritize
  relative to Phase 5.
- **Two-phase streaming (Axis 4)**: interesting but less leverage than Phase 5, more risk
  (protocol change).

If Phase 5 is a real win, then streaming's story becomes: shards do sub-agg work on survivor
buckets only; coord merges survivors with bounded heap. That's a genuinely more efficient
framework — not just a patch on CB behavior.
