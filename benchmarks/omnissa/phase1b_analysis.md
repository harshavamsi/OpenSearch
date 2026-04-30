# Phase 1b analysis — dictionary encoding isn't the right target

## Original hypothesis
Swap `VectorStreamOutput`'s single `VarBinaryVector`-of-Java-serde for a
`Dictionary<Int32, Binary>` + `UInt64` columnar layout. Expected win: ≥5× wire bytes on the
2.25M-cardinality `device_guid` shape from Omnissa.

## What I found instead (paper math, not prototyped)

Per-bucket serialization in `InternalTerms.AbstractInternalBucket.writeTo`
(server/.../bucket/terms/InternalTerms.java:144-151):
```
docCount         ~2 B   (vlong)
aggregations     8-16 KB (cardinality sub-agg = HLL++ sketch, precision 14)
term             ~36 B  (device_guid UUID as BytesRef)
```

HLL++ default precision is 14 → 2^14 = 16,384 registers, 1 byte each = **16 KB dense**.
Sparse mode is smaller at low cardinality but converges to dense as the inner `app_count`
grows. For Omnissa's `device_guid__app_package_id` cardinality sub-agg, sketches are
typically 8 KB+ per bucket because the inner cardinality is non-trivial.

### Per-shard-stream payload

For `nested_agg_high_cardinality` (terms→terms(3M device_guid)→cardinality):
```
8 KB sketch × 100,000 topN inner buckets = 800 MB per shard-stream
```
× 128 shards = **~100 GB aggregate streaming wire**.

### What dictionary encoding would save

Dictionary-encoding just the `term` column (the only column that benefits):
```
term column today:   36 B × 100,000 = 3.6 MB per shard (terms only)
term column dict:    4 B × 100,000  = 0.4 MB per shard
savings:             ~3 MB per shard = 0.4% of the 800 MB payload
```

**0.4% wire reduction.** Not 5×. Not 2×. Not meaningful.

## Why the original hypothesis was wrong

I assumed the term column was the dominant cost. For plain terms aggs without sub-aggs, it
would be — a bucket is basically `(term, count)` and dictionary encoding wins handily. But
the Omnissa queries all have **metric sub-aggs that dominate the payload**:
- `cardinality(device_guid__app_package_id)` → HLL sketch 8–16 KB
- `max(app_creation_date)` → ~8 B (sum/max is cheap)

The max-variants (e.g. `multi_term_low_cardinality_max_agg`) would see a real dict win,
but those queries already complete in 22 s on the classic path. The CB-tripping queries are
the cardinality ones, where the term column is noise.

## The actual target is the sketch aggregation pattern

Coord-side parent CB trips on `<reused_arrays>` and `<http_request>` contexts — not because
terms bytes are huge, but because **the coord buffers all shard streams before reducing HLL
sketches**. The failure mode is:

1. Shard 1 streams 100K (term, sketch) pairs → coord receives & buffers.
2. Shard 2 streams 100K (term, sketch) pairs → coord receives & buffers.
3. ... continues for all 128 shards ...
4. Coord tries to reduce → peak memory = 128 × 800 MB = ~100 GB > 29 GB parent CB limit.

Dictionary encoding doesn't help because the sketches don't compress meaningfully (they're
near-random registers).

## The fix that actually moves the needle

**Phase 4 — streaming reduce with bounded priority queue + HLL merge-as-you-go:**

1. Coord maintains a bounded priority queue of `topN_size` candidates ordered by doc count.
2. As each shard batch arrives, merge its buckets into the PQ:
   - Same term already present → HLL.merge(existing, incoming) — drops one sketch from heap
   - New term → insert if below topN cap, else compare against PQ floor
3. Once the PQ's bottom doc count exceeds the best-possible remaining contribution from
   un-received shards (tracked via per-shard `StreamingCostMetrics.topNSize`), send
   `cancel()` to remaining Flight streams.

Expected effect: coord-peak memory drops from **128 × 800 MB** to **1 × 800 MB + top-N PQ
(few hundred MB)** — a **~100× reduction** in worst-case coord heap.

This is what makes streaming actually win on the Omnissa CB-tripping shapes. Dictionary
encoding is a nice-to-have for low-sketch-density workloads, but not the Omnissa unblock.

## Decision

Phase 1b closed as "analysis only." Phase 4 (streaming reduce) promoted to the top of the
post-Phase-0 queue. Phase 3 (Arrow columnar schema) remains valuable for other workload
shapes (especially max/min/sum + terms) but isn't the Omnissa lever.
