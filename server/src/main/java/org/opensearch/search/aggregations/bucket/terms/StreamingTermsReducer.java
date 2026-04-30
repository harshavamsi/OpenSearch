/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregation.ReduceContext;
import org.opensearch.search.aggregations.InternalAggregations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bounded-size incremental reducer for streaming terms aggregations.
 *
 * <p>The non-streaming coord reduce buffers every {@link InternalTerms} result from every shard
 * before merging (via {@link InternalTerms#reduce}). For wide terms + HLL cardinality sub-aggs,
 * that's O(unique_terms_across_shards × sketch_size) — which is exactly what trips the coord's
 * parent circuit breaker on Omnissa-shaped workloads.
 *
 * <p>This reducer keeps running state bounded: a persistent hash map of survivor buckets keyed
 * by term, capped at {@code topN}. Each incoming shard batch is merged directly into survivors:
 * existing terms get their doc counts added and sub-aggs re-reduced (which merges HLL sketches
 * in-place via {@link org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus#merge});
 * new terms either join if under cap or displace the min-count survivor.
 *
 * <p>Scope (v1):
 * <ul>
 *   <li>Count-order and key-order only. Aggregation ordering (e.g. {@code order: {sub_agg:
 *       desc}}) is deferred — a correct implementation must keep candidate tails per shard, not
 *       just top-N, and that's a larger design.</li>
 *   <li>Single top-level terms agg at a time — callers (e.g. StreamingPartialReducer) own
 *       per-request multi-agg orchestration.</li>
 *   <li>Bucket types: whatever the incoming {@link InternalTerms} produces (StringTerms,
 *       LongTerms, DoubleTerms, UnsignedLongTerms). The reducer doesn't peek at term types.</li>
 * </ul>
 *
 * <p>Thread-safety: NOT thread-safe. Callers serialize accepts (same pattern as
 * {@code PendingReduces} in {@code QueryPhaseResultConsumer}).
 *
 * @opensearch.internal
 */
public final class StreamingTermsReducer<A extends InternalTerms<A, B>, B extends InternalTerms.Bucket<B>> {

    /** Top-N buckets we're willing to keep. Above this, displacement logic kicks in. */
    private final int topN;

    /** Reduce context used for partial (mid-flight) reduces of sub-aggregations. */
    private final ReduceContext partialReduceContext;

    /**
     * Survivor map keyed by the bucket's bytes-level identity. We use {@code getKey().toString()}
     * stored lazily as a stable identity — {@link InternalTerms.Bucket} doesn't expose a clean
     * byte key at this abstraction level, so we canonicalize via the DocValueFormat-agnostic
     * {@code compareKey} semantics by round-tripping through the bucket itself.
     *
     * <p>Using a map (instead of a priority queue) gives O(1) term lookup on incoming batches.
     * The min-docCount survivor is recomputed lazily when we need to displace.
     */
    private final Map<Object, B> survivors;

    /** Sum of doc counts that were rejected (didn't make the top-N cut). */
    private long otherDocCount;

    /** First non-null batch we see — used at finalize() to build the output. */
    private A templateBatch;

    /** Cached pointer to the current min-docCount survivor, invalidated on mutation. */
    private B cachedMin;
    private boolean cachedMinValid;

    public StreamingTermsReducer(int topN, ReduceContext partialReduceContext) {
        if (topN <= 0) {
            throw new IllegalArgumentException("topN must be positive, got " + topN);
        }
        this.topN = topN;
        this.partialReduceContext = partialReduceContext;
        // Size the map a bit above topN to absorb the transient state where a displacement is in progress
        // without triggering a rehash.
        this.survivors = new HashMap<>(topN + (topN >> 2));
    }

    /**
     * Accept a shard batch (one {@link InternalTerms} result from one shard) and fold it into
     * the running top-N. Safe to call repeatedly; idempotent-equivalent to calling
     * {@link InternalTerms#reduce} once with all batches at the end (for count-order and
     * key-order), but with memory bounded at {@code O(topN)}.
     */
    public void accept(A batch) {
        if (batch == null) {
            return;
        }
        if (templateBatch == null) {
            templateBatch = batch;
        }
        otherDocCount += batch.getSumOfOtherDocCounts();

        List<B> incoming = batch.getBuckets();
        for (B in : incoming) {
            Object key = in.getKey();
            B existing = survivors.get(key);
            if (existing != null) {
                mergeInto(existing, in);
                cachedMinValid = false;
            } else if (survivors.size() < topN) {
                survivors.put(key, in);
                cachedMinValid = false;
            } else {
                B min = currentMin();
                if (in.getDocCount() > min.getDocCount()) {
                    // Evict min, admit in.
                    otherDocCount += min.getDocCount();
                    survivors.remove(min.getKey());
                    survivors.put(key, in);
                    cachedMinValid = false;
                } else {
                    // Rejected — counts as "other"
                    otherDocCount += in.getDocCount();
                }
            }
        }
    }

    /**
     * Merge incoming bucket into existing survivor. Sums doc counts and reduces sub-aggs —
     * which for HLL cardinality merges sketches in-place via the existing
     * {@link org.opensearch.search.aggregations.metrics.InternalCardinality#reduce} path.
     */
    private void mergeInto(B existing, B incoming) {
        existing.docCount += incoming.getDocCount();
        if (existing.aggregations != null && incoming.aggregations != null) {
            List<InternalAggregations> toMerge = new ArrayList<>(2);
            toMerge.add(existing.aggregations);
            toMerge.add(incoming.aggregations);
            existing.aggregations = InternalAggregations.reduce(toMerge, partialReduceContext);
        } else if (existing.aggregations == null) {
            existing.aggregations = incoming.aggregations;
        }
        // doc_count_error accounting is intentionally not merged per-survivor here; it's
        // reconstructed at finalize() from the batch-level error counter, matching the semantics
        // of InternalTerms.reduce.
    }

    private B currentMin() {
        if (cachedMinValid && cachedMin != null && survivors.containsKey(cachedMin.getKey())) {
            return cachedMin;
        }
        B min = null;
        for (B b : survivors.values()) {
            if (min == null || b.getDocCount() < min.getDocCount()) {
                min = b;
            }
        }
        cachedMin = min;
        cachedMinValid = true;
        return min;
    }

    /**
     * Build the final {@link InternalAggregation} representing the merged state. Delegates to
     * the template batch's own reduce path for a single-element list — cheap, and picks up any
     * final-reduce-only behavior (pipeline sub-aggs, sorting, doc_count_error finalization)
     * without us having to reimplement it.
     */
    public InternalAggregation finalize(ReduceContext finalReduceContext) {
        if (templateBatch == null) {
            return null;
        }
        // Snapshot the survivors as a list in whatever order; the delegated reduce will re-sort
        // per the batch's final order. We construct a synthetic InternalTerms that carries the
        // accumulated otherDocCount — subclasses implement the protected create(...) factory
        // we need for this.
        List<B> snapshot = new ArrayList<>(survivors.values());
        BucketOrder reduceOrder = templateBatch.getReduceOrder();
        A merged = templateBatch.create(templateBatch.getName(), snapshot, reduceOrder, 0L, otherDocCount);
        List<InternalAggregation> list = new ArrayList<>(1);
        list.add(merged);
        return merged.reduce(list, finalReduceContext);
    }

    /** Number of buckets currently held. For tests/assertions. */
    public int size() {
        return survivors.size();
    }

    /** Accumulated "other" doc count. For tests/assertions. */
    public long otherDocCount() {
        return otherDocCount;
    }
}
