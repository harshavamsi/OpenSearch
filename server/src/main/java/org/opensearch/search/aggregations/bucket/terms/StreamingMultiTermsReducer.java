/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregation.ReduceContext;
import org.opensearch.search.aggregations.InternalAggregations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Bounded-size incremental reducer for streaming multi_terms aggregations.
 *
 * <p>This is the {@link InternalMultiTerms} analogue of {@link StreamingTermsReducer}. We can't
 * reuse that class directly because {@link InternalMultiTerms.Bucket} does not extend
 * {@link InternalTerms.Bucket} — it extends {@link InternalTerms.AbstractInternalBucket} as a
 * sibling. The logic is identical though: a bounded survivor map keyed on the composite
 * {@code List<Object>} term key, with lazy sub-agg merges deferred to {@link #finalize(ReduceContext)}.
 */
public final class StreamingMultiTermsReducer {

    private static final Logger logger = LogManager.getLogger(StreamingMultiTermsReducer.class);

    private final int topN;
    private final ReduceContext partialReduceContext;

    private final Map<Object, InternalMultiTerms.Bucket> survivors;
    private final Map<Object, List<InternalAggregations>> pendingSubAggs = new HashMap<>();

    // Identity-tracked set of outputs this reducer previously emitted, to short-circuit the
    // QueryPhaseResultConsumer partialReduce feedback loop (see StreamingTermsReducer for the
    // same pattern and rationale).
    private final Set<InternalMultiTerms> selfEmittedOutputs = java.util.Collections.newSetFromMap(new IdentityHashMap<>());

    private long otherDocCount;
    private InternalMultiTerms templateBatch;

    private InternalMultiTerms.Bucket cachedMin;
    private boolean cachedMinValid;

    // Telemetry.
    private long batchesAccepted = 0;
    private long bucketsSeen = 0;
    private long bucketsMerged = 0;
    private long bucketsAdmitted = 0;
    private long bucketsDisplaced = 0;
    private long bucketsRejected = 0;
    private int maxSurvivorMapSize = 0;

    public StreamingMultiTermsReducer(int topN, ReduceContext partialReduceContext) {
        if (topN <= 0) {
            throw new IllegalArgumentException("topN must be positive, got " + topN);
        }
        this.topN = topN;
        this.partialReduceContext = partialReduceContext;
        this.survivors = new HashMap<>(topN + (topN >> 2));
    }

    public void accept(InternalMultiTerms batch) {
        if (batch == null) {
            return;
        }
        if (selfEmittedOutputs.contains(batch)) {
            return;
        }
        if (templateBatch == null) {
            templateBatch = batch;
        }
        otherDocCount += batch.getSumOfOtherDocCounts();
        batchesAccepted++;

        List<InternalMultiTerms.Bucket> incoming = batch.getBuckets();
        for (InternalMultiTerms.Bucket in : incoming) {
            bucketsSeen++;
            Object key = in.getKey();
            InternalMultiTerms.Bucket existing = survivors.get(key);
            if (existing != null) {
                mergeInto(existing, in);
                bucketsMerged++;
                cachedMinValid = false;
            } else if (survivors.size() < topN) {
                survivors.put(key, in);
                bucketsAdmitted++;
                cachedMinValid = false;
            } else {
                InternalMultiTerms.Bucket min = currentMin();
                if (in.getDocCount() > min.getDocCount()) {
                    otherDocCount += min.getDocCount();
                    survivors.remove(min.getKey());
                    pendingSubAggs.remove(min.getKey());
                    survivors.put(key, in);
                    bucketsDisplaced++;
                    cachedMinValid = false;
                } else {
                    otherDocCount += in.getDocCount();
                    bucketsRejected++;
                }
            }
        }
        if (survivors.size() > maxSurvivorMapSize) {
            maxSurvivorMapSize = survivors.size();
        }
    }

    private void mergeInto(InternalMultiTerms.Bucket existing, InternalMultiTerms.Bucket incoming) {
        existing.docCount += incoming.getDocCount();
        if (incoming.aggregations != null) {
            List<InternalAggregations> pending = pendingSubAggs.computeIfAbsent(existing.getKey(), k -> {
                List<InternalAggregations> l = new ArrayList<>();
                if (existing.aggregations != null) {
                    l.add(existing.aggregations);
                }
                return l;
            });
            pending.add(incoming.aggregations);
        }
    }

    private InternalMultiTerms.Bucket currentMin() {
        if (cachedMinValid && cachedMin != null && survivors.containsKey(cachedMin.getKey())) {
            return cachedMin;
        }
        InternalMultiTerms.Bucket min = null;
        for (InternalMultiTerms.Bucket b : survivors.values()) {
            if (min == null || b.getDocCount() < min.getDocCount()) {
                min = b;
            }
        }
        cachedMin = min;
        cachedMinValid = true;
        return min;
    }

    public InternalAggregation finalize(ReduceContext finalReduceContext) {
        if (templateBatch == null) {
            return null;
        }
        if (pendingSubAggs.isEmpty() == false) {
            for (Map.Entry<Object, List<InternalAggregations>> e : pendingSubAggs.entrySet()) {
                InternalMultiTerms.Bucket survivor = survivors.get(e.getKey());
                if (survivor == null) {
                    continue;
                }
                List<InternalAggregations> toMerge = e.getValue();
                if (toMerge.size() == 1) {
                    survivor.aggregations = toMerge.get(0);
                } else if (toMerge.size() > 1) {
                    survivor.aggregations = InternalAggregations.reduce(toMerge, partialReduceContext);
                }
            }
            pendingSubAggs.clear();
        }
        if (logger.isDebugEnabled()) {
            logger.debug(
                "streaming_multiterms_reducer agg={} batches={} seen={} merged={} admitted={} displaced={} rejected={} "
                    + "survivors={} maxMapSize={} topN={} otherDocCount={}",
                templateBatch.getName(),
                batchesAccepted,
                bucketsSeen,
                bucketsMerged,
                bucketsAdmitted,
                bucketsDisplaced,
                bucketsRejected,
                survivors.size(),
                maxSurvivorMapSize,
                topN,
                otherDocCount
            );
        }
        List<InternalMultiTerms.Bucket> snapshot = new ArrayList<>(survivors.values());
        BucketOrder reduceOrder = templateBatch.getReduceOrder();
        InternalMultiTerms merged = templateBatch.create(templateBatch.getName(), snapshot, reduceOrder, 0L, otherDocCount);
        List<InternalAggregation> list = new ArrayList<>(1);
        list.add(merged);
        InternalAggregation reduced = merged.reduce(list, finalReduceContext);
        if (reduced instanceof InternalMultiTerms asMulti) {
            selfEmittedOutputs.add(asMulti);
        }
        return reduced;
    }

    public int size() {
        return survivors.size();
    }

    public long otherDocCount() {
        return otherDocCount;
    }

    public long batchesAccepted() {
        return batchesAccepted;
    }

    public long bucketsSeen() {
        return bucketsSeen;
    }

    public long bucketsMerged() {
        return bucketsMerged;
    }

    public long bucketsAdmitted() {
        return bucketsAdmitted;
    }

    public long bucketsDisplaced() {
        return bucketsDisplaced;
    }

    public long bucketsRejected() {
        return bucketsRejected;
    }

    public int maxSurvivorMapSize() {
        return maxSurvivorMapSize;
    }
}
