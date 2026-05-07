/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.LongHash;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Optimized filtered_metric for terms + cardinality. Extends {@link DeferableBucketAggregator}
 * to leverage {@link org.opensearch.search.aggregations.bucket.BestBucketsDeferringCollector}
 * for BFS replay — no manual PackedLongValues recording needed.
 *
 * <p>Acts as a bucket aggregator on group_field (like terms agg). The cardinality metric
 * is a deferred sub-aggregator that only runs for eligible buckets (docCount >= minDocCount).
 *
 * @opensearch.internal
 */
public class CardinalityFilteredMetricAggregator extends DeferableBucketAggregator {

    private final ValuesSource.Bytes.WithOrdinals groupSource;
    private final int threshold;
    private final int minDocCount;
    private final int minBorderlineCount;
    private final int precision;

    // Stable per-shard identity: MurmurHash3 h1 of the group BytesRef → internal bucketOrd.
    // Same hash space that {@link InternalFilteredMetric} uses on the wire for HLL + borderline keys,
    // so we avoid a second hash at buildAggregation.
    private final LongHash bucketOrds;

    CardinalityFilteredMetricAggregator(
        String name,
        AggregatorFactories factories,
        ValuesSource.Bytes.WithOrdinals groupSource,
        int threshold,
        int minDocCount,
        int minBorderlineCount,
        int precision,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, metadata);
        this.groupSource = groupSource;
        this.threshold = threshold;
        this.minDocCount = minDocCount;
        this.minBorderlineCount = minBorderlineCount;
        this.precision = precision;
        this.bucketOrds = new LongHash(1, context.bigArrays());
    }

    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        // Defer all sub-aggregators (the cardinality metric)
        return true;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        // Segment-local ords — we don't need the global OrdinalMap to be built.
        final SortedSetDocValues groupOrds = groupSource.ordinalsValues(ctx);
        final int segOrdCount = Math.toIntExact(groupOrds.getValueCount());
        // Per-segment cache: segment-local ord → shard-scoped internal bucketOrd. -1 = not yet seen.
        final long[] segOrdToBucketOrd = new long[segOrdCount];
        Arrays.fill(segOrdToBucketOrd, -1L);
        final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();

        return new LeafBucketCollectorBase(sub, groupOrds) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (groupOrds.advanceExact(doc) == false) return;
                long segOrd = groupOrds.nextOrd();
                if (segOrd == SortedSetDocValues.NO_MORE_DOCS) return;

                long bucketOrd = segOrdToBucketOrd[(int) segOrd];
                if (bucketOrd == -1L) {
                    BytesRef groupValue = groupOrds.lookupOrd(segOrd);
                    MurmurHash3.hash128(groupValue.bytes, groupValue.offset, groupValue.length, 0, hash);
                    long key = hash.h1;
                    long added = bucketOrds.add(key);
                    if (added < 0) {
                        bucketOrd = -1 - added;
                        segOrdToBucketOrd[(int) segOrd] = bucketOrd;
                        collectExistingBucket(sub, doc, bucketOrd);
                    } else {
                        bucketOrd = added;
                        segOrdToBucketOrd[(int) segOrd] = bucketOrd;
                        collectBucket(sub, doc, bucketOrd);
                    }
                } else {
                    collectExistingBucket(sub, doc, bucketOrd);
                }
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        // For each owning bucket, find eligible group buckets and build results
        InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
        for (int i = 0; i < owningBucketOrds.length; i++) {
            results[i] = buildSingle(owningBucketOrds[i]);
        }
        return results;
    }

    private InternalAggregation buildSingle(long owningBucketOrd) throws IOException {
        // Find eligible buckets (docCount >= minDocCount). bucketOrds.size() is the number of
        // distinct groups seen so far; bucket ids run [0, size).
        List<Long> eligibleBucketOrds = new ArrayList<>();
        for (long bucketOrd = 0; bucketOrd < bucketOrds.size(); bucketOrd++) {
            if (bucketDocCount(bucketOrd) >= minDocCount) {
                eligibleBucketOrds.add(bucketOrd);
            }
        }

        if (eligibleBucketOrds.isEmpty()) return buildEmptyAggregation();

        // Trigger BFS replay for eligible buckets only
        long[] selectedOrds = eligibleBucketOrds.stream().mapToLong(Long::longValue).toArray();
        prepareSelectedBuckets(selectedOrds);

        // Build sub-agg results for selected buckets
        InternalAggregation[][] subAggResults = new InternalAggregation[subAggregators.length][];
        for (int i = 0; i < subAggregators.length; i++) {
            subAggResults[i] = subAggregators[i].buildAggregations(selectedOrds);
        }

        try (HyperLogLogPlusPlus hll = new HyperLogLogPlusPlus(precision, context.bigArrays(), 1)) {
            Map<Long, Object> borderline = new HashMap<>();

            for (int idx = 0; idx < selectedOrds.length; idx++) {
                long bucketOrd = selectedOrds[idx];

                // Get cardinality from sub-agg result
                InternalAggregations subAggs = InternalAggregations.from(buildSubAggsForBucket(subAggResults, idx));
                double metricValue = 0;
                InternalAggregation metricAgg = null;
                if (subAggs.asList().isEmpty() == false) {
                    metricAgg = (InternalAggregation) subAggs.asList().get(0);
                    if (metricAgg instanceof InternalNumericMetricsAggregation.SingleValue) {
                        metricValue = ((InternalNumericMetricsAggregation.SingleValue) metricAgg).value();
                    }
                }

                // The 64-bit group hash is the key we stored in LongHash during collect.
                long groupHash = bucketOrds.get(bucketOrd);

                if (metricValue > threshold) {
                    hll.collect(0, groupHash);
                } else if (metricValue >= minBorderlineCount) {
                    Object compactData = extractBorderlineData(metricAgg, metricValue);
                    if (compactData != null) {
                        borderline.put(groupHash, compactData);
                    }
                }
            }

            AbstractHyperLogLogPlusPlus passedCopy = hll.cardinality(0) > 0 ? hll.clone(0, BigArrays.NON_RECYCLING_INSTANCE) : null;

            return new InternalFilteredMetric(name, passedCopy, borderline, threshold, precision, metadata());
        }
    }

    private List<InternalAggregation> buildSubAggsForBucket(InternalAggregation[][] subAggResults, int bucketIdx) {
        List<InternalAggregation> result = new ArrayList<>(subAggResults.length);
        for (InternalAggregation[] subAggResult : subAggResults) {
            result.add(subAggResult[bucketIdx]);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private Object extractBorderlineData(InternalAggregation metricAgg, double metricValue) {
        if (metricAgg instanceof InternalCardinality) {
            InternalCardinality card = (InternalCardinality) metricAgg;
            AbstractHyperLogLogPlusPlus counts = card.getCounts();
            if (counts instanceof HyperLogLogPlusPlusSparse) {
                Set<Long> hashes = new HashSet<>();
                AbstractLinearCounting.HashesIterator iter = ((HyperLogLogPlusPlusSparse) counts).getLinearCounting(0);
                while (iter.next()) {
                    hashes.add((long) iter.value());
                }
                return hashes;
            }
            return metricValue;
        }
        return metricValue;
    }

    private void prepareSelectedBuckets(long[] selectedBuckets) throws IOException {
        // This triggers BestBucketsDeferringCollector.prepareSelectedBuckets
        // which replays recorded docs only for selected buckets
        beforeBuildingBuckets(selectedBuckets);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalFilteredMetric(name, null, new HashMap<>(), threshold, precision, metadata());
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("threshold", threshold);
        add.accept("min_doc_count", minDocCount);
        add.accept("min_borderline_count", minBorderlineCount);
        add.accept("total_groups", bucketOrds.size());
        long eligible = 0;
        for (long i = 0; i < bucketOrds.size(); i++) {
            if (bucketDocCount(i) >= minDocCount) eligible++;
        }
        add.accept("groups_eligible", eligible);
    }

    @Override
    protected void doClose() {
        Releasables.close(bucketOrds);
    }
}
