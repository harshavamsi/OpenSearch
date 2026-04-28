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
import org.opensearch.common.util.LongArray;
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

    // group global ordinal → bucket ordinal mapping (0 = unassigned)
    private LongArray groupOrdToBucketOrd;
    private long nextBucketOrd = 1; // start from 1, 0 means unassigned

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
        this.groupOrdToBucketOrd = context.bigArrays().newLongArray(1, true); // 0 = unassigned
    }

    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        // Defer all sub-aggregators (the cardinality metric)
        return true;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        final SortedSetDocValues groupOrds = groupSource.globalOrdinalsValues(ctx);

        return new LeafBucketCollectorBase(sub, groupOrds) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (groupOrds.advanceExact(doc) == false) return;
                long groupOrd = groupOrds.nextOrd();
                if (groupOrd == SortedSetDocValues.NO_MORE_DOCS) return;

                // Map group ordinal to bucket ordinal
                groupOrdToBucketOrd = context.bigArrays().grow(groupOrdToBucketOrd, groupOrd + 1);
                long bucketOrd = groupOrdToBucketOrd.get(groupOrd);
                if (bucketOrd == 0) {
                    bucketOrd = nextBucketOrd++;
                    groupOrdToBucketOrd.set(groupOrd, bucketOrd);
                    collectBucket(sub, doc, bucketOrd);
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
        // Find eligible buckets (docCount >= minDocCount)
        List<Long> eligibleBucketOrds = new ArrayList<>();
        Map<Long, Long> bucketOrdToGroupOrd = new HashMap<>();

        for (long groupOrd = 0; groupOrd < groupOrdToBucketOrd.size(); groupOrd++) {
            long bucketOrd = groupOrdToBucketOrd.get(groupOrd);
            if (bucketOrd > 0 && bucketDocCount(bucketOrd) >= minDocCount) {
                eligibleBucketOrds.add(bucketOrd);
                bucketOrdToGroupOrd.put(bucketOrd, groupOrd);
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

        // Classify each eligible bucket
        SortedSetDocValues groupGlobalOrds = groupSource.globalOrdinalsValues(context.searcher().getIndexReader().leaves().get(0));
        MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();

        try (HyperLogLogPlusPlus hll = new HyperLogLogPlusPlus(precision, context.bigArrays(), 1)) {
            Map<Long, Object> borderline = new HashMap<>();

            for (int idx = 0; idx < selectedOrds.length; idx++) {
                long bucketOrd = selectedOrds[idx];
                long groupOrd = bucketOrdToGroupOrd.get(bucketOrd);

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

                // Hash group key
                BytesRef groupValue = groupGlobalOrds.lookupOrd(groupOrd);
                MurmurHash3.hash128(groupValue.bytes, groupValue.offset, groupValue.length, 0, hash);
                long groupHash = hash.h1;

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
        add.accept("total_groups", nextBucketOrd);
        long eligible = 0;
        for (long i = 0; i < nextBucketOrd; i++) {
            if (bucketDocCount(i) >= minDocCount) eligible++;
        }
        add.accept("groups_eligible", eligible);
    }

    @Override
    protected void doClose() {
        Releasables.close(groupOrdToBucketOrd);
    }
}
