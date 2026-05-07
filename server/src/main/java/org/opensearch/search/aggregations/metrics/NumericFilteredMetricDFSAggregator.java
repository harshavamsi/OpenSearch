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
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.DoubleArray;
import org.opensearch.common.util.LongArray;
import org.opensearch.common.util.LongHash;
import org.opensearch.common.util.ObjectArray;
import org.opensearch.index.fielddata.NumericDoubleValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorBase;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Single-pass (DFS) filtered_metric for terms + numeric metric (max, min, sum, value_count).
 * Uses a DoubleArray accumulator per group — much cheaper than Roaring bitmaps.
 *
 * <p>For max/min: per-shard value fully determines pass/fail — no borderline needed.
 * For sum/value_count: borderline sends partial Double for cross-shard summation.
 *
 * @opensearch.internal
 */
public class NumericFilteredMetricDFSAggregator extends AggregatorBase {

    enum MetricType {
        MAX,
        MIN,
        SUM,
        VALUE_COUNT
    }

    private final ValuesSource.Bytes.WithOrdinals groupSource;
    private final ValuesSource.Numeric metricSource;
    private final MetricType metricType;
    private final double threshold;
    private final int minDocCount;
    private final int precision;
    private final BigArrays bigArrays;

    // Shard-scoped: MurmurHash3 h1 of group BytesRef → stable bucketOrd. Same hash
    // that InternalFilteredMetric uses on the wire, so no second hash at build time.
    private final LongHash bucketOrds;

    // Per parent bucket
    private ObjectArray<PerParentState> states;

    NumericFilteredMetricDFSAggregator(
        String name,
        ValuesSource.Bytes.WithOrdinals groupSource,
        ValuesSource.Numeric metricSource,
        MetricType metricType,
        double threshold,
        int minDocCount,
        int precision,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, AggregatorFactories.EMPTY, context, parent, CardinalityUpperBound.NONE, metadata);
        this.groupSource = groupSource;
        this.metricSource = metricSource;
        this.metricType = metricType;
        this.threshold = threshold;
        this.minDocCount = minDocCount;
        this.precision = precision;
        this.bigArrays = context.bigArrays();
        this.bucketOrds = new LongHash(1, bigArrays);
        this.states = bigArrays.newObjectArray(1);
    }

    private double initialValue() {
        switch (metricType) {
            case MAX:
                return Double.NEGATIVE_INFINITY;
            case MIN:
                return Double.POSITIVE_INFINITY;
            default:
                return 0.0;
        }
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        final SortedSetDocValues groupOrds = groupSource.ordinalsValues(ctx);
        final int segOrdCount = Math.toIntExact(groupOrds.getValueCount());
        // Per-segment cache: segment-local ord → shard-scoped bucketOrd. -1 = unseen.
        final long[] segOrdToBucketOrd = new long[segOrdCount];
        Arrays.fill(segOrdToBucketOrd, -1L);
        final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();

        final SortedNumericDoubleValues allValues = metricSource.doubleValues(ctx);
        final NumericDoubleValues values = metricType == MetricType.MAX ? MultiValueMode.MAX.select(allValues)
            : metricType == MetricType.MIN ? MultiValueMode.MIN.select(allValues)
            : MultiValueMode.SUM.select(allValues);

        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long parentBucket) throws IOException {
                if (groupOrds.advanceExact(doc) == false) return;
                long segOrd = groupOrds.nextOrd();
                if (segOrd == SortedSetDocValues.NO_MORE_DOCS) return;

                long bucketOrd = segOrdToBucketOrd[(int) segOrd];
                if (bucketOrd == -1L) {
                    BytesRef gv = groupOrds.lookupOrd(segOrd);
                    MurmurHash3.hash128(gv.bytes, gv.offset, gv.length, 0, hash);
                    long added = bucketOrds.add(hash.h1);
                    bucketOrd = added < 0 ? -1 - added : added;
                    segOrdToBucketOrd[(int) segOrd] = bucketOrd;
                }

                states = bigArrays.grow(states, parentBucket + 1);
                PerParentState state = states.get(parentBucket);
                if (state == null) {
                    state = new PerParentState(bigArrays, metricType);
                    states.set(parentBucket, state);
                }

                state.collect(bucketOrd, doc, values);
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
        for (int i = 0; i < owningBucketOrds.length; i++) {
            results[i] = buildSingle(owningBucketOrds[i]);
        }
        return results;
    }

    private InternalAggregation buildSingle(long owningBucketOrd) throws IOException {
        if (owningBucketOrd >= states.size()) return buildEmptyAggregation();
        PerParentState state = states.get(owningBucketOrd);
        if (state == null) return buildEmptyAggregation();

        try (HyperLogLogPlusPlus hll = new HyperLogLogPlusPlus(precision, bigArrays, 1)) {
            Map<Long, Object> borderline = new HashMap<>();
            long passed = 0;
            long borderlineSent = 0;

            for (long g = 0; g < state.docCounts.size(); g++) {
                long dc = state.docCounts.get(g);
                if (dc < minDocCount) continue;

                double val = state.metricValues.get(g);
                // LongHash key is the group hash — direct lookup, no lookupOrd needed.
                long groupHash = bucketOrds.get(g);

                if (metricType == MetricType.MAX || metricType == MetricType.MIN) {
                    if (val > threshold) {
                        hll.collect(0, groupHash);
                        passed++;
                    }
                } else {
                    if (val > threshold) {
                        hll.collect(0, groupHash);
                        passed++;
                    } else if (val > 0) {
                        borderline.put(groupHash, val);
                        borderlineSent++;
                    }
                }
            }

            AbstractHyperLogLogPlusPlus passedCopy = hll.cardinality(0) > 0 ? hll.clone(0, BigArrays.NON_RECYCLING_INSTANCE) : null;

            return new InternalFilteredMetric(name, passedCopy, borderline, threshold, precision, metadata());
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalFilteredMetric(name, null, new HashMap<>(), threshold, precision, metadata());
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        add.accept("execution_hint", "dfs");
        add.accept("metric_type", metricType.name().toLowerCase());
        add.accept("threshold", threshold);
        add.accept("min_doc_count", minDocCount);
        long totalGroups = 0, eligible = 0;
        if (states != null) {
            for (long i = 0; i < states.size(); i++) {
                PerParentState s = states.get(i);
                if (s != null) {
                    totalGroups += s.totalGroups;
                    eligible += s.groupsEligible;
                }
            }
        }
        add.accept("total_groups", totalGroups);
        add.accept("groups_eligible", eligible);
    }

    @Override
    protected void doClose() {
        if (states != null) {
            for (long i = 0; i < states.size(); i++) {
                PerParentState s = states.get(i);
                if (s != null) s.close();
            }
        }
        Releasables.close(states, bucketOrds);
    }

    /**
     * Per-parent-bucket state: doc counts and metric accumulators.
     */
    static class PerParentState implements Releasable {
        private final BigArrays bigArrays;
        private final MetricType metricType;
        private final double initVal;
        LongArray docCounts;
        DoubleArray metricValues;
        long totalGroups;
        long groupsEligible;

        PerParentState(BigArrays bigArrays, MetricType metricType) {
            this.bigArrays = bigArrays;
            this.metricType = metricType;
            this.initVal = metricType == MetricType.MAX ? Double.NEGATIVE_INFINITY
                : metricType == MetricType.MIN ? Double.POSITIVE_INFINITY
                : 0.0;
            this.docCounts = bigArrays.newLongArray(1, true);
            this.metricValues = bigArrays.newDoubleArray(1, false);
            this.metricValues.fill(0, 1, initVal);
        }

        void collect(long groupOrd, int doc, NumericDoubleValues values) throws IOException {
            long needed = groupOrd + 1;
            if (needed > docCounts.size()) {
                long oldSize = metricValues.size();
                docCounts = bigArrays.grow(docCounts, needed);
                metricValues = bigArrays.grow(metricValues, needed);
                metricValues.fill(oldSize, metricValues.size(), initVal);
            }

            long count = docCounts.increment(groupOrd, 1);
            if (count == 1) totalGroups++;

            if (values.advanceExact(doc)) {
                double val = values.doubleValue();
                switch (metricType) {
                    case MAX:
                        if (val > metricValues.get(groupOrd)) metricValues.set(groupOrd, val);
                        break;
                    case MIN:
                        if (val < metricValues.get(groupOrd)) metricValues.set(groupOrd, val);
                        break;
                    case SUM:
                        metricValues.increment(groupOrd, val);
                        break;
                    case VALUE_COUNT:
                        metricValues.increment(groupOrd, 1.0);
                        break;
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(docCounts, metricValues);
        }
    }
}
