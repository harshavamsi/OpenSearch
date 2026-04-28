/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.util.BigArrays;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorBase;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Delegates collection to an inner bucket aggregator. At buildAggregation,
 * iterates inner bucket results, classifies each as passed/borderline/dropped,
 * and returns a compact {@link InternalFilteredMetric}.
 *
 * @opensearch.internal
 */
public class FilteredMetricAggregator extends AggregatorBase {

    private final Aggregator innerBucketAgg;
    private final FilteredMetricAggregationBuilder config;
    private final int precision;

    FilteredMetricAggregator(
        String name,
        Aggregator innerBucketAgg,
        FilteredMetricAggregationBuilder config,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, AggregatorFactories.EMPTY, context, parent, CardinalityUpperBound.NONE, metadata);
        this.innerBucketAgg = innerBucketAgg;
        this.config = config;
        this.precision = HyperLogLogPlusPlus.precisionFromThreshold(3000);
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        return innerBucketAgg.getLeafCollector(ctx);
    }

    @Override
    protected void doPreCollection() throws IOException {
        innerBucketAgg.preCollection();
    }

    @Override
    protected void doPostCollection() throws IOException {
        innerBucketAgg.postCollection();
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
        for (int i = 0; i < owningBucketOrds.length; i++) {
            results[i] = buildSingleAggregation(owningBucketOrds[i]);
        }
        return results;
    }

    private InternalAggregation buildSingleAggregation(long owningBucketOrd) throws IOException {
        InternalAggregation[] innerResults = innerBucketAgg.buildAggregations(new long[] { owningBucketOrd });
        InternalAggregation innerResult = innerResults[0];

        if (innerResult instanceof InternalTerms == false) {
            return buildEmptyAggregation();
        }

        InternalTerms<?, ?> terms = (InternalTerms<?, ?>) innerResult;
        MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();

        double threshold = getThreshold();
        double shardSendValue = config.getShardSendValue() >= 0 ? config.getShardSendValue() : 1;

        try (HyperLogLogPlusPlus passedHLL = new HyperLogLogPlusPlus(precision, context.bigArrays(), 1)) {
            Map<Long, Object> borderline = new HashMap<>();
            String metricName = config.getMetricAgg().getName();

            for (Object rawBucket : terms.getBuckets()) {
                InternalTerms.Bucket<?> bucket = (InternalTerms.Bucket<?>) rawBucket;
                InternalAggregation metricAgg = bucket.getAggregations().get(metricName);
                if (metricAgg == null) continue;

                double metricValue = getMetricValue(metricAgg);

                byte[] keyBytes = bucket.getKeyAsString().getBytes();
                MurmurHash3.hash128(keyBytes, 0, keyBytes.length, 0, hash);
                long bucketKeyHash = hash.h1;

                if (meetsFilter(metricValue)) {
                    passedHLL.collect(0, bucketKeyHash);
                } else if (metricValue >= shardSendValue) {
                    Object compactData = extractBorderlineData(metricAgg, metricValue);
                    if (compactData != null) {
                        borderline.put(bucketKeyHash, compactData);
                    }
                }
            }

            AbstractHyperLogLogPlusPlus passedCopy = passedHLL.cardinality(0) > 0
                ? passedHLL.clone(0, BigArrays.NON_RECYCLING_INSTANCE)
                : null;

            return new InternalFilteredMetric(name, passedCopy, borderline, threshold, precision, metadata());
        }
    }

    private double getMetricValue(InternalAggregation metricAgg) {
        if (metricAgg instanceof InternalNumericMetricsAggregation.SingleValue) {
            return ((InternalNumericMetricsAggregation.SingleValue) metricAgg).value();
        }
        return 0;
    }

    private boolean meetsFilter(double value) {
        if (config.getFilterGt() != null && value <= config.getFilterGt()) return false;
        if (config.getFilterGte() != null && value < config.getFilterGte()) return false;
        if (config.getFilterLt() != null && value >= config.getFilterLt()) return false;
        if (config.getFilterLte() != null && value > config.getFilterLte()) return false;
        return true;
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

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalFilteredMetric(name, null, new HashMap<>(), getThreshold(), precision, metadata());
    }

    private double getThreshold() {
        if (config.getFilterGt() != null) return config.getFilterGt();
        if (config.getFilterGte() != null) return config.getFilterGte() - 1;
        return 0;
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        add.accept("inner_bucket_agg", config.getBucketsAgg().getType());
        add.accept("metric_type", config.getMetricAgg().getType());
        if (config.getFilterGt() != null) add.accept("filter_gt", config.getFilterGt());
    }

    @Override
    protected void doClose() {
        // innerBucketAgg is registered with SearchContext and closed by it
    }
}
