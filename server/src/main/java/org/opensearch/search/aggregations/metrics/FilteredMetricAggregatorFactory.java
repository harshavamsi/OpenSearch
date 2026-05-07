/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.streaming.StreamingCostEstimable;
import org.opensearch.search.streaming.StreamingCostMetrics;

import java.io.IOException;
import java.util.Map;

/**
 * @opensearch.internal
 */
public class FilteredMetricAggregatorFactory extends AggregatorFactory implements StreamingCostEstimable {

    private final FilteredMetricAggregationBuilder config;

    FilteredMetricAggregatorFactory(
        String name,
        FilteredMetricAggregationBuilder config,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.config = config;
    }

    private double getThreshold() {
        if (config.getFilterGt() != null) return config.getFilterGt();
        if (config.getFilterGte() != null) return config.getFilterGte() - 1;
        return 0;
    }

    @Override
    protected Aggregator createInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        AggregationBuilder bucketsBuilder = config.getBucketsAgg();
        AggregationBuilder metricBuilder = config.getMetricAgg();

        if (bucketsBuilder instanceof TermsAggregationBuilder && metricBuilder instanceof CardinalityAggregationBuilder) {
            String hint = config.getExecutionHint();
            if ("dfs".equals(hint)) {
                return createCardinalityDFS(searchContext, parent, metadata);
            }
            return createCardinalityBFS(searchContext, parent, metadata);
        }

        if (bucketsBuilder instanceof TermsAggregationBuilder) {
            NumericFilteredMetricDFSAggregator.MetricType numericType = resolveNumericType(metricBuilder);
            if (numericType != null) {
                return createNumericDFS(searchContext, parent, metadata, numericType);
            }
        }

        return createDelegating(searchContext, parent, cardinality, metadata);
    }

    private Aggregator createCardinalityBFS(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata)
        throws IOException {
        TermsAggregationBuilder termsBuilder = (TermsAggregationBuilder) config.getBucketsAgg();
        CardinalityAggregationBuilder cardBuilder = (CardinalityAggregationBuilder) config.getMetricAgg();

        double threshold = getThreshold();
        int shardMinDocCount = config.getShardMinDocCount() > 0 ? config.getShardMinDocCount() : (int) threshold + 1;
        int precision = HyperLogLogPlusPlus.precisionFromThreshold(3000);

        ValuesSourceConfig groupConfig = ValuesSourceConfig.resolveUnregistered(
            searchContext.getQueryShardContext(),
            null,
            termsBuilder.field(),
            null,
            null,
            null,
            null,
            CoreValuesSourceType.BYTES
        );

        // Build cardinality sub-agg factories
        AggregatorFactories.Builder subFactories = new AggregatorFactories.Builder();
        subFactories.addAggregator(
            new CardinalityAggregationBuilder("_metric").field(cardBuilder.field()).executionHint("deferred_ordinals")
        );

        return new CardinalityFilteredMetricAggregator(
            name,
            subFactories.build(searchContext.getQueryShardContext(), null),
            (ValuesSource.Bytes.WithOrdinals) groupConfig.getValuesSource(),
            (int) threshold,
            shardMinDocCount,
            1,
            precision,
            searchContext,
            parent,
            metadata
        );
    }

    private Aggregator createCardinalityDFS(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata)
        throws IOException {
        TermsAggregationBuilder termsBuilder = (TermsAggregationBuilder) config.getBucketsAgg();
        CardinalityAggregationBuilder cardBuilder = (CardinalityAggregationBuilder) config.getMetricAgg();

        double threshold = getThreshold();
        int shardMinDocCount = config.getShardMinDocCount() > 0 ? config.getShardMinDocCount() : (int) threshold + 1;
        int shardPassValue = config.getShardPassValue() >= 0 ? (int) config.getShardPassValue() : (int) threshold;
        int minBorderlineCount = config.getShardSendValue() >= 0 ? (int) config.getShardSendValue() : 1;
        int precision = HyperLogLogPlusPlus.precisionFromThreshold(3000);

        ValuesSourceConfig groupConfig = ValuesSourceConfig.resolveUnregistered(
            searchContext.getQueryShardContext(),
            null,
            termsBuilder.field(),
            null,
            null,
            null,
            null,
            CoreValuesSourceType.BYTES
        );
        ValuesSourceConfig countConfig = ValuesSourceConfig.resolveUnregistered(
            searchContext.getQueryShardContext(),
            null,
            cardBuilder.field(),
            null,
            null,
            null,
            null,
            CoreValuesSourceType.BYTES
        );

        return new CardinalityFilteredMetricDFSAggregator(
            name,
            (ValuesSource.Bytes.WithOrdinals) groupConfig.getValuesSource(),
            (ValuesSource.Bytes.WithOrdinals) countConfig.getValuesSource(),
            (int) threshold,
            shardMinDocCount,
            shardPassValue,
            minBorderlineCount,
            precision,
            searchContext,
            parent,
            metadata
        );
    }

    private static NumericFilteredMetricDFSAggregator.MetricType resolveNumericType(AggregationBuilder metricBuilder) {
        if (metricBuilder instanceof MaxAggregationBuilder) return NumericFilteredMetricDFSAggregator.MetricType.MAX;
        if (metricBuilder instanceof MinAggregationBuilder) return NumericFilteredMetricDFSAggregator.MetricType.MIN;
        if (metricBuilder instanceof SumAggregationBuilder) return NumericFilteredMetricDFSAggregator.MetricType.SUM;
        if (metricBuilder instanceof ValueCountAggregationBuilder) return NumericFilteredMetricDFSAggregator.MetricType.VALUE_COUNT;
        return null;
    }

    private Aggregator createNumericDFS(
        SearchContext searchContext,
        Aggregator parent,
        Map<String, Object> metadata,
        NumericFilteredMetricDFSAggregator.MetricType metricType
    ) throws IOException {
        TermsAggregationBuilder termsBuilder = (TermsAggregationBuilder) config.getBucketsAgg();
        ValuesSourceAggregationBuilder<?> numericBuilder = (ValuesSourceAggregationBuilder<?>) config.getMetricAgg();

        double threshold = getThreshold();
        int shardMinDocCount = config.getShardMinDocCount() > 0 ? config.getShardMinDocCount() : 1;
        int precision = HyperLogLogPlusPlus.precisionFromThreshold(3000);

        ValuesSourceConfig groupConfig = ValuesSourceConfig.resolveUnregistered(
            searchContext.getQueryShardContext(),
            null,
            termsBuilder.field(),
            null,
            null,
            null,
            null,
            CoreValuesSourceType.BYTES
        );
        ValuesSourceConfig metricConfig = ValuesSourceConfig.resolveUnregistered(
            searchContext.getQueryShardContext(),
            null,
            numericBuilder.field(),
            null,
            null,
            null,
            null,
            CoreValuesSourceType.NUMERIC
        );

        return new NumericFilteredMetricDFSAggregator(
            name,
            (ValuesSource.Bytes.WithOrdinals) groupConfig.getValuesSource(),
            (ValuesSource.Numeric) metricConfig.getValuesSource(),
            metricType,
            threshold,
            shardMinDocCount,
            precision,
            searchContext,
            parent,
            metadata
        );
    }

    private Aggregator createDelegating(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        AggregationBuilder bucketsBuilder = config.getBucketsAgg();
        AggregationBuilder metricBuilder = config.getMetricAgg();

        AggregatorFactories.Builder innerSubAggs = new AggregatorFactories.Builder();
        innerSubAggs.addAggregator(metricBuilder);

        AggregationBuilder innerBucketsBuilder;
        if (bucketsBuilder instanceof TermsAggregationBuilder) {
            TermsAggregationBuilder origTerms = (TermsAggregationBuilder) bucketsBuilder;
            innerBucketsBuilder = new TermsAggregationBuilder(bucketsBuilder.getName()).field(origTerms.field())
                .minDocCount(1)
                .size(Integer.MAX_VALUE);
        } else {
            innerBucketsBuilder = bucketsBuilder;
        }

        AggregatorFactory innerFactory = ((AbstractAggregationBuilder<?>) innerBucketsBuilder).subAggregations(innerSubAggs)
            .build(searchContext.getQueryShardContext(), this);

        Aggregator innerBucketAgg = innerFactory.create(searchContext, parent, cardinality);
        return new FilteredMetricAggregator(name, innerBucketAgg, config, searchContext, parent, metadata);
    }

    /**
     * Streaming cost estimation: filtered_metric is a terminal single-scalar emitter whose
     * shard algorithm (count-then-replay for cardinality, pass-threshold filter for numeric)
     * needs all segments visible before pruning, so PER_SEGMENT is wrong granularity.
     * Declaring the factory streamable with neutral cost routes this through PER_SHARD_STREAM
     * (classic shard compute + streaming transport) once the enclosing terms agg opts in.
     */
    @Override
    public StreamingCostMetrics estimateStreamingCost(SearchContext searchContext) {
        return StreamingCostMetrics.neutral();
    }
}
