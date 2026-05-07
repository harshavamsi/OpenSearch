/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.streaming.StreamingCostEstimable;
import org.opensearch.search.streaming.StreamingCostMetrics;

import java.io.IOException;
import java.util.Map;

/**
 * @opensearch.internal
 */
public class ThresholdCardinalityCountAggregatorFactory extends AggregatorFactory implements StreamingCostEstimable {

    private final String groupField;
    private final String countField;
    private final int threshold;
    private final int minDocCount;
    private final int minBorderlineCount;
    private final int precision;

    ThresholdCardinalityCountAggregatorFactory(
        String name,
        String groupField,
        String countField,
        int threshold,
        int minDocCount,
        int minBorderlineCount,
        int precision,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.groupField = groupField;
        this.countField = countField;
        this.threshold = threshold;
        this.minDocCount = minDocCount;
        this.minBorderlineCount = minBorderlineCount;
        this.precision = precision;
    }

    @Override
    protected Aggregator createInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        ValuesSourceConfig groupConfig = ValuesSourceConfig.resolveUnregistered(
            searchContext.getQueryShardContext(),
            null,
            groupField,
            null,
            null,
            null,
            null,
            CoreValuesSourceType.BYTES
        );
        ValuesSourceConfig countConfig = ValuesSourceConfig.resolveUnregistered(
            searchContext.getQueryShardContext(),
            null,
            countField,
            null,
            null,
            null,
            null,
            CoreValuesSourceType.BYTES
        );

        ValuesSource groupSource = groupConfig.getValuesSource();
        ValuesSource countSource = countConfig.getValuesSource();

        if (groupSource instanceof ValuesSource.Bytes.WithOrdinals == false) {
            throw new IllegalArgumentException("group_field [" + groupField + "] must be a keyword field");
        }
        if (countSource instanceof ValuesSource.Bytes.WithOrdinals == false) {
            throw new IllegalArgumentException("count_field [" + countField + "] must be a keyword field");
        }

        return new ThresholdCardinalityCountAggregator(
            name,
            (ValuesSource.Bytes.WithOrdinals) groupSource,
            (ValuesSource.Bytes.WithOrdinals) countSource,
            threshold,
            minDocCount,
            minBorderlineCount,
            precision,
            searchContext,
            parent,
            metadata
        );
    }

    /**
     * Streaming cost estimation: TCC's shard-side algorithm is two-pass (count then replay),
     * so PER_SEGMENT is the wrong granularity — a group's doc count only exceeds the threshold
     * when the whole shard's per-group totals are known. We mark the factory streamable as a
     * signal to {@link org.opensearch.search.streaming.FlushModeResolver} that this factory
     * participates in streaming via the PER_SHARD_STREAM route (classic shard compute +
     * streaming transport + bounded coord reducer). Neutral cost metrics (topN=1) — TCC emits
     * a single scalar per outer bucket.
     */
    @Override
    public StreamingCostMetrics estimateStreamingCost(SearchContext searchContext) {
        return StreamingCostMetrics.neutral();
    }
}
