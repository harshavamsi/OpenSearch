/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming;

import org.opensearch.common.settings.Settings;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for FlushModeResolver settings and decideFlushMode logic.
 */
public class FlushModeResolverTests extends OpenSearchTestCase {

    public void testSettingsDefaults() {
        assertEquals(100_000L, FlushModeResolver.STREAMING_MAX_ESTIMATED_BUCKET_COUNT.getDefault(Settings.EMPTY).longValue());
        assertEquals(0.01, FlushModeResolver.STREAMING_MIN_CARDINALITY_RATIO.getDefault(Settings.EMPTY).doubleValue(), 0.001);
        assertEquals(1000L, FlushModeResolver.STREAMING_MIN_ESTIMATED_BUCKET_COUNT.getDefault(Settings.EMPTY).longValue());
        assertEquals(1000, FlushModeResolver.STREAMING_AGGREGATION_MIN_SEGMENT_SIZE_SETTING.getDefault(Settings.EMPTY).intValue());
    }

    public void testDecideFlushModeNonStreamable() {
        StreamingCostMetrics nonStreamable = StreamingCostMetrics.nonStreamable();
        FlushMode result = FlushModeResolver.decideFlushMode(nonStreamable, FlushMode.PER_SHARD, 100_000);
        assertEquals(FlushMode.PER_SHARD, result);
    }

    public void testDecideFlushModeStreamable() {
        // topN=10 <= maxBucketCount=100_000, should stream
        StreamingCostMetrics streamable = new StreamingCostMetrics(true, 10);
        FlushMode result = FlushModeResolver.decideFlushMode(streamable, FlushMode.PER_SHARD, 100_000);
        assertEquals(FlushMode.PER_SEGMENT, result);
    }

    public void testDecideFlushModeTopNExceedsMax() {
        // topN=200_000 > maxBucketCount=100_000, should not stream
        StreamingCostMetrics highTopN = new StreamingCostMetrics(true, 200_000);
        FlushMode result = FlushModeResolver.decideFlushMode(highTopN, FlushMode.PER_SHARD, 100_000);
        assertEquals(FlushMode.PER_SHARD, result);
    }

    public void testDecideFlushModeTopNExactlyAtMax() {
        // topN=100_000 == maxBucketCount=100_000, should stream (<=)
        StreamingCostMetrics exactMatch = new StreamingCostMetrics(true, 100_000);
        FlushMode result = FlushModeResolver.decideFlushMode(exactMatch, FlushMode.PER_SHARD, 100_000);
        assertEquals(FlushMode.PER_SEGMENT, result);
    }

    public void testDecideFlushModeNeutralMetrics() {
        // Neutral metrics have topN=1, which is <= any reasonable max, so should stream
        StreamingCostMetrics neutral = StreamingCostMetrics.neutral();
        FlushMode result = FlushModeResolver.decideFlushMode(neutral, FlushMode.PER_SHARD, 100_000);
        assertEquals(FlushMode.PER_SEGMENT, result);
    }

    public void testDecideFlushModeSmallTopN() {
        // Very small topN (cardinality case), should stream
        StreamingCostMetrics smallTopN = new StreamingCostMetrics(true, 1);
        FlushMode result = FlushModeResolver.decideFlushMode(smallTopN, FlushMode.PER_SHARD, 100_000);
        assertEquals(FlushMode.PER_SEGMENT, result);
    }

    public void testDecideFlushModePrefersPerShardStreamForCardinalitySubAgg() {
        // terms→cardinality shape: PER_SEGMENT pays per-segment protocol overhead and
        // cross-segment sketch merge amplification. PER_SHARD_STREAM should win.
        StreamingCostMetrics streamable = new StreamingCostMetrics(true, 10);
        AggregatorFactories.Builder aggs = new AggregatorFactories.Builder().addAggregator(
            new TermsAggregationBuilder("by_term").field("term").subAggregation(new CardinalityAggregationBuilder("card").field("other"))
        );
        FlushMode result = FlushModeResolver.decideFlushMode(streamable, FlushMode.PER_SHARD, 100_000, aggs);
        assertEquals(FlushMode.PER_SHARD_STREAM, result);
    }

    public void testDecideFlushModeWithoutCardinalityKeepsPerSegment() {
        // terms→max shape: no sketch state, PER_SEGMENT's first-byte latency win applies.
        StreamingCostMetrics streamable = new StreamingCostMetrics(true, 10);
        AggregatorFactories.Builder aggs = new AggregatorFactories.Builder().addAggregator(
            new TermsAggregationBuilder("by_term").field("term").subAggregation(new MaxAggregationBuilder("max_price").field("price"))
        );
        FlushMode result = FlushModeResolver.decideFlushMode(streamable, FlushMode.PER_SHARD, 100_000, aggs);
        assertEquals(FlushMode.PER_SEGMENT, result);
    }

    public void testDecideFlushModeDetectsCardinalityNestedDeeply() {
        // terms→terms→cardinality: the detector must recurse into sub-aggregations.
        StreamingCostMetrics streamable = new StreamingCostMetrics(true, 10);
        AggregatorFactories.Builder aggs = new AggregatorFactories.Builder().addAggregator(
            new TermsAggregationBuilder("outer").field("outer_field")
                .subAggregation(
                    new TermsAggregationBuilder("inner").field("inner_field")
                        .subAggregation(new CardinalityAggregationBuilder("card").field("tracked"))
                )
        );
        FlushMode result = FlushModeResolver.decideFlushMode(streamable, FlushMode.PER_SHARD, 100_000, aggs);
        assertEquals(FlushMode.PER_SHARD_STREAM, result);
    }

    public void testDecideFlushModeNonStreamableStillFallsBackEvenWithCardinality() {
        // Non-streamable metrics trump the PER_SHARD_STREAM heuristic.
        StreamingCostMetrics nonStreamable = StreamingCostMetrics.nonStreamable();
        AggregatorFactories.Builder aggs = new AggregatorFactories.Builder().addAggregator(
            new TermsAggregationBuilder("by_term").field("term").subAggregation(new CardinalityAggregationBuilder("card").field("other"))
        );
        FlushMode result = FlushModeResolver.decideFlushMode(nonStreamable, FlushMode.PER_SHARD, 100_000, aggs);
        assertEquals(FlushMode.PER_SHARD, result);
    }

    public void testHasStatefulMetricSubAggDetectsCardinality() {
        AggregatorFactories.Builder withCard = new AggregatorFactories.Builder().addAggregator(
            new TermsAggregationBuilder("by_term").field("term").subAggregation(new CardinalityAggregationBuilder("c").field("other"))
        );
        assertTrue(FlushModeResolver.hasStatefulMetricSubAgg(withCard));

        AggregatorFactories.Builder withoutCard = new AggregatorFactories.Builder().addAggregator(
            new TermsAggregationBuilder("by_term").field("term").subAggregation(new MaxAggregationBuilder("m").field("price"))
        );
        assertFalse(FlushModeResolver.hasStatefulMetricSubAgg(withoutCard));
    }
}
