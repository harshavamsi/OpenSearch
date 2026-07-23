/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;

import java.util.Collection;

/**
 * Determines optimal {@link FlushMode} for streaming aggregations based on cost metrics.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class FlushModeResolver {

    private static final Logger logger = LogManager.getLogger(FlushModeResolver.class);

    private FlushModeResolver() {}

    /**
     * Minimum segment-level size for streaming aggregations to ensure accuracy.
     * This applies per-segment in streaming mode to control the topN buckets collected.
     * Default is 1000. Can be adjusted based on accuracy requirements.
     */
    public static final Setting<Integer> STREAMING_AGGREGATION_MIN_SEGMENT_SIZE_SETTING = Setting.intSetting(
        "index.aggregation.streaming.min_segment_size",
        1000,
        1,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    /**
     * Maximum estimated bucket count allowed for streaming aggregations.
     * If an aggregation is estimated to produce more buckets than this threshold,
     * traditional shard-level processing will be used instead of streaming.
     * This prevents coordinator overload from processing too many streaming buckets.
     */
    public static final Setting<Long> STREAMING_MAX_ESTIMATED_BUCKET_COUNT = Setting.longSetting(
        "search.aggregations.streaming.max_estimated_bucket_count",
        100_000L,
        1L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Minimum cardinality ratio required for streaming aggregations.
     * Calculated as (estimated_buckets / documents_with_field).
     * If the ratio is below this threshold, traditional processing is used
     * to prevent performance regression on low-cardinality data.
     * Range: 0.0 to 1.0, where 0.01 means at least 1% unique values.
     */
    public static final Setting<Double> STREAMING_MIN_CARDINALITY_RATIO = Setting.doubleSetting(
        "search.aggregations.streaming.min_cardinality_ratio",
        0.01,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Minimum estimated bucket count required for streaming aggregations.
     * If an aggregation is estimated to produce fewer buckets than this threshold,
     * traditional processing is used to avoid streaming overhead for small result sets.
     */
    public static final Setting<Long> STREAMING_MIN_ESTIMATED_BUCKET_COUNT = Setting.longSetting(
        "search.aggregations.streaming.min_estimated_bucket_count",
        1000L,
        1L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Evaluates cost metrics to determine if streaming is beneficial.
     *
     * @param metrics combined cost metrics from the factory tree
     * @param defaultMode fallback mode when streaming is not beneficial
     * @param maxBucketCount maximum bucket count threshold
     * @return {@link FlushMode#PER_SEGMENT} if streaming is beneficial, otherwise the default mode
     */
    public static FlushMode decideFlushMode(StreamingCostMetrics metrics, FlushMode defaultMode, long maxBucketCount) {
        return decideFlushMode(metrics, defaultMode, maxBucketCount, null);
    }

    /**
     * Evaluates cost metrics to determine the right flush mode, with awareness of the aggregation
     * tree's shape so we can prefer {@link FlushMode#PER_SHARD_STREAM} on shapes where
     * {@link FlushMode#PER_SEGMENT} pays structural overhead (per-segment protocol framing,
     * cross-segment sketch merge amplification, redundant term identification).
     *
     * <p>Current heuristic: prefer {@code PER_SHARD_STREAM} when the tree contains a stateful
     * metric sub-aggregation (today: {@code cardinality}, which carries an HLL sketch). For
     * these shapes, classic shard-level compute plus streaming transport gives the same shard
     * latency as classic while keeping the coordinator heap bounded via the streaming consumer.
     *
     * @param aggregations the aggregation tree, used for shape-aware heuristics; may be null
     */
    public static FlushMode decideFlushMode(
        StreamingCostMetrics metrics,
        FlushMode defaultMode,
        long maxBucketCount,
        AggregatorFactories.Builder aggregations
    ) {
        if (!metrics.streamable()) {
            return defaultMode;
        }
        // For shapes with stateful metric sub-aggs (HLL sketches etc.), PER_SEGMENT's
        // cross-segment merge amplification is more expensive than classic compute. Route
        // those through PER_SHARD_STREAM so the shard builds one merged state via global
        // ordinals and the coord still gets bounded incremental reduce.
        if (aggregations != null && hasStatefulMetricSubAgg(aggregations)) {
            return FlushMode.PER_SHARD_STREAM;
        }
        // Prevent coordinator overload with too many buckets
        if (metrics.topNSize() <= maxBucketCount) {
            return FlushMode.PER_SEGMENT;
        }
        return defaultMode;
    }

    /**
     * True if the aggregation tree contains a stateful metric sub-agg: {@link CardinalityAggregationBuilder}.
     * It carries HLL sketches that are expensive to merge across per-segment partials, so we prefer
     * PER_SHARD_STREAM for it.
     *
     * <p>multi_terms + cardinality routes through here too: classic shard compute emits one HLL
     * per composite key total (vs one HLL per bucket per segment on PER_SEGMENT), and the Flight
     * outbound handler runs columnar eligibility on the single final QSR batch — so we still get
     * Arrow-columnar transport without paying per-segment HLL re-serialization amplification.
     */
    public static boolean hasStatefulMetricSubAgg(AggregatorFactories.Builder aggregations) {
        for (AggregationBuilder agg : aggregations.getAggregatorFactories()) {
            if (containsStatefulMetric(agg)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsStatefulMetric(AggregationBuilder agg) {
        if (agg instanceof CardinalityAggregationBuilder) {
            return true;
        }
        for (AggregationBuilder sub : agg.getSubAggregations()) {
            if (containsStatefulMetric(sub)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Determines if an aggregation tree is eligible for streaming based on aggregation types.
     *
     * <p>Streaming aggregations support:
     * <ul>
     *   <li>Top level: terms aggregations (string or numeric)</li>
     *   <li>Sub-aggregations: numeric terms, cardinality, max, min, sum</li>
     * </ul>
     *
     * @param aggregations the aggregation factories to validate
     * @return true if all aggregations are eligible for streaming, false otherwise
     */
    public static boolean isStreamable(AggregatorFactories.Builder aggregations) {
        if (aggregations == null || aggregations.count() == 0) {
            logger.debug("streaming gate: reject, reason=no_aggregations");
            return false;
        }

        Collection<AggregationBuilder> topLevelAggs = aggregations.getAggregatorFactories();
        for (AggregationBuilder agg : topLevelAggs) {
            String reason = topLevelStreamableReason(agg);
            if (reason != null) {
                logger.debug("streaming gate: reject, agg={}, reason={}", agg.getName(), reason);
                return false;
            }
        }
        return true;
    }

    /**
     * @return null if the top-level agg is streamable, otherwise a short reason code
     *         ({@code unsupported_top_level:<type>}, {@code unsupported_sub_agg:...}).
     */
    private static String topLevelStreamableReason(AggregationBuilder agg) {
        if (!(agg instanceof TermsAggregationBuilder) && !(agg instanceof MultiTermsAggregationBuilder)) {
            return "unsupported_top_level:" + agg.getType();
        }

        for (AggregationBuilder subAgg : agg.getSubAggregations()) {
            String subReason = subAggregationStreamableReason(subAgg);
            if (subReason != null) {
                return subReason;
            }
        }
        return null;
    }

    private static String subAggregationStreamableReason(AggregationBuilder agg) {
        if (agg instanceof TermsAggregationBuilder) {
            for (AggregationBuilder nestedAgg : agg.getSubAggregations()) {
                if (!isMetricAggregation(nestedAgg)) {
                    return "unsupported_level3:" + nestedAgg.getType() + " under " + agg.getName();
                }
            }
            return null;
        }
        if (isMetricAggregation(agg)) {
            return null;
        }
        return "unsupported_sub_agg:" + agg.getType() + " (" + agg.getName() + ")";
    }

    private static boolean isMetricAggregation(AggregationBuilder agg) {
        return agg instanceof CardinalityAggregationBuilder
            || agg instanceof MaxAggregationBuilder
            || agg instanceof MinAggregationBuilder
            || agg instanceof SumAggregationBuilder;
    }
}
