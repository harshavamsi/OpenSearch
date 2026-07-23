/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import java.io.IOException;

/**
 * Shard-side SPI that lets a streaming terms aggregator read a metric sub-aggregator's
 * ordinal-indexed state directly into columns, without materializing per-bucket
 * {@link org.opensearch.search.aggregations.InternalAggregation} objects.
 *
 * <p>Every metric aggregator already stores its state as primitive columns keyed by bucket
 * ordinal ({@code DoubleArray maxes}, {@code DoubleArray sums} + {@code compensations},
 * {@code LongArray counts}, {@code HyperLogLogPlusPlus counts}). This interface exposes a
 * per-ordinal read so the columnar emit path ({@code ColumnarTermsShardResult} +
 * {@code ColumnarAggWriter.writeFromColumns}) copies values straight from that state into Arrow
 * vectors. A sub-aggregator that does not implement this interface makes the streaming shape
 * ineligible for columnar emit, and the aggregator falls back to the object path.
 *
 * <p>The aggregator stays authoritative over its own layout: it reports its {@link Kind} and,
 * per selected ordinal, pushes the right typed value(s) into a {@link ValueSink}. This mirrors
 * the metric column set the coordinator reader/folder already understands
 * ({@code AggColumnarPlan.MetricKind}).
 *
 * @opensearch.internal
 */
public interface ColumnarMetricSink {

    /** Metric column shape. Mirrors the wire-side {@code AggColumnarPlan.MetricKind}. */
    enum Kind {
        MAX,
        MIN,
        SUM,
        AVG,
        VALUE_COUNT,
        CARDINALITY
    }

    /** The column shape this metric emits. */
    Kind columnarKind();

    /**
     * Push this metric's value(s) for {@code bucketOrd} into {@code sink}, calling exactly the
     * one {@code put*} method that matches {@link #columnarKind()}.
     */
    void writeColumnarValue(long bucketOrd, ValueSink sink) throws IOException;

    /**
     * Typed value receiver. The columnar emit code supplies an implementation that appends into
     * the carrier's per-metric column at the current row.
     */
    interface ValueSink {
        /** max / min / sum scalar. */
        void putDouble(double value);

        /** avg: running sum + count. */
        void putSumCount(double sum, long count);

        /** value_count. */
        void putLong(long value);

        /**
         * cardinality: an independent (NON_RECYCLING-backed) clone of the survivor's HLL sketch, or
         * {@code null} for an empty bucket. The carrier holds the clone directly — the Flight writer
         * serializes it at most once, and the non-Flight fallback wraps it without any decode. This
         * avoids the eager-serialize-then-decode round-trip that dominated wide cardinality aggs.
         */
        void putHll(AbstractHyperLogLogPlusPlus sketch);
    }
}
