/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Defines when streaming responses should be flushed during search execution.
 * Currently only used in aggregations.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public enum FlushMode {
    /**
     * Flush results after each segment is processed.
     * Provides fastest streaming but may have more overhead.
     */
    PER_SEGMENT,

    /**
     * Flush results after each slice is processed.
     * Intermediate streaming frequency between segment and shard.
     */
    PER_SLICE,

    /**
     * Flush results only after the entire shard is processed.
     * This is a traditional and default approach.
     */
    PER_SHARD,

    /**
     * Classic shard-level aggregation compute (global ordinals, amortized per-segment state),
     * but coordinator-side incremental reduce via the streaming transport. Eliminates the
     * per-segment protocol overhead and cross-segment sketch-merge amplification that
     * {@link #PER_SEGMENT} pays on terms→cardinality shapes, while keeping the bounded-heap
     * reduce path at the coordinator.
     *
     * <p>Target profile: same shard latency as classic ({@link #PER_SHARD}), coord memory
     * bounded by topN via the streaming consumer (see {@code StreamQueryPhaseResultConsumer}).
     */
    PER_SHARD_STREAM
}
