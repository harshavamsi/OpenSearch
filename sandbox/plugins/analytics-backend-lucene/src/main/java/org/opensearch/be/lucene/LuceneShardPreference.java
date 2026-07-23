/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.spi.BackendShardPreference;
import org.opensearch.analytics.spi.ShardPreferenceContext;

import java.util.OptionalInt;

/**
 * Lucene's per-shard preference: opt in to drive count-fast-path fragments when the user has
 * enabled {@code analytics.planner.prefer_metadata_driver}.
 *
 * <p>Today the only signal is the cluster setting + fragment shape. Future shard-local
 * inputs (deletes, segment count, query-cache warmth) plug into the same scoring function as
 * {@link ShardPreferenceContext} grows.
 *
 * @opensearch.internal
 */
final class LuceneShardPreference implements BackendShardPreference {

    /** Wants-to-drive score — beats generic alternatives (score 0). */
    private static final int COUNT_FAST_PATH_SCORE = 100;

    /** Veto score — actively don't pick this plan. Lucene returns this when the fragment
     *  isn't a count-fast-path so the selector doesn't accidentally collapse to a non-drivable
     *  Lucene alternative just because it appeared first in PlanForker order. */
    private static final int NOT_DRIVABLE_SCORE = -1;

    /**
     * Doc_values group-by score — lower than the count fast path (metadata-only beats
     * decode-and-aggregate) but above generic alternatives, so lucene-format shards route
     * grouped aggregates through the doc_values → engine path.
     */
    private static final int DOC_VALUES_AGG_SCORE = 50;

    @Override
    public OptionalInt scoreFor(RelNode fragment, ShardPreferenceContext ctx) {
        if (ctx.preferMetadataDriver() == false) return OptionalInt.empty();
        if (LuceneFragmentConvertor.isCountFastPath(fragment)) {
            return OptionalInt.of(COUNT_FAST_PATH_SCORE);
        }
        if (LuceneFragmentConvertor.isDocValuesGroupByPath(fragment)) {
            return OptionalInt.of(DOC_VALUES_AGG_SCORE);
        }
        // Engine-plan (wire v3) shapes the v2 spec can't express — keyword group keys,
        // expressions, DISTINCT, row-returning Sort/Project fragments. Without this arm
        // they'd score NOT_DRIVABLE and the selector would veto the Lucene alternative on
        // dual-format shards even though convertFragment compiles them fine.
        if (org.opensearch.analytics.spi.ShardAggregationEngineHolder.isAvailable()
            && LuceneFragmentConvertor.extractGeneralDvShape(fragment) != null) {
            return OptionalInt.of(DOC_VALUES_AGG_SCORE);
        }
        return OptionalInt.of(NOT_DRIVABLE_SCORE);
    }
}
