/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Registry seam for {@link ShardAggregationEngine}: the engine-side plugin (DataFusion)
 * installs its implementation at init; storage-side backends look it up lazily at fragment
 * execution. Static holder because the two sides are sibling plugins wired through
 * {@code ExtensiblePlugin} discovery with no shared injection context (same pattern as
 * {@code org.opensearch.search.streaming.collection.ColumnSinkFactory} in server).
 *
 * @opensearch.internal
 */
public final class ShardAggregationEngineHolder {

    private static final AtomicReference<ShardAggregationEngine> ENGINE = new AtomicReference<>();

    private ShardAggregationEngineHolder() {}

    public static void install(ShardAggregationEngine engine) {
        ENGINE.set(engine);
    }

    public static boolean isAvailable() {
        return ENGINE.get() != null;
    }

    /** @throws IllegalStateException when no engine plugin installed one */
    public static ShardAggregationEngine get() {
        ShardAggregationEngine engine = ENGINE.get();
        if (engine == null) {
            throw new IllegalStateException(
                "No ShardAggregationEngine installed — the analytics-backend-datafusion plugin must be "
                    + "installed for doc_values-backed aggregation fragments"
            );
        }
        return engine;
    }
}
