/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import java.util.Map;

/**
 * Test-only bridge that exposes {@link InternalCardinality}'s package-private constructor to
 * tests living outside the {@code metrics} package (e.g. streaming reducer tests that need to
 * feed pre-built HLL sketches into buckets).
 */
public final class CardinalityTestHelpers {

    private CardinalityTestHelpers() {}

    public static InternalCardinality newInternalCardinality(
        String name,
        AbstractHyperLogLogPlusPlus counts,
        Map<String, Object> metadata
    ) {
        return new InternalCardinality(name, counts, metadata);
    }
}
