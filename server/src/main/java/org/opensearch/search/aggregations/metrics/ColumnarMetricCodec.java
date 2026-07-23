/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.common.util.BigArrays;

import java.util.Map;

/**
 * Bridge providing public factories for metric aggs whose constructors are package-private.
 * Used by the Arrow-columnar streaming reader in the arrow-flight-rpc plugin so it doesn't
 * need reflective construction.
 *
 * @opensearch.internal
 */
public final class ColumnarMetricCodec {

    private ColumnarMetricCodec() {}

    /** Construct an {@link InternalCardinality} from a pre-built HLL state. */
    public static InternalCardinality buildCardinality(String name, AbstractHyperLogLogPlusPlus counts, Map<String, Object> metadata) {
        return new InternalCardinality(name, counts, metadata);
    }

    /** Expose the non-recycling BigArrays instance so the reader can decode HLL blobs. */
    public static BigArrays nonRecyclingBigArrays() {
        return BigArrays.NON_RECYCLING_INSTANCE;
    }
}
