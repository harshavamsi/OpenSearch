/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.ExchangeSinkProvider;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility logic that operates on {@link CapabilityRegistry} results.
 *
 * @opensearch.internal
 */
public final class CapabilityResolutionUtils {

    private CapabilityResolutionUtils() {}

    /**
     * Filters viable backends to those that can act as coordinator-side executors,
     * i.e., backends that provide a non-null {@link ExchangeSinkProvider}.
     */
    public static List<String> filterByReduceCapability(CapabilityRegistry registry, List<String> viableBackends) {
        List<String> result = new ArrayList<>();
        for (String name : viableBackends) {
            AnalyticsSearchBackendPlugin backend = registry.getBackend(name);
            if (backend.getExchangeSinkProvider() != null) {
                result.add(name);
            }
        }
        if (result.isEmpty()) {
            // The reduce stage is scan-free: it consumes child-stage exchange streams, so any
            // registered sink-capable backend can run it even when it wasn't scan-viable. This
            // is the doc_values path — Lucene drives the shard scan (no sink provider today)
            // and DataFusion runs the coordinator reduce.
            for (AnalyticsSearchBackendPlugin backend : registry.allBackends()) {
                if (viableBackends.contains(backend.name()) == false && backend.getExchangeSinkProvider() != null) {
                    result.add(backend.name());
                }
            }
        }
        if (result.isEmpty()) {
            throw new IllegalStateException("No viable backend supports coordinator reduce among " + viableBackends);
        }
        return result;
    }
}
