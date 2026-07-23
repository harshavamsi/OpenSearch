/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming.collection;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Registry seam between {@code server} and the Arrow plugin for columnar collection sinks.
 *
 * <p>The arrow-flight-rpc plugin installs an Arrow-backed provider at startup; when no provider
 * is installed (plugin absent), {@link #newLongSink} returns {@code null} and callers fall back
 * to scratch-array-only collection. POC-scoped plumbing: a static holder mirrors how the
 * {@code arrow_columnar} transport gate is wired through {@code FlightOutboundHandler}.
 *
 * @opensearch.internal
 */
public final class ColumnSinkFactory {

    /** Provider of long column sinks; installed by the Arrow plugin. */
    @FunctionalInterface
    public interface LongSinkProvider {
        LongColumnSink create(String name, int expectedCount);
    }

    private static final AtomicReference<LongSinkProvider> PROVIDER = new AtomicReference<>();

    /**
     * Dynamic gate for batched/columnar leaf collection, bound to the
     * {@code search.aggregations.streaming.columnar_collection.enabled} cluster setting by the
     * Arrow plugin. Independent of the transport-side {@code arrow_columnar} gate so collection
     * and serialization can be benchmarked separately.
     */
    private static final java.util.concurrent.atomic.AtomicBoolean COLLECTION_ENABLED = new java.util.concurrent.atomic.AtomicBoolean(
        false
    );

    /**
     * Server-visible mirror of the {@code search.aggregations.streaming.arrow_columnar.enabled}
     * transport gate (the plugin owns the authoritative value on {@code FlightOutboundHandler}).
     * The shard-side columnar emit path ({@code ColumnarTermsShardResult}) reads this so it only
     * builds the emit-only carrier when the Flight transport will actually write it as Arrow
     * columns — the carrier cannot be serialized any other way. Installed by the plugin from the
     * same cluster setting, next to {@link #setCollectionEnabled}.
     */
    private static final java.util.concurrent.atomic.AtomicBoolean ARROW_COLUMNAR_TRANSPORT_ENABLED =
        new java.util.concurrent.atomic.AtomicBoolean(false);

    private ColumnSinkFactory() {}

    public static void setCollectionEnabled(boolean enabled) {
        COLLECTION_ENABLED.set(enabled);
    }

    public static boolean isCollectionEnabled() {
        return COLLECTION_ENABLED.get();
    }

    public static void setArrowColumnarTransportEnabled(boolean enabled) {
        ARROW_COLUMNAR_TRANSPORT_ENABLED.set(enabled);
    }

    public static boolean isArrowColumnarTransportEnabled() {
        return ARROW_COLUMNAR_TRANSPORT_ENABLED.get();
    }

    public static void installProvider(LongSinkProvider provider) {
        PROVIDER.set(provider);
    }

    /** Returns a sink for {@code expectedCount} longs, or {@code null} if no provider is installed. */
    public static LongColumnSink newLongSink(String name, int expectedCount) {
        LongSinkProvider p = PROVIDER.get();
        return p == null ? null : p.create(name, expectedCount);
    }

    public static boolean isAvailable() {
        return PROVIDER.get() != null;
    }
}
