/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.backend.EngineResultStream;

import java.util.List;

/**
 * Shard-local vectorized aggregation over caller-pushed Arrow batches.
 *
 * <p>Seam between storage backends that can materialize columns as Arrow batches (e.g. the
 * Lucene backend decoding doc_values) and an execution engine that can run a grouped
 * aggregation over them (the DataFusion backend). The two backends are sibling
 * {@code ExtensiblePlugin}s with no classpath visibility into each other, so the engine side
 * installs an implementation into {@link ShardAggregationEngineHolder} at plugin init and the
 * storage side consumes it through this interface only.
 *
 * <p>v1 scope: all pushed columns are non-null {@code Int64}; group keys and aggregate inputs
 * are column references into the pushed schema.
 *
 * @opensearch.internal
 */
public interface ShardAggregationEngine {

    /** Kind of a pushed input column: how the storage backend decodes it and the engine types it. */
    enum ColumnKind {
        /** Int64 (numeric doc_values). */
        LONG,
        /** Utf8 (keyword doc_values, ordinals materialized to terms). */
        KEYWORD,
        /** Timestamp(ms) — date doc_values; same Int64 epoch-millis wire layout, typed for the engine. */
        TIMESTAMP
    }

    /** One pushed column: name + decode/type kind. */
    record InputColumn(String name, ColumnKind kind) {
    }

    /** Aggregate functions supported by v1 (long-typed inputs). */
    enum AggFunction {
        COUNT,
        SUM,
        MIN,
        MAX,
        AVG
    }

    /**
     * One aggregate call: {@code function} over {@code inputColumn} (ignored for COUNT),
     * emitting {@code outputName}.
     */
    record AggCall(AggFunction function, String inputColumn, String outputName) {
    }

    /**
     * Grouped-aggregation spec over the pushed batches: {@code inputColumns} is the exact
     * schema (name order) of every batch fed to the session; {@code groupColumns} and each
     * {@link AggCall#inputColumn()} must name columns from it.
     */
    record AggSpec(List<String> inputColumns, List<String> groupColumns, List<AggCall> aggCalls) {
    }

    /**
     * Opens an aggregation session. The caller feeds batches matching {@code spec.inputColumns()}
     * (all Int64, non-null), then calls {@link Session#finish()} exactly once to obtain the
     * result stream (group columns first, then aggregate outputs, all Int64). Closing the
     * session without {@code finish()} aborts and releases native state.
     *
     * @param allocator allocator for the result-side Arrow imports
     * @param spec      the grouped aggregation to run
     * @param taskId    owning task id for native-side cancellation registration (0 = none)
     */
    Session open(BufferAllocator allocator, AggSpec spec, long taskId);

    /**
     * Compiles a fragment plan (whose leaf reads are stage-input scans) to the engine's plan
     * bytes. Runs coordinator-side at fragment-conversion time; the returned bytes ride the
     * fragment wire format and are executed data-node-side via
     * {@link #open(BufferAllocator, byte[], List, long)}. This is what lets a storage backend
     * push arbitrary plan shapes (projections/expressions, any aggregate the engine supports)
     * without a per-function SPI surface.
     *
     * @param rebasedFragment fragment whose leaf is the engine-recognized stage-input scan
     *                        over exactly the columns the storage backend will feed
     */
    byte[] compileFragment(org.apache.calcite.rel.RelNode rebasedFragment);

    /**
     * Plan-bytes variant of {@link #open(BufferAllocator, AggSpec, long)}: runs a pre-compiled
     * fragment plan (from {@link #compileFragment}) over pushed batches whose schema follows
     * {@code inputColumns} (LONG → Int64, KEYWORD → Utf8; all nullable). Output schema is
     * whatever the plan produces.
     */
    Session open(BufferAllocator allocator, byte[] fragmentPlanBytes, List<InputColumn> inputColumns, long taskId);

    /** One shard-fragment aggregation run. Not thread-safe; feed and finish from one thread. */
    interface Session extends AutoCloseable {
        /**
         * Pushes one batch. Ownership of the batch's buffers transfers to the engine —
         * the caller must not touch the root after this call returns; the engine closes it.
         */
        void feed(VectorSchemaRoot batch);

        /**
         * Signals end of input and returns the aggregated result stream. The returned stream
         * owns its batches; the caller must close it. May be called once.
         */
        EngineResultStream finish();

        @Override
        void close();
    }
}
