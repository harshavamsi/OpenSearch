/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming.collection;

import org.opensearch.search.aggregations.InternalAggregation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Registry seam between {@code server} and the Arrow plugin for the coordinator-side
 * columnar terms folder.
 *
 * <p>The Arrow plugin folds each received Arrow batch directly into a per-query survivor
 * state (see {@code ColumnarTermsFolder} in the {@code arrow-flight-rpc} plugin) instead of
 * rebuilding {@code InternalTerms}/{@code InternalMax}/... objects per bucket and re-hashing
 * them through {@code StreamingTermsReducer}. That fold happens synchronously inside the
 * plugin's transport {@code nextResponse()} because the off-heap Arrow root's lifetime is a
 * single {@code next()} call.
 *
 * <p>topN displacement is a cross-shard decision, so the survivor state is <b>per-query</b>,
 * not per-shard-stream. Each Flight stream is one shard; multiple shard streams for the same
 * search share one folder keyed by the coordinator {@code SearchTask} id. Shard streams arrive
 * on multiple transport threads, so the folder carries its own lock (implemented plugin-side).
 *
 * <p>This holder is Arrow-free so {@code server} keeps no compile dependency on the plugin.
 * The folder is exposed only through the vector-agnostic {@link Folder} interface; the actual
 * vector fold lives in the plugin. The {@code StreamQueryPhaseResultConsumer} looks the folder
 * up by task id at final reduce, materializes the merged {@link InternalAggregation} exactly
 * once, then releases it. Mirrors the {@link ColumnSinkFactory} pattern.
 *
 * <p><b>Task-id bridge.</b> The stream response handler and its {@code nextResponse()} calls run
 * on the same transport thread (the handler drives the read loop inline). The handler knows the
 * coordinator {@code SearchTask} id; {@code nextResponse()} (plugin-side) does not. The handler
 * brackets its read loop with {@link #bindCurrentTask}/{@link #unbindCurrentTask} so the fold in
 * {@code nextResponse()} can recover the id via {@link #currentTask()} without threading a new
 * parameter through the generic transport interfaces.
 *
 * @opensearch.internal
 */
public final class ColumnarTermsFolderFactory {

    /**
     * Per-query columnar terms folder. Vector-agnostic surface: the plugin folds Arrow batches
     * into it directly; the consumer only finalizes and releases.
     */
    public interface Folder {
        /**
         * Materialize the merged survivor state as a single top-level {@link InternalAggregation}
         * (an {@code InternalTerms}), delegating final topN selection / min-doc-count / ordering to
         * {@code InternalTerms.reduce} under {@code ctx}. Returns {@code null} if no batch was ever
         * folded.
         */
        InternalAggregation finalizeAggregation(InternalAggregation.ReduceContext ctx);

        /** Release retained survivor state (off-heap sketches, primitive columns). Idempotent. */
        void release();
    }

    private static final ConcurrentHashMap<Long, Folder> FOLDERS = new ConcurrentHashMap<>();

    /**
     * Coordinator {@code SearchTask} id for the stream currently being read on this transport
     * thread. Set by the stream handler around its {@code nextResponse()} loop; read by the
     * plugin's fold. {@code null} when no streaming search read is in progress on this thread.
     */
    private static final ThreadLocal<Long> CURRENT_TASK = new ThreadLocal<>();

    private ColumnarTermsFolderFactory() {}

    /**
     * Returns the folder for {@code taskId}, creating it via {@code creator} on first sighting.
     * Atomic across the concurrent shard-stream transport threads that race the first batch.
     */
    public static Folder computeIfAbsent(long taskId, Supplier<Folder> creator) {
        return FOLDERS.computeIfAbsent(taskId, k -> creator.get());
    }

    /** Removes and returns the folder for {@code taskId}, or {@code null} if none. */
    public static Folder remove(long taskId) {
        return FOLDERS.remove(taskId);
    }

    /** Bind the current thread's streaming-search task id for the duration of a read loop. */
    public static void bindCurrentTask(long taskId) {
        CURRENT_TASK.set(taskId);
    }

    /** Clear the current thread's task binding. */
    public static void unbindCurrentTask() {
        CURRENT_TASK.remove();
    }

    /** The current thread's bound task id, or {@code null} if none is bound. */
    public static Long currentTask() {
        return CURRENT_TASK.get();
    }
}
