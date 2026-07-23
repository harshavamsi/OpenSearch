/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming.collection;

import org.apache.lucene.index.NumericDocValues;
import org.opensearch.common.lease.Releasable;

import java.io.IOException;

/**
 * Receives batches of long values read from doc_values during columnar collection.
 *
 * <p>Implementations live behind the plugin classloader (Arrow-backed); this interface keeps
 * {@code server} free of Arrow types. Single-threaded, same ownership model as the leaf
 * collector that feeds it: whoever creates the sink must {@link #close()} it exactly once.
 *
 * @opensearch.internal
 */
public interface LongColumnSink extends Releasable {

    /** Bulk-append {@code length} values from {@code values[offset]}. */
    void appendLongs(long[] values, int offset, int length);

    /**
     * Decode {@code size} values for the ascending {@code docs} straight from doc_values into the
     * sink's backing storage, bypassing the heap {@code long[]} copy. Implementations backed by
     * off-heap memory use {@link NumericDocValues#longValuesInto}; the default returns false so
     * callers fall back to a heap decode + {@link #appendLongs}. No FFM types appear here because
     * server compiles at --release 21 where {@code java.lang.foreign} is preview.
     *
     * @return true when the values were appended directly; false when the caller must fall back
     */
    default boolean appendFromDocValues(NumericDocValues dv, int size, int[] docs) throws IOException {
        return false;
    }

    /** Bytes currently allocated by the underlying column, for stats/debug output. */
    long bytesAllocated();

    /** Number of values appended so far. */
    int valueCount();
}
