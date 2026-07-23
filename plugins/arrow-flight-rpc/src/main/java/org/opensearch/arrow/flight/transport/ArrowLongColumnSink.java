/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.lucene.index.NumericDocValues;
import org.opensearch.search.streaming.collection.LongColumnSink;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

/**
 * Arrow-backed {@link LongColumnSink}: appends collected doc_values straight into an off-heap
 * {@link BigIntVector}. The vector grows by doubling; {@code setSafe} is avoided in favour of a
 * pre-sized {@code set} loop per bulk append. When the segment supports it,
 * {@link #appendFromDocValues} bulk-decodes doc_values directly into the vector's data buffer
 * via {@link NumericDocValues#longValuesInto} — zero heap copy.
 *
 * <p>POC note: the vector is currently released at segment flush (the collector owns it end to
 * end); handing it to the transport root instead of re-serializing is the follow-up step.
 *
 * @opensearch.internal
 */
final class ArrowLongColumnSink implements LongColumnSink {

    private final BigIntVector vector;
    private int count = 0;

    ArrowLongColumnSink(BufferAllocator allocator, String name, int initialCapacity) {
        this.vector = new BigIntVector(name, allocator);
        this.vector.allocateNew(Math.max(initialCapacity, 1));
    }

    @Override
    public void appendLongs(long[] values, int offset, int length) {
        while (count + length > vector.getValueCapacity()) {
            vector.reAlloc();
        }
        for (int i = 0; i < length; i++) {
            vector.set(count + i, values[offset + i]);
        }
        count += length;
    }

    @Override
    public boolean appendFromDocValues(NumericDocValues dv, int size, int[] docs) throws IOException {
        // Reserve capacity FIRST — reAlloc moves the buffer, invalidating any address taken
        // before it.
        while (count + size > vector.getValueCapacity()) {
            vector.reAlloc();
        }
        MemorySegment dst = MemorySegment.ofAddress(vector.getDataBuffer().memoryAddress())
            .reinterpret(((long) count + size) * Long.BYTES)
            .asSlice((long) count * Long.BYTES);
        boolean ok = dv.longValuesInto(size, docs, 0, dst, 0L, 0L);
        if (ok == false) {
            return false;
        }
        count += size;
        return true;
    }

    @Override
    public long bytesAllocated() {
        return vector.getBufferSize();
    }

    @Override
    public int valueCount() {
        return count;
    }

    BigIntVector vector() {
        // The direct decode path writes the data buffer but not validity bits (appendLongs'
        // set() marks them). The sink is append-dense-from-zero, so bulk-mark [0, count) valid
        // here; trailing bits past count in the last byte are ignored by readers.
        if (count > 0) {
            vector.getValidityBuffer().setOne(0, (count + 7) / 8);
        }
        vector.setValueCount(count);
        return vector;
    }

    @Override
    public void close() {
        vector.close();
    }
}
