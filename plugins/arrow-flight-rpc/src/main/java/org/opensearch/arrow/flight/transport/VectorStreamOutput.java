/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * Writes an OpenSearch {@link org.opensearch.core.common.io.stream.Writeable} into a single
 * Arrow {@link VarBinaryVector} row.
 *
 * <p>Prior implementation created a new vector row on every {@code writeBytes} call. For an
 * InternalAggregations with thousands of buckets, each bucket's term/docCount/sub-agg triggered
 * a new row — thousands of {@code VarBinaryVector.setSafe} invocations per batch, each one
 * growing the underlying Arrow buffer and paying Arrow's row-offset bookkeeping. Measured cost
 * was ~40 MB/s serialize throughput (350-500ms for 18MB batches), making serde the dominant
 * cost on streaming queries.
 *
 * <p>This implementation accumulates all bytes into a single growable {@code byte[]} and writes
 * it as ONE vector row at close/getRoot time. Matches the pattern that classic transport uses
 * via {@code BytesStreamOutput}. {@link VectorStreamInput} already reads multiple rows correctly,
 * so the single-row write is wire-compatible with older writers.
 */
class VectorStreamOutput extends StreamOutput {

    private final VarBinaryVector vector;
    private VectorSchemaRoot root;
    // Single growable buffer. Sized to start at 64KB, doubles on demand.
    private byte[] buf;
    private int pos;

    public VectorStreamOutput(BufferAllocator allocator, VectorSchemaRoot root) {
        if (root != null) {
            vector = (VarBinaryVector) root.getVector(0);
            this.root = root;
        } else {
            Field field = new Field("0", new FieldType(true, new ArrowType.Binary(), null, null), null);
            vector = (VarBinaryVector) field.createVector(allocator);
            vector.setInitialCapacity(1);
            vector.allocateNew();
        }
        this.buf = new byte[65536];
        this.pos = 0;
    }

    @Override
    public void writeByte(byte b) {
        ensureCapacity(1);
        buf[pos++] = b;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) {
        if (length == 0) {
            return;
        }
        if (b.length < (offset + length)) {
            throw new IllegalArgumentException("Illegal offset " + offset + "/length " + length + " for byte[] of length " + b.length);
        }
        ensureCapacity(length);
        System.arraycopy(b, offset, buf, pos, length);
        pos += length;
    }

    private void ensureCapacity(int more) {
        int required = pos + more;
        if (required <= buf.length) {
            return;
        }
        int newCap = buf.length;
        while (newCap < required) {
            newCap = (newCap == 0) ? 64 : newCap * 2;
        }
        byte[] n = new byte[newCap];
        System.arraycopy(buf, 0, n, 0, pos);
        buf = n;
    }

    @Override
    public void flush() {
        // no-op — all bytes held in memory until getRoot()
    }

    @Override
    public void close() throws IOException {
        // Reset so the object can be reused (callers do try-with-resources over a single serialize).
        pos = 0;
        vector.close();
    }

    @Override
    public void reset() {
        pos = 0;
        vector.clear();
    }

    public VectorSchemaRoot getRoot() {
        if (pos > 0) {
            // Single-row write of the whole payload. Downstream reader (VectorStreamInput)
            // supports arbitrary row counts.
            vector.setSafe(0, buf, 0, pos);
            vector.setValueCount(1);
        } else {
            vector.setValueCount(0);
        }
        if (root == null) {
            root = new VectorSchemaRoot(List.of(vector));
        }
        root.setRowCount(pos > 0 ? 1 : 0);
        return root;
    }
}
