/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.concurrent.CompletableFuture;

public class RecordBatchStream implements AutoCloseable {

    private final SessionContext context;
    private final long ptr;
    private final BufferAllocator allocator;
    private final CDataDictionaryProvider dictionaryProvider;

    public RecordBatchStream(SessionContext ctx, long streamId, BufferAllocator allocator) {
        this.context = ctx;
        this.ptr = streamId;
        this.allocator = allocator;
        this.dictionaryProvider = new CDataDictionaryProvider();
    }

    private static native void destroy(long pointer);


    @Override
    public void close() throws Exception {
        destroy(ptr);
        dictionaryProvider.close();
        if (initialized) {
            vectorSchemaRoot.close();
        }
    }

    private static native void next(long runtime, long pointer, ObjectResultCallback callback);

    public CompletableFuture<Boolean> loadNextBatch() {
        ensureInitialized();
        long runtimePointer =  context.getRuntime();
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        next(
            runtimePointer,
            ptr,
            (String errString, long arrowArrayAddress) -> {
                if (errString != null && errString.isEmpty() == false) {
                    result.completeExceptionally(new RuntimeException(errString));
                } else if (arrowArrayAddress == 0) {
                    // Reached end of stream
                    result.complete(false);
                } else {
                    try {
                        ArrowArray arrowArray = ArrowArray.wrap(arrowArrayAddress);
                        Data.importIntoVectorSchemaRoot(
                            allocator, arrowArray, vectorSchemaRoot, dictionaryProvider);
                        result.complete(true);
                    } catch (Exception e) {
                        result.completeExceptionally(e);
                    }
                }
            });
        return result;
    }

    private boolean initialized = false;
    private VectorSchemaRoot vectorSchemaRoot = null;

    public VectorSchemaRoot getVectorSchemaRoot() {
        ensureInitialized();
        return vectorSchemaRoot;
    }

    private void ensureInitialized() {
        if (!initialized) {
            Schema schema = getSchema();
            this.vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
        }
        initialized = true;
    }

    private Schema getSchema() {
        // Native method is not async, but use a future to store the result for convenience
        CompletableFuture<Schema> result = new CompletableFuture<>();
        getSchema(
            ptr,
            (errString, arrowSchemaAddress) -> {
                if (errString != null && errString.isEmpty() == false) {
                    result.completeExceptionally(new RuntimeException(errString));
                } else {
                    try {
                        ArrowSchema arrowSchema = ArrowSchema.wrap(arrowSchemaAddress);
                        Schema schema = Data.importSchema(allocator, arrowSchema, dictionaryProvider);
                        result.complete(schema);
                        // The FFI schema will be released from rust when it is dropped
                    } catch (Exception e) {
                        result.completeExceptionally(e);
                    }
                }
            });
        return result.join();
    }

    private static native void getSchema(long pointer, ObjectResultCallback callback);
}
