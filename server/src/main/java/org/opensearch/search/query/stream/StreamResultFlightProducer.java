/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.stream;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.search.Collector;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.query.StreamCollector;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@ExperimentalApi
public class StreamResultFlightProducer extends NoOpFlightProducer {

    final ConcurrentHashMap<Ticket, StreamState> lookup;
    final BufferAllocator allocator;

    public StreamResultFlightProducer(BufferAllocator allocator) {
        this.lookup = new ConcurrentHashMap<>();
        this.allocator = allocator;
    }

    public Ticket createStream(StreamCollector streamCollector, CollectorCallback callback) {
        String id = UUID.randomUUID().toString();
        Ticket ticket = new Ticket(id.getBytes(StandardCharsets.UTF_8));
        lookup.put(ticket, new StreamState(streamCollector, callback));
        return ticket;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        if (lookup.get(ticket) == null) {
            listener.error(new IllegalStateException("Data not ready"));
            return;
        }
        StreamState streamState = lookup.get(ticket);
        VectorSchemaRoot root = streamState.streamCollector.getVectorSchemaRoot(allocator);
        StreamWriter streamWriter = new StreamWriter(root, new BackpressureStrategy.CallbackBackpressureStrategy(), listener);
        streamState.streamCollector.registerStreamWriter(streamWriter);

        listener.setOnCancelHandler(() -> {
            root.close();
            lookup.remove(ticket);
        });
        try {
            streamState.collectorCallback.collect(streamState.streamCollector);
            streamState.streamCollector.finish();
            root.close();
            listener.completed();
        } catch (Exception e) {
            listener.error(e);
            throw new RuntimeException(e);
        } finally {

            lookup.remove(ticket);
        }
    }

    static class StreamState {
        StreamCollector streamCollector;
        CollectorCallback collectorCallback;
        StreamState(StreamCollector streamCollector, CollectorCallback collectorCallback) {
            this.streamCollector = streamCollector;
            this.collectorCallback = collectorCallback;
        }
    }

    @ExperimentalApi
    public static abstract class CollectorCallback {
        public abstract void collect(Collector collector) throws IOException;
    }
}
