/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.stream;

import io.grpc.internal.ServerStreamListener;
import org.apache.arrow.flight.BackpressureStrategy;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.vector.VectorSchemaRoot;

public class StreamContext {

    private VectorSchemaRoot vectorSchemaRoot;
    private FlightDescriptor flightDescriptor;
    private ServerStreamListener listener;
    private BackpressureStrategy backpressureStrategy;

    public StreamContext(VectorSchemaRoot vectorSchemaRoot, FlightDescriptor flightDescriptor, ServerStreamListener listener,
                         BackpressureStrategy backpressureStrategy) {
        this.vectorSchemaRoot = vectorSchemaRoot;
        this.flightDescriptor = flightDescriptor;
        this.listener = listener;
        this.backpressureStrategy = backpressureStrategy;
    }

    public VectorSchemaRoot getVectorSchemaRoot() {
        return vectorSchemaRoot;
    }
    public FlightDescriptor getFlightDescriptor() {
        return flightDescriptor;
    }
    public ServerStreamListener getListener() {
        return listener;
    }
    public BackpressureStrategy getBackpressureStrategy() {
        return backpressureStrategy;
    }
}
