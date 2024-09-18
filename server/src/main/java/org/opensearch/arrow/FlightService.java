/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.search.query.stream.StreamResultFlightProducer;

import java.io.IOException;

@ExperimentalApi
public class FlightService extends AbstractLifecycleComponent {
    protected static final String LOCALHOST = "localhost";
    protected static final int PORT = 9309;
    protected static BufferAllocator serverAllocator;
    protected static BufferAllocator clientAllocator;

    protected static FlightServer server;
    protected static FlightClient flightClient;
    protected static StreamResultFlightProducer flightProducer;

    @Override
    protected void doStart() {
        serverAllocator = new RootAllocator(Integer.MAX_VALUE);
        clientAllocator = new RootAllocator(Integer.MAX_VALUE);

        final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, PORT);
        flightProducer = new StreamResultFlightProducer(serverAllocator);
        server = FlightServer.builder(serverAllocator, serverLocation, flightProducer).build();
        try {
            server.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        flightClient = FlightClient.builder(clientAllocator, serverLocation).build();
    }

    public BufferAllocator getAllocator() {
        return serverAllocator;
    }

    public FlightClient getFlightClient() {
        return flightClient;
    }

    public StreamResultFlightProducer getFlightProducer() {
        return flightProducer;
    }

    @Override
    protected void doStop() {
        server.shutdown();
    }

    @Override
    protected void doClose() throws IOException {
        // allocator.close();
        try {
            if (server != null && flightClient != null) {
                server.close();
                flightClient.close();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
