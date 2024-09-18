/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.stream;

import org.apache.arrow.flight.BackpressureStrategy;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.common.annotation.ExperimentalApi;

@ExperimentalApi
public class StreamWriter {
    private final BackpressureStrategy backpressureStrategy;
    private final ServerStreamListener listener;
    private final VectorSchemaRoot root;
    private static final int timeout = 1000;
    private int batches = 0;

    public StreamWriter(VectorSchemaRoot root, BackpressureStrategy backpressureStrategy, ServerStreamListener listener) {
        this.backpressureStrategy = backpressureStrategy;
        this.listener = listener;
        this.root = root;
        this.backpressureStrategy.register(listener);
        listener.start(root);
    }

    public void writeBatch(int rowCount) {
        backpressureStrategy.waitForListener(timeout);
        root.setRowCount(rowCount);
        listener.putNext();
        batches++;
    }

    public void finish() {}
}
