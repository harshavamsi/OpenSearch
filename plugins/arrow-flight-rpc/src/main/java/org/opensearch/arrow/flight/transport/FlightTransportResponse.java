/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.Header;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static org.opensearch.arrow.flight.transport.ClientHeaderMiddleware.CORRELATION_ID_KEY;

/**
 * Streaming transport response implementation using Arrow Flight.
 * Manages Flight stream lifecycle with lazy initialization and prefetching support.
 */
class FlightTransportResponse<T extends TransportResponse> implements StreamTransportResponse<T> {
    private static final Logger logger = LogManager.getLogger(FlightTransportResponse.class);

    private final FlightClient flightClient;
    private final Ticket ticket;
    private final FlightCallHeaders callHeaders;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final HeaderContext headerContext;
    private final TransportResponseHandler<T> handler;
    private final FlightTransportConfig config;
    private final long correlationId;

    private volatile FlightStream flightStream;
    private volatile long currentBatchSize;
    private volatile boolean firstBatchConsumed;
    private volatile boolean closed;
    private volatile boolean prefetchStarted;
    private volatile Header initialHeader;

    // Bound to the columnar reader if the server emits a multi-column schema. Computed on
    // first batch and reused for the rest of the stream (Flight locks the schema).
    private volatile ColumnarAggReader columnarReader = null;
    private volatile boolean columnarReaderResolved = false;

    // Serde telemetry — cumulative counters for this response channel. Exposed so the
    // coordinator-side consumer can log per-query totals alongside latency numbers.
    private final java.util.concurrent.atomic.AtomicLong deserializeNanos = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong wireBytesReceived = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicInteger batchesReceived = new java.util.concurrent.atomic.AtomicInteger(0);

    FlightTransportResponse(
        TransportResponseHandler<T> handler,
        long correlationId,
        FlightClient flightClient,
        HeaderContext headerContext,
        Ticket ticket,
        NamedWriteableRegistry namedWriteableRegistry,
        FlightTransportConfig config
    ) {
        this.handler = Objects.requireNonNull(handler);
        this.correlationId = correlationId;
        this.flightClient = Objects.requireNonNull(flightClient);
        this.headerContext = Objects.requireNonNull(headerContext);
        this.ticket = Objects.requireNonNull(ticket);
        this.namedWriteableRegistry = Objects.requireNonNull(namedWriteableRegistry);
        this.config = Objects.requireNonNull(config);
        this.callHeaders = new FlightCallHeaders();
        this.callHeaders.insert(CORRELATION_ID_KEY, String.valueOf(correlationId));
    }

    void openAndPrefetchAsync(CompletableFuture<Header> future) {
        if (prefetchStarted) return;

        synchronized (this) {
            if (prefetchStarted) return;
            if (closed) {
                future.completeExceptionally(new StreamException(StreamErrorCode.UNAVAILABLE, "Stream is closed"));
                return;
            }

            prefetchStarted = true;

            Thread.ofVirtual().start(() -> {
                try {
                    long start = System.nanoTime();
                    flightStream = flightClient.getStream(ticket, new HeaderCallOption(callHeaders));
                    long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                    logger.debug("FlightClient.getStream() for correlationId: {} took {}ms", correlationId, elapsedMs);
                    start = System.nanoTime();
                    flightStream.next();
                    elapsedMs = (System.nanoTime() - start) / 1_000_000;
                    logger.debug("First FlightClient.next() for correlationId: {} took {}ms", correlationId, elapsedMs);
                    initialHeader = headerContext.getHeader(correlationId);
                    future.complete(initialHeader);
                } catch (FlightRuntimeException e) {
                    future.completeExceptionally(FlightErrorMapper.fromFlightException(e));
                } catch (Exception e) {
                    future.completeExceptionally(new StreamException(StreamErrorCode.INTERNAL, "Stream open/prefetch failed", e));
                }
            });
        }
    }

    TransportResponseHandler<T> getHandler() {
        return handler;
    }

    @Override
    public T nextResponse() {
        if (closed) throw new StreamException(StreamErrorCode.UNAVAILABLE, "Stream is closed");
        if (flightStream == null) throw new IllegalStateException("openAndPrefetch() must be called first");

        long startTime = System.currentTimeMillis();
        try {
            boolean hasNext = firstBatchConsumed ? flightStream.next() : (firstBatchConsumed = true);
            if (!hasNext) return null;

            VectorSchemaRoot root = flightStream.getRoot();
            currentBatchSize = FlightUtils.calculateVectorSchemaRootSize(root);
            wireBytesReceived.addAndGet(currentBatchSize);
            long dsStart = System.nanoTime();

            // On the first batch, inspect the schema to decide between the columnar reader
            // (multi-column schema with a "header" field) and the legacy single-VarBinary reader.
            // We use a strict name walk to avoid Arrow's findField quirks.
            if (!columnarReaderResolved) {
                columnarReaderResolved = true;
                boolean hasHeader = false;
                for (org.apache.arrow.vector.types.pojo.Field f : root.getSchema().getFields()) {
                    if (AggColumnarSchema.HEADER.equals(f.getName())) {
                        hasHeader = true;
                        break;
                    }
                }
                if (hasHeader) {
                    AggColumnarPlan plan = ColumnarPlanFromSchema.build(root.getSchema());
                    columnarReader = new ColumnarAggReader(plan, namedWriteableRegistry);
                }
            }

            if (columnarReader != null) {
                @SuppressWarnings("unchecked")
                T result = (T) columnarReader.read(root);
                deserializeNanos.addAndGet(System.nanoTime() - dsStart);
                batchesReceived.incrementAndGet();
                return result;
            }

            try (VectorStreamInput input = new VectorStreamInput(root, namedWriteableRegistry)) {
                input.setVersion(initialHeader.getVersion());
                T result = handler.read(input);
                deserializeNanos.addAndGet(System.nanoTime() - dsStart);
                batchesReceived.incrementAndGet();
                return result;
            }
        } catch (FlightRuntimeException e) {
            throw FlightErrorMapper.fromFlightException(e);
        } catch (IOException e) {
            throw new StreamException(StreamErrorCode.INTERNAL, "Failed to deserialize batch", e);
        } finally {
            long took = System.currentTimeMillis() - startTime;
            if (took > config.getSlowLogThreshold().millis()) {
                logger.warn("Flight stream next() took [{}ms], exceeding threshold [{}ms]", took, config.getSlowLogThreshold().millis());
            }
            logger.debug("FlightClient.next() for correlationId: {} took {}ms", correlationId, took);
        }
    }

    /** Cumulative wire bytes received. For telemetry. */
    public long getWireBytesReceived() {
        return wireBytesReceived.get();
    }

    /** Cumulative deserialize nanos. For telemetry. */
    public long getDeserializeNanos() {
        return deserializeNanos.get();
    }

    /** Batches deserialized on this channel. */
    public int getBatchesReceived() {
        return batchesReceived.get();
    }

    long getCurrentBatchSize() {
        return currentBatchSize;
    }

    @Override
    public void cancel(String reason, Throwable cause) {
        if (closed) return;
        try {
            if (flightStream != null) flightStream.cancel(reason, cause);
        } catch (Exception e) {
            logger.warn("Error cancelling flight stream", e);
        } finally {
            close();
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;

        int batches = batchesReceived.get();
        if (batches > 0 && logger.isDebugEnabled()) {
            long bytes = wireBytesReceived.get();
            long dsNs = deserializeNanos.get();
            logger.debug(
                "flight_client_stream_close correlationId={} batches={} bytes={} deserialize_ms={} "
                    + "bytes_per_batch={} deserialize_us_per_batch={}",
                correlationId,
                batches,
                bytes,
                dsNs / 1_000_000,
                bytes / batches,
                (dsNs / batches) / 1000
            );
        }

        if (flightStream != null) {
            try {
                flightStream.close();
            } catch (IllegalStateException ignore) {} catch (Exception e) {
                throw new StreamException(StreamErrorCode.INTERNAL, "Error closing flight stream", e);
            }
        }
    }
}
