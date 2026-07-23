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
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.terms.ColumnarTermsCodec;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.streaming.collection.ColumnarTermsFolderFactory;
import org.opensearch.transport.Header;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final boolean isNativeHandler;
    private final FlightTransportConfig config;
    private final long correlationId;

    private volatile FlightStream flightStream;
    private volatile long currentBatchSize;
    private volatile boolean firstBatchConsumed;
    // True iff the prefetch's flightStream.next() observed a batch. If false, the stream
    // was already drained/empty at prefetch time — nextResponse() must return null without
    // calling getRoot(), which would otherwise block on an AbstractFuture that never completes.
    private volatile boolean firstBatchAvailable;
    private volatile boolean closed;
    private final AtomicBoolean streamClosed = new AtomicBoolean(false);
    private volatile boolean prefetchStarted;
    private volatile Header initialHeader;

    // Bound to the columnar reader if the server emits a multi-column schema. Computed on
    // first batch and reused for the rest of the stream (Flight locks the schema).
    private volatile ColumnarAggReader columnarReader = null;
    private volatile boolean columnarReaderResolved = false;

    // Coordinator-side vector fold. When the schema is columnar AND the shape is fold-eligible
    // (non-MULTI, count/key order, topN in range) AND a per-query folder is reachable for the
    // bound task, batches are folded directly into per-query survivor state and nextResponse()
    // returns a placeholder QSR (header shell, empty aggs). The merged terms are injected once at
    // the consumer's final reduce. Resolved on the first columnar batch alongside columnarReader.
    private volatile ColumnarTermsFolder foldTarget = null;

    // Cap mirrors StreamQueryPhaseResultConsumer.maxStreamingTopN — above it we fall back to the
    // reader + StreamingTermsReducer path rather than folding.
    private static final int MAX_FOLD_TOPN = 100_000;

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
        this.isNativeHandler = handler.skipsDeserialization();
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
                    // close() may have run while we were inside getStream() and missed the stream because
                    // flightStream was still null. Now that it is published, re-check the flag: if a close()
                    // already happened, self-close the stream we just opened so the prefetched first-batch
                    // root is not stranded, then abort. This check is performed *before* future.complete(),
                    // so once the future completes, any subsequent close() always observes flightStream != null
                    // and owns the close itself. There is no post-completion window in which both this thread
                    // and a racing close() could close the same stream.
                    if (closed) {
                        try {
                            closeStreamQuietly();
                        } catch (StreamException e) {
                            logger.warn("Error closing flight stream after close() raced the prefetch", e);
                        }
                        future.completeExceptionally(new StreamException(StreamErrorCode.UNAVAILABLE, "Stream is closed"));
                        return;
                    }
                    long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                    logger.debug("FlightClient.getStream() for correlationId: {} took {}ms", correlationId, elapsedMs);
                    start = System.nanoTime();
                    firstBatchAvailable = flightStream.next();
                    elapsedMs = (System.nanoTime() - start) / 1_000_000;
                    logger.debug(
                        "First FlightClient.next() for correlationId: {} took {}ms, hasBatch={}",
                        correlationId,
                        elapsedMs,
                        firstBatchAvailable
                    );
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
            boolean hasNext;
            if (firstBatchConsumed) {
                hasNext = flightStream.next();
            } else {
                // The prefetch call at openAndPrefetchAsync() already advanced the stream; its
                // return value is captured in firstBatchAvailable. If it was false (empty stream,
                // e.g. producer called completeStream without any sendBatch), return null now —
                // do NOT call getRoot(), which would block on a future that never completes.
                firstBatchConsumed = true;
                hasNext = firstBatchAvailable;
            }
            if (!hasNext) return null;

            VectorSchemaRoot root = flightStream.getRoot();
            currentBatchSize = FlightUtils.calculateVectorSchemaRootSize(root);
            wireBytesReceived.addAndGet(currentBatchSize);
            long dsStart = System.nanoTime();
            // Flight owns getLatestMetadata()'s buffer until the next next() call;
            // we copy off so the response can outlive the stream cursor.
            byte[] metadata = readMetadata();

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
                    resolveFoldTarget(plan, root);
                }
            }

            if (foldTarget != null) {
                @SuppressWarnings("unchecked")
                T result = (T) foldBatch(root);
                deserializeNanos.addAndGet(System.nanoTime() - dsStart);
                batchesReceived.incrementAndGet();
                return result;
            }

            if (columnarReader != null) {
                @SuppressWarnings("unchecked")
                T result = (T) columnarReader.read(root);
                deserializeNanos.addAndGet(System.nanoTime() - dsStart);
                batchesReceived.incrementAndGet();
                return result;
            }

            try (VectorStreamInput input = newStreamInput(root, metadata)) {
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

    /**
     * Decide whether this columnar stream is fold-eligible and, if so, bind {@link #foldTarget} to
     * the per-query folder for the bound task. Called once on the first columnar batch. Eligibility:
     * a task is bound on this thread, the key kind is LONG/STRING (not MULTI), the terms order is
     * count- or key-based (not sub-agg ordering), and topN is within {@link #MAX_FOLD_TOPN}. Any
     * miss leaves {@code foldTarget == null} and the stream uses the {@link ColumnarAggReader} path.
     */
    private void resolveFoldTarget(AggColumnarPlan plan, VectorSchemaRoot root) {
        Long taskId = org.opensearch.search.streaming.collection.ColumnarTermsFolderFactory.currentTask();
        if (taskId == null) {
            return;
        }
        if (plan.getTermKeyKind() == AggColumnarPlan.TermKeyKind.MULTI) {
            return;
        }
        ColumnarTermsCodec.TermsHeader header;
        try {
            header = decodeTermsHeader(root);
        } catch (IOException e) {
            // Can't read the header — leave folding off and let the reader path surface the error.
            return;
        }
        if (header == null) {
            return;
        }
        if (header.requiredSize <= 0 || header.requiredSize > MAX_FOLD_TOPN) {
            return;
        }
        BucketOrder order = header.order;
        boolean orderEligible = InternalOrder.isCountDesc(order) || InternalOrder.isKeyOrder(order);
        if (orderEligible == false) {
            return;
        }
        final int topN = header.requiredSize;
        final AggColumnarPlan capturedPlan = plan;
        ColumnarTermsFolderFactory.Folder folder = ColumnarTermsFolderFactory.computeIfAbsent(
            taskId,
            () -> new ColumnarTermsFolder(capturedPlan, topN)
        );
        // The folder is created by the first shard stream to arrive; every stream shares it. Guard
        // against a heterogeneous concurrent stream having installed an incompatible folder type
        // (shouldn't happen for one query, but keeps the cast total).
        if (folder instanceof ColumnarTermsFolder ctf) {
            this.foldTarget = ctf;
        }
    }

    /**
     * Fold one batch into {@link #foldTarget} and return a placeholder {@link QuerySearchResult}
     * carrying the header shell with empty aggregations. The QSR keeps downstream accounting
     * (topDocsStats, processedShards, breaker bookkeeping, stream counters) intact; the merged
     * terms are injected once at the consumer's final reduce.
     */
    private QuerySearchResult foldBatch(VectorSchemaRoot root) throws IOException {
        BigIntVector docCountVec = AggColumnarSchema.bigInt(root, AggColumnarSchema.DOC_COUNT);
        int bucketCount = docCountVec.getValueCount();
        QuerySearchResult placeholder;
        ColumnarTermsCodec.TermsHeader header;
        VarBinaryVector headerVec = AggColumnarSchema.varBinary(root, AggColumnarSchema.HEADER);
        byte[] headerBytes = headerVec.get(0);
        if (headerBytes == null || headerBytes.length == 0) {
            throw new IOException("Columnar batch missing header payload at row 0");
        }
        try (
            org.opensearch.core.common.io.stream.StreamInput raw = new org.opensearch.core.common.io.stream.BytesStreamInput(headerBytes);
            org.opensearch.core.common.io.stream.StreamInput in = new org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput(
                raw,
                namedWriteableRegistry
            )
        ) {
            placeholder = new QuerySearchResult(in);
            header = ColumnarTermsCodec.readTermsHeader(in);
        }
        // The writer serialized the QSR with aggs swapped to EMPTY, so the placeholder already
        // carries no aggregations. Fold the bucket columns into the shared survivor state.
        foldTarget.fold(root, header, bucketCount);
        return placeholder;
    }

    /** Decode just the {@link ColumnarTermsCodec.TermsHeader} from row 0 for eligibility checks. */
    private ColumnarTermsCodec.TermsHeader decodeTermsHeader(VectorSchemaRoot root) throws IOException {
        VarBinaryVector headerVec = AggColumnarSchema.varBinary(root, AggColumnarSchema.HEADER);
        byte[] headerBytes = headerVec.get(0);
        if (headerBytes == null || headerBytes.length == 0) {
            return null;
        }
        try (
            org.opensearch.core.common.io.stream.StreamInput raw = new org.opensearch.core.common.io.stream.BytesStreamInput(headerBytes);
            org.opensearch.core.common.io.stream.StreamInput in = new org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput(
                raw,
                namedWriteableRegistry
            )
        ) {
            // Skip the QSR shell prefix, then read the terms header (same layout the writer emits).
            new QuerySearchResult(in);
            return ColumnarTermsCodec.readTermsHeader(in);
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

    private VectorStreamInput newStreamInput(VectorSchemaRoot streamRoot, byte[] metadata) {
        return isNativeHandler
            ? VectorStreamInput.forNativeArrow(streamRoot, namedWriteableRegistry, metadata)
            : VectorStreamInput.forByteSerialized(streamRoot, namedWriteableRegistry);
    }

    private byte[] readMetadata() {
        return copyMetadata(flightStream.getLatestMetadata());
    }

    /**
     * Copies an Arrow Flight metadata buffer into a {@code byte[]} the consumer owns, or
     * returns {@code null} if the buffer is absent/empty. Package-private for testing.
     */
    static byte[] copyMetadata(ArrowBuf buf) {
        if (buf == null || buf.readableBytes() == 0) return null;
        int len = (int) buf.readableBytes();
        byte[] copy = new byte[len];
        buf.getBytes(0, copy);
        return copy;
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
        closeStreamQuietly();
    }

    private void closeStreamQuietly() {
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

        FlightStream stream = flightStream;
        if (stream != null && streamClosed.compareAndSet(false, true)) {
            try {
                stream.close();
            } catch (IllegalStateException ignore) {} catch (Exception e) {
                throw new StreamException(StreamErrorCode.INTERNAL, "Error closing flight stream", e);
            }
        }
    }
}
