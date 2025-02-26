/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.lucene.util.BytesRef;
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.arrow.spi.StreamReader;
import org.opensearch.arrow.spi.StreamTicket;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.opensearch.search.aggregations.metrics.InternalHyperLogLogCardinality;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import static org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus.DEFAULT_PRECISION;

public class TicketSubAggProcessor implements Callable<TicketSubAggProcessor.TicketProcessorResult> {
    private final byte[] ticket;
    private final StreamManager streamManager;
    private final HyperLogLogPlusPlus.HyperLogLog counts;
    private final BlockingQueue<List<StringTerms.Bucket>> batchQueue;

    public TicketSubAggProcessor(
        byte[] ticket,
        StreamManager streamManager,
        HyperLogLogPlusPlus.HyperLogLog counts,
        BlockingQueue<List<StringTerms.Bucket>> batchQueue
    ) {
        this.ticket = ticket;
        this.streamManager = streamManager;
        this.batchQueue = batchQueue;
        this.counts = counts;
    }

    @Override
    public TicketProcessorResult call() throws Exception {
        int localRowCount = 0;
        long localValueCount = 0;
        StreamTicket streamTicket = streamManager.getStreamTicketFactory().fromBytes(ticket);
        try (StreamReader streamReader = streamManager.getStreamReader(streamTicket)) {
            while (streamReader.next()) {
                List<StringTerms.Bucket> currentBatch = new ArrayList<>();
                VectorSchemaRoot root = streamReader.getRoot();
                int rowCount = root.getRowCount();
                localRowCount += rowCount;
                int precision = DEFAULT_PRECISION;
                for (int row = 0; row < rowCount; row++) {
                    // Get the struct vector and its children
                    StructVector bucketOrdStruct = (StructVector) root.getVector("bucketOrd");
                    VarCharVector termVector = (VarCharVector) bucketOrdStruct.getChild("bucketOrd");
                    UInt8Vector countVector = (UInt8Vector) root.getVector("count");

                    // Get sub-aggregation vectors from the struct
                    VarBinaryVector subCountVector = (VarBinaryVector) bucketOrdStruct.getChild("subCount");

                    final int registers = 1 << precision;
                    for (int i = 0; i < registers; ++i) {
                        counts.addRunLen(0, i, ByteBuffer.wrap(subCountVector.get(i)).get());
                    }

                    StringTerms.Bucket bucket = new StringTerms.Bucket(
                        new BytesRef(termVector.get(row)),
                        countVector.get(row),  // Note: This might need type conversion if count is expected as int
                        new InternalAggregations(
                            List.of(new InternalHyperLogLogCardinality(Aggregation.CommonFields.VALUE.getPreferredName(), counts, null))
                        ),
                        false,
                        0,
                        DocValueFormat.RAW
                    );
                    currentBatch.add(bucket);
                }
                batchQueue.put(currentBatch);
            }
            return new TicketProcessorResult(localRowCount, localValueCount);
        }
    }

    public static class TicketProcessorResult {
        private final int rowCount;
        private final long valueCount;

        TicketProcessorResult(int rowCount, long valueCount) {
            this.rowCount = rowCount;
            this.valueCount = valueCount;
        }

        public int getRowCount() {
            return rowCount;
        }

        public long getValueCount() {
            return valueCount;
        }

    }
}
