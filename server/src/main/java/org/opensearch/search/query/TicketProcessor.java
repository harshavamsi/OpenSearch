/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.arrow.spi.StreamReader;
import org.opensearch.arrow.spi.StreamTicket;
import org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import static org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus.DEFAULT_PRECISION;

public class TicketProcessor implements Callable<TicketProcessor.TicketProcessorResult> {
    private final byte[] ticket;
    private final StreamManager streamManager;
    private final HyperLogLogPlusPlus.HyperLogLog counts;

    public TicketProcessor(byte[] ticket, StreamManager streamManager, HyperLogLogPlusPlus.HyperLogLog counts) {
        this.ticket = ticket;
        this.streamManager = streamManager;
        this.counts = counts;
    }

    @Override
    public TicketProcessorResult call() throws Exception {
        int localRowCount = 0;
        long localValueCount = 0;
        StreamTicket streamTicket = streamManager.getStreamTicketFactory().fromBytes(ticket);
        try (StreamReader streamReader = streamManager.getStreamReader(streamTicket)) {
            while (streamReader.next()) {
                VectorSchemaRoot root = streamReader.getRoot();
                int rowCount = root.getRowCount();
                localRowCount += rowCount;
                int precision = DEFAULT_PRECISION;
                final int registers = 1 << precision;
                for (int i = 0; i < registers; ++i) {
                    VarBinaryVector countVector = (VarBinaryVector) root.getVector("count");
                    counts.addRunLen(0, i, ByteBuffer.wrap(countVector.get(i)).get());
                }

                // for (int row = 0; row < rowCount; row++) {
                //// VarCharVector termVector = (VarCharVector) root.getVector("ord");
                // VarBinaryVector countVector = (VarBinaryVector) root.getVector("count");
                // counts.addRunLen();
                //
                //// StringTerms.Bucket bucket = new StringTerms.Bucket(
                //// new BytesRef(termVector.get(row)),
                //// countVector.get(row),
                //// new InternalAggregations(List.of()),
                //// false,
                //// 0,
                //// DocValueFormat.RAW
                //// );
                //// currentBatch.add(bucket);
                // }
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
