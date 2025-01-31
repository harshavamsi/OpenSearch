/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.util.BytesRef;
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.arrow.spi.StreamReader;
import org.opensearch.arrow.spi.StreamTicket;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

public class TicketProcessor implements Callable<TicketProcessor.TicketProcessorResult> {
    private final byte[] ticket;
    private final StreamManager streamManager;
    private final BlockingQueue<List<StringTerms.Bucket>> batchQueue;

    public TicketProcessor(byte[] ticket, StreamManager streamManager, BlockingQueue<List<StringTerms.Bucket>> batchQueue) {
        this.ticket = ticket;
        this.streamManager = streamManager;
        this.batchQueue = batchQueue;
    }

    @Override
    public TicketProcessorResult call() throws Exception {
        int localRowCount = 0;
        StreamTicket streamTicket = streamManager.getStreamTicketFactory().fromBytes(ticket);
        try (StreamReader streamReader = streamManager.getStreamReader(streamTicket)) {
            while (streamReader.next()) {
                List<StringTerms.Bucket> currentBatch = new ArrayList<>();

                VectorSchemaRoot root = streamReader.getRoot();
                int rowCount = root.getRowCount();
                localRowCount += rowCount;

                for (int row = 0; row < rowCount; row++) {
                    VarCharVector termVector = (VarCharVector) root.getVector("ord");
                    UInt8Vector countVector = (UInt8Vector) root.getVector("count");

                    StringTerms.Bucket bucket = new StringTerms.Bucket(
                        new BytesRef(termVector.get(row)),
                        countVector.get(row),
                        new InternalAggregations(List.of()),
                        false,
                        0,
                        DocValueFormat.RAW
                    );
                    currentBatch.add(bucket);
                }
                batchQueue.put(currentBatch);
            }
            return new TicketProcessorResult(localRowCount);
        }
    }

    public static class TicketProcessorResult {
        private final int rowCount;

        TicketProcessorResult(int rowCount) {
            this.rowCount = rowCount;
        }

        public int getRowCount() {
            return rowCount;
        }
    }
}
