/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.support;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.BytesRef;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.BitArray;
import org.opensearch.common.util.LongArray;
import org.opensearch.common.util.ObjectArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.metrics.AbstractHyperLogLog;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator;
import org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StreamingCardinalityAggregator extends FilterCollector implements Releasable {

    private final Aggregator aggregator;
    private final SearchContext searchContext;
    private final VectorSchemaRoot root;
    private final StreamProducer.FlushSignal flushSignal;
    private final int batchSize;
    private final ShardId shardId;

    private final BigArrays bigArrays;
    private final HyperLogLogPlusPlus.HyperLogLog counts;
    private ObjectArray<BitArray> visited;

    /**
     * Sole constructor.
     *
     * @param in
     */
    public StreamingCardinalityAggregator(
        Aggregator in,
        SearchContext searchContext,
        VectorSchemaRoot root,
        int batchSize,
        StreamProducer.FlushSignal flushSignal,
        ShardId shardId,
        BigArrays bigArrays,
        HyperLogLogPlusPlus.HyperLogLog counts
    ) {
        super(in);
        this.aggregator = in;
        this.searchContext = searchContext;
        this.root = root;
        this.batchSize = batchSize;
        this.flushSignal = flushSignal;
        this.shardId = shardId;
        this.bigArrays = bigArrays;
        this.counts = counts;
        this.visited = bigArrays.newObjectArray(1);
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {

        Map<String, FieldVector> vectors = new HashMap<>();
        vectors.put("ord", root.getVector("ord"));
        vectors.put("count", root.getVector("count"));
        final int[] currentRow = { 0 };
        String fieldName = ((CardinalityAggregator) aggregator).fieldName;
        SortedSetDocValues dv = context.reader().getSortedSetDocValues(fieldName);
        long maxOrd = dv.getValueCount();
        return new LeafBucketCollector() {
            ;
            @Override
            public void setScorer(Scorable scorer) throws IOException {

            }

            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                visited = bigArrays.grow(visited, owningBucketOrd + 1);
                BitArray bits = visited.get(owningBucketOrd);
                if (bits == null) {
                    bits = new BitArray(maxOrd, bigArrays);
                    visited.set(owningBucketOrd, bits);
                }
                if (dv.advanceExact(doc)) {
                    int count = dv.docValueCount();
                    long ord;
                    while ((count-- > 0) && (ord = dv.nextOrd()) != SortedSetDocValues.NO_MORE_DOCS) {
                        bits.set((int) ord);
                    }
                }
                currentRow[0]++;
                if (currentRow[0] >= batchSize) {
                    flushBatch();
                }
            }

            private void flushBatch() throws IOException {
                try (BitArray allVisitedOrds = new BitArray(maxOrd, bigArrays)) {
                    for (long bucket = visited.size() - 1; bucket >= 0; --bucket) {
                        final BitArray bits = visited.get(bucket);
                        if (bits != null) {
                            allVisitedOrds.or(bits);
                        }
                    }

                    try (LongArray hashes = bigArrays.newLongArray(maxOrd, false)) {
                        final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
                        for (long ord = allVisitedOrds.nextSetBit(0); ord < Long.MAX_VALUE; ord = ord + 1 < maxOrd
                            ? allVisitedOrds.nextSetBit(ord + 1)
                            : Long.MAX_VALUE) {
                            final BytesRef value = dv.lookupOrd(ord);
                            MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                            hashes.set(ord, hash.h1);
                        }

                        for (long bucket = visited.size() - 1; bucket >= 0; --bucket) {
                            final BitArray bits = visited.get(bucket);
                            if (bits != null) {
                                for (long ord = bits.nextSetBit(0); ord < Long.MAX_VALUE; ord = ord + 1 < maxOrd
                                    ? bits.nextSetBit(ord + 1)
                                    : Long.MAX_VALUE) {
                                    counts.collect(bucket, hashes.get(ord));
                                }
                            }
                        }
                    }
                }
                VarBinaryVector countVector = (VarBinaryVector) vectors.get("count");
                AbstractHyperLogLog.RunLenIterator iterator = counts.getRunLens(0);
                int index = 0;
                while (iterator.next()) {
                    countVector.setSafe(index, new byte[] { iterator.value() });
                    index++;
                }
                // aggregator.reset();
                // // Reset for next batch
                root.setRowCount(index);
                // System.out.println("## Flushing batch of size: " + bucketCount);
                flushSignal.awaitConsumption(TimeValue.timeValueMillis(1000));
                currentRow[0] = 0;
            }

            @Override
            public void finish() throws IOException {
                if (currentRow[0] > 0) {
                    flushBatch();
                }
            }
        };
    }

    @Override
    public void close() {
        for (int i = 0; i < visited.size(); i++) {
            Releasables.close(visited.get(i));
        }
        Releasables.close(visited);
    }

}
