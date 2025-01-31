/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.support;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.bucket.terms.InternalMappedTerms;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamingAggregator extends FilterCollector {

    private final Aggregator aggregator;
    private final SearchContext searchContext;
    private final VectorSchemaRoot root;
    private final StreamProducer.FlushSignal flushSignal;
    private final int batchSize;
    private final ShardId shardId;

    /**
     * Sole constructor.
     *
     * @param in
     */
    public StreamingAggregator(
        Aggregator in,
        SearchContext searchContext,
        VectorSchemaRoot root,
        int batchSize,
        StreamProducer.FlushSignal flushSignal,
        ShardId shardId
    ) {
        super(in);
        this.aggregator = in;
        this.searchContext = searchContext;
        this.root = root;
        this.batchSize = batchSize;
        this.flushSignal = flushSignal;
        this.shardId = shardId;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {

        Map<String, FieldVector> vectors = new HashMap<>();
        vectors.put("ord", root.getVector("ord"));
        vectors.put("count", root.getVector("count"));
        final int[] currentRow = { 0 };
        return new LeafBucketCollector() {
            final LeafBucketCollector leaf = aggregator.getLeafCollector(context);

            @Override
            public void setScorer(Scorable scorer) throws IOException {

            }

            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                leaf.collect(doc);
                currentRow[0]++;
                if (currentRow[0] >= batchSize) {
                    flushBatch();
                }
            }

            private void flushBatch() throws IOException {
                int bucketCount = 0;
                InternalAggregation agg = aggregator.buildAggregations(new long[] { 0 })[0];
                if (agg instanceof InternalMappedTerms) {
                    InternalMappedTerms<?, ?> terms = (InternalMappedTerms<?, ?>) agg;

                    List<? extends InternalTerms.Bucket> buckets = terms.getBuckets();
                    for (int i = 0; i < buckets.size(); i++) {
                        // Get key/value info
                        String key = buckets.get(i).getKeyAsString();
                        long docCount = buckets.get(i).getDocCount();

                        Aggregations aggregations = buckets.get(i).getAggregations();
                        for (Aggregation aggregation : aggregations) {
                            // TODO: subs
                        }

                        FieldVector termVector = vectors.get("ord");
                        FieldVector countVector = vectors.get("count");
                        ((VarCharVector) termVector).setSafe(i, key.getBytes());
                        ((UInt8Vector) countVector).setSafe(i, docCount);
                        bucketCount++;
                    }

                    aggregator.reset();
                }

                // Reset for next batch
                root.setRowCount(bucketCount);
                flushSignal.awaitConsumption(TimeValue.timeValueMillis(100000));
                currentRow[0] = 0;
            }

            @Override
            public void finish() throws IOException {
                if (currentRow[0] > 0) {
                    flushBatch();
                }
                root.close();
            }
        };
    }
}
