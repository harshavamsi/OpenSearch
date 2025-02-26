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
import org.apache.arrow.vector.complex.StructVector;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.LeafCollector;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.BitArray;
import org.opensearch.common.util.ObjectArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.bucket.terms.GlobalOrdinalsStringTermsAggregator;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator;
import org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StreamingAggregator extends FilterCollector implements Releasable {
    private final Aggregator aggregator;
    private final SearchContext searchContext;
    private final VectorSchemaRoot root;
    private final StreamProducer.FlushSignal flushSignal;
    private final int batchSize;
    private final ShardId shardId;

    private final BigArrays bigArrays;
    private final HyperLogLogPlusPlus.HyperLogLog counts;
    private ObjectArray<BitArray> visited;

    private LeafBucketCollector topLevelLeafCollector;
    private final List<LeafBucketCollector> subLevelLeafCollectors = new ArrayList<>();

    public StreamingAggregator(
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
    public LeafCollector getLeafCollector(LeafReaderContext leafReaderContext) throws IOException {
        final List<FieldVector> fieldVectors = root.getFieldVectors();
        if (aggregator instanceof GlobalOrdinalsStringTermsAggregator) {
            topLevelLeafCollector = new StreamingTermsAggregator(aggregator, searchContext, root, batchSize, flushSignal, shardId)
                .getLeafCollector(leafReaderContext, List.of(fieldVectors.get(0), fieldVectors.get(1)));
            StructVector bucketOrdVector = (StructVector) fieldVectors.get(1);
            VarBinaryVector subCountVector = (VarBinaryVector) bucketOrdVector.getChild("subCount");
            Aggregator[] subAggregators = ((GlobalOrdinalsStringTermsAggregator) aggregator).subAggregators();
            for (Aggregator subAggregator : subAggregators) {
                if (subAggregator instanceof CardinalityAggregator) {
                    StreamingCardinalityAggregator streamingCardinalityAggregator = new StreamingCardinalityAggregator(
                        subAggregator,
                        searchContext,
                        root,
                        batchSize,
                        flushSignal,
                        shardId,
                        bigArrays,
                        counts
                    );

                    subLevelLeafCollectors.add(
                        streamingCardinalityAggregator.getLeafCollector(leafReaderContext, Collections.singletonList(subCountVector))
                    );
                }
            }
        }
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                topLevelLeafCollector.collect(doc, owningBucketOrd);
                for (LeafBucketCollector sublevelLeafCollector : subLevelLeafCollectors) {
                    sublevelLeafCollector.collect(doc, owningBucketOrd);
                }
            }

            @Override
            public void finish() throws IOException {
                topLevelLeafCollector.finish();
                for (LeafBucketCollector subLevelLeafCollector : subLevelLeafCollectors) {
                    subLevelLeafCollector.finish();
                }
                root.syncSchema();
            }
        };

    }

    @Override
    public void close() {

    }
}
