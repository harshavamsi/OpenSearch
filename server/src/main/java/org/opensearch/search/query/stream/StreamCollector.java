/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.stream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

@ExperimentalApi
public abstract class StreamCollector extends FilterCollector {

    private final int batchSize;
    private int docsInCurrentBatch;
    private StreamWriter streamWriter = null;

    public StreamCollector(Collector collector) {
        this(collector, 1000);
    }

    public StreamCollector(Collector collector, int batchSize) {
        super(collector);
        this.batchSize = batchSize;
        docsInCurrentBatch = 0;
    }

    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        LeafCollector leafCollector = ((this.in != null) ? super.getLeafCollector(context) : null);
        return new LeafCollector() {
            @Override
            public void setScorer(Scorable scorable) throws IOException {
                if (leafCollector != null) {
                    leafCollector.setScorer(scorable);
                }
            }

            @Override
            public void collect(int i) throws IOException {
                if (leafCollector != null) {
                    leafCollector.collect(i);
                }
                docsInCurrentBatch++;
                if (docsInCurrentBatch >= batchSize) {
                    streamWriter.writeBatch(docsInCurrentBatch);
                    docsInCurrentBatch = 0;
                    onNewBatch();
                }
            }
        };
    }

    public abstract void onNewBatch();

    public abstract VectorSchemaRoot getVectorSchemaRoot(BufferAllocator allocator);

    public void registerStreamWriter(StreamWriter streamWriter) {
        this.streamWriter = streamWriter;
    }

    public void finish() {
        if (docsInCurrentBatch > 0) {
            streamWriter.writeBatch(docsInCurrentBatch);
            docsInCurrentBatch = 0;
        }
        streamWriter.finish();
    }
}
