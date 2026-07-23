/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming.collection;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Scorable;
import org.opensearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;

/**
 * Batched leaf collection for streaming keyword-terms aggregations (POC).
 *
 * <p>Keyword analogue of {@link BatchedLongTermsLeafCollector}: buffers matching docids into a
 * fixed batch and bulk-reads segment ordinals via {@code SortedDocValues#ordValues} — the
 * bulk-ordinal API added on our local Lucene branch, mirroring {@code NumericDocValues#longValues}
 * — then folds the batch into the aggregator in one tight loop. Combined with dense
 * per-segment-ordinal counting the hot loop is a bulk decode plus an array increment per doc,
 * with no per-doc virtual dispatch and no hash.
 *
 * <p><b>Scope (POC):</b> single-valued sorted fields, root aggregation only
 * ({@code owningBucketOrd == 0}), scores not needed downstream. Callers must verify eligibility
 * before installing this collector.
 *
 * <p>The bulk path applies only when the batch lands in a dense run of the doc_values iterator
 * ({@code advanceExact(first) && docIDRunEnd() > last}); sparse batches fall back to per-doc
 * {@code advanceExact} so missing docs are skipped correctly.
 *
 * @opensearch.internal
 */
public final class BatchedOrdinalTermsLeafCollector extends LeafBucketCollector {

    /** Callback invoked once per resolved batch with parallel docid/ordinal arrays. */
    @FunctionalInterface
    public interface OrdinalBatchConsumer {
        void accept(int[] docs, int[] ords, int count) throws IOException;
    }

    public static final int BATCH_SIZE = 4096;

    private final SortedDocValues values;
    private final LeafBucketCollector sub;
    private final OrdinalBatchConsumer consumer;

    private final int[] docs = new int[BATCH_SIZE];
    private final int[] ords = new int[BATCH_SIZE];
    private int size = 0;

    private long bulkBatches = 0;
    private long sparseBatches = 0;

    public BatchedOrdinalTermsLeafCollector(SortedDocValues values, LeafBucketCollector sub, OrdinalBatchConsumer consumer) {
        this.values = values;
        this.sub = sub;
        this.consumer = consumer;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        // Batching delays sub.collect() until flush; eligibility gating (needsScores() == false)
        // must exclude score-consuming sub-aggs. Forward for the normal chain regardless.
        sub.setScorer(scorer);
    }

    @Override
    public void collect(int doc, long owningBucketOrd) throws IOException {
        assert owningBucketOrd == 0 : "batched collection is root-level only, got owningBucketOrd=" + owningBucketOrd;
        docs[size++] = doc;
        if (size == BATCH_SIZE) {
            flushBatch();
        }
    }

    private void flushBatch() throws IOException {
        if (size == 0) {
            return;
        }
        int first = docs[0];
        int last = docs[size - 1];

        boolean dense = false;
        if (values.docID() <= first) {
            if (values.docID() == first || values.advanceExact(first)) {
                dense = values.docIDRunEnd() > last;
            }
        }

        if (dense) {
            values.ordValues(size, docs, 0, ords, 0, -1);
            bulkBatches++;
            consumer.accept(docs, ords, size);
        } else {
            int kept = 0;
            for (int i = 0; i < size; i++) {
                if (values.advanceExact(docs[i])) {
                    docs[kept] = docs[i];
                    ords[kept] = values.ordValue();
                    kept++;
                }
            }
            sparseBatches++;
            if (kept > 0) {
                consumer.accept(docs, ords, kept);
            }
        }
        size = 0;
    }

    /** Flush any buffered tail. Must be called at segment end, before buildAggregations. */
    public void finish() throws IOException {
        flushBatch();
    }

    public long bulkBatches() {
        return bulkBatches;
    }

    public long sparseBatches() {
        return sparseBatches;
    }
}
