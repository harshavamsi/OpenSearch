/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming.collection;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Scorable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;

/**
 * Batched leaf collection for streaming long-terms aggregations (POC).
 *
 * <p>Instead of resolving doc_values per document inside {@code collect(doc, owningBucketOrd)},
 * this collector buffers matching docids into a fixed batch and, when full (or at segment end),
 * bulk-reads the group-by field via {@link NumericDocValues#longValues(int, int[], int, long[], int, long)}
 * — Lucene 10.5's codec-overridden bulk decode — then hands the docid and value arrays to the
 * aggregator's per-batch callback in one tight loop. Values are simultaneously appended to an
 * optional Arrow-backed {@link LongColumnSink} so the segment's column materializes off-heap as
 * a side effect of collection.
 *
 * <p><b>Scope (POC):</b> single-valued numeric fields, root aggregation only
 * ({@code owningBucketOrd == 0}), no include/exclude filter. Callers must verify eligibility
 * before installing this collector; anything else takes the classic per-doc path.
 *
 * <p>The bulk path applies only when the batch lands in a dense run of the doc_values iterator
 * ({@code advanceExact(first) && docIDRunEnd() > last}) — {@code longValues}'s defaultValue
 * cannot distinguish a missing doc from a real value, so sparse batches fall back to per-doc
 * {@code advanceExact} for correct semantics. Both paths are counted so the profile can report
 * which one actually ran.
 *
 * @opensearch.internal
 */
public final class BatchedLongTermsLeafCollector extends LeafBucketCollector implements Releasable {

    /** Callback invoked once per full batch with parallel docid/value arrays. */
    @FunctionalInterface
    public interface BatchConsumer {
        void accept(int[] docs, long[] values, int count) throws IOException;
    }

    public static final int BATCH_SIZE = 4096;

    private final SortedNumericDocValues dv;
    private final NumericDocValues singleton;
    private final LeafBucketCollector sub;
    private final BatchConsumer consumer;
    private final LongColumnSink sink;

    private final int[] docs = new int[BATCH_SIZE];
    private final long[] values = new long[BATCH_SIZE];
    private int size = 0;

    private long bulkBatches = 0;
    private long sparseBatches = 0;
    private long directSinkBatches = 0;
    private long copySinkBatches = 0;

    /**
     * @param dv the field's doc values for this segment
     * @param sub sub-aggregation collector chain (advanced per doc after batch resolution)
     * @param consumer aggregator callback that folds a resolved batch into bucket state
     * @param sink optional Arrow column sink; null disables column materialization
     * @return a batched collector, or null when the field is multi-valued in this segment
     *         (caller must fall back to the classic per-doc collector)
     */
    public static BatchedLongTermsLeafCollector tryCreate(
        SortedNumericDocValues dv,
        LeafBucketCollector sub,
        BatchConsumer consumer,
        LongColumnSink sink
    ) {
        NumericDocValues singleton = DocValues.unwrapSingleton(dv);
        if (singleton == null) {
            return null;
        }
        return new BatchedLongTermsLeafCollector(dv, singleton, sub, consumer, sink);
    }

    private BatchedLongTermsLeafCollector(
        SortedNumericDocValues dv,
        NumericDocValues singleton,
        LeafBucketCollector sub,
        BatchConsumer consumer,
        LongColumnSink sink
    ) {
        this.dv = dv;
        this.singleton = singleton;
        this.sub = sub;
        this.consumer = consumer;
        this.sink = sink;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        // Sub-aggs that need scores get the scorer through the normal chain. Note batching
        // delays sub.collect() until flush, so score-consuming sub-aggs would read the scorer
        // at the wrong doc — eligibility gating (needsScores() == false) must exclude them.
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

    /**
     * Resolve values for the buffered docids and fold them into the aggregator.
     *
     * <p>Docids arrive ascending (Lucene scorers emit in order), so the batch can be checked
     * against the iterator's current dense run in O(1). The iterator may already be positioned
     * past docs[0] by a previous batch's bulk read — longValues() advances it — in which case
     * docIDRunEnd() from an earlier position still bounds the run correctly because runs are
     * contiguous: {@code docID() <= docs[0] < docIDRunEnd()} implies every docid in
     * {@code [docs[0], last]} within the run has a value.
     */
    private void flushBatch() throws IOException {
        if (size == 0) {
            return;
        }
        int first = docs[0];
        int last = docs[size - 1];

        boolean dense = false;
        if (singleton.docID() <= first) {
            if (singleton.docID() == first || singleton.advanceExact(first)) {
                dense = singleton.docIDRunEnd() > last;
            }
        }

        if (dense) {
            // Direct sink decode first: the Arrow impl bulk-decodes straight into off-heap
            // memory via NumericDocValues#longValuesInto. Returns false when unsupported
            // (non-mmap directory, gcd/delta/table encodings) — then copy via appendLongs
            // after the heap decode below. Both reads are random-access over the same dense
            // run, so decoding the same batch twice is safe.
            boolean sinkFilled = false;
            if (sink != null) {
                sinkFilled = sink.appendFromDocValues(singleton, size, docs);
            }
            // Bulk decode: one call, codec-side loop. defaultValue is irrelevant — every doc
            // in the batch is proven to have a value. Kept unconditionally: the aggregator
            // consumer needs the heap values even when the sink took the direct path.
            singleton.longValues(size, docs, 0, values, 0, 0L);
            bulkBatches++;
            if (sink != null) {
                if (sinkFilled) {
                    directSinkBatches++;
                } else {
                    copySinkBatches++;
                    sink.appendLongs(values, 0, size);
                }
            }
            consumer.accept(docs, values, size);
        } else {
            // Sparse: per-doc existence checks; compact missing docs out of the batch so the
            // consumer only sees real values.
            int kept = 0;
            for (int i = 0; i < size; i++) {
                if (singleton.advanceExact(docs[i])) {
                    docs[kept] = docs[i];
                    values[kept] = singleton.longValue();
                    kept++;
                }
            }
            sparseBatches++;
            if (kept > 0) {
                if (sink != null) {
                    copySinkBatches++;
                    sink.appendLongs(values, 0, kept);
                }
                consumer.accept(docs, values, kept);
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

    /** Batches the sink decoded directly into its backing storage (no heap copy). */
    public long directSinkBatches() {
        return directSinkBatches;
    }

    /** Batches the sink received via the heap-array copy path. */
    public long copySinkBatches() {
        return copySinkBatches;
    }

    public LeafBucketCollector sub() {
        return sub;
    }

    @Override
    public void close() {
        Releasables.close(sink);
    }
}
