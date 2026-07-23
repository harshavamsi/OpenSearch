/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregation.ReduceContext;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.ColumnarTermsCodec;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus;
import org.opensearch.search.aggregations.metrics.ColumnarMetricCodec;
import org.opensearch.search.aggregations.metrics.CompensatedSum;
import org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.streaming.collection.ColumnarTermsFolderFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Coordinator-side vector fold for streaming terms aggregations. Replaces the
 * {@link ColumnarAggReader} + {@code StreamingTermsReducer} object round-trip on the eligible
 * receive path: instead of rebuilding a {@code LongTerms.Bucket}/{@code InternalMax}/... per
 * Arrow row and re-hashing them, each received batch is folded directly into per-query survivor
 * columns keyed by term.
 *
 * <p>Survivor state is bounded by {@code topN}: a removable term&rarr;ordinal map plus an ordinal
 * free-list backing parallel primitive columns ({@code long[] docCount}, per-metric
 * {@code double[]} / {@code long[]} accumulators, Kahan compensation for sum/avg, one 1-bucket
 * {@link HyperLogLogPlusPlus} per survivor for cardinality). Displacement (evict the
 * min-docCount survivor on overflow) and {@code otherDocCount} accounting replicate
 * {@code StreamingTermsReducer} exactly so streaming-vs-streaming results are identical.
 *
 * <p>The fold math mirrors the object reduce path bit-for-bit: sum/avg use
 * {@link CompensatedSum} (Kahan), cardinality merges register-wise via
 * {@link HyperLogLogPlusPlus#merge}, and {@link #finalizeAggregation} delegates topN selection /
 * min-doc-count / final ordering to
 * {@code org.opensearch.search.aggregations.bucket.terms.InternalTerms#reduce} on a one-element
 * list — the same trick {@code StreamingTermsReducer.finalize} uses.
 *
 * <p>Scope: {@code TermKeyKind.LONG}/{@code STRING} with the six metric sub-aggs; count- and
 * key-order. Ineligible shapes ({@code MULTI}, agg-ordering, topN over the cap) never reach this
 * class — the transport falls back to {@link ColumnarAggReader}.
 *
 * <p>Thread-safety: shard streams for one query arrive on multiple transport threads, so every
 * {@link #fold}/{@link #finalizeAggregation}/{@link #release} runs under {@code this} monitor.
 */
final class ColumnarTermsFolder implements ColumnarTermsFolderFactory.Folder {

    private final AggColumnarPlan plan;
    private final int topN;

    // Captured from the first folded batch; carries reduceOrder/order/format/etc. for finalize.
    private ColumnarTermsCodec.TermsHeader header;

    // Term identity -> survivor ordinal. Long key for LONG kind, BytesRef for STRING.
    private final Map<Object, Integer> termToOrdinal = new HashMap<>();

    // Ordinal-indexed survivor columns. Grown lazily up to topN.
    private long[] docCount = new long[0];
    private long[] docCountError = new long[0];      // first-seen per survivor; not merged (matches reducer)
    private boolean[] showError = new boolean[0];
    private Object[] termKey = new Object[0];         // Long or BytesRef, for finalize
    private MetricAccumulator[] metrics;

    // Ordinal allocation: reuse evicted ordinals before allocating fresh ones.
    private int nextFreshOrdinal = 0;
    private final List<Integer> freeOrdinals = new ArrayList<>();

    private long otherDocCount;

    // Indexed binary min-heap over survivor ordinals, keyed by docCount. heap[0] is the
    // min-docCount survivor (the displacement victim). heapPos maps ordinal -> heap index (-1 if
    // absent) so a merge that raises a survivor's docCount can sift it in O(log topN) instead of
    // rescanning all survivors. At topN=10k this was the fold's dominant cost (O(topN) per
    // displacement) on wide aggs like qheavy.
    private int[] heap = new int[0];
    private int[] heapPos = new int[0];
    private int heapSize = 0;

    private boolean anyFolded = false;

    ColumnarTermsFolder(AggColumnarPlan plan, int topN) {
        if (topN <= 0) {
            throw new IllegalArgumentException("topN must be positive, got " + topN);
        }
        this.plan = plan;
        this.topN = topN;
    }

    /**
     * Fold one received batch. The caller decodes {@code header} once (row 0); the first non-null
     * header becomes the finalize template. {@code bucketCount} is the number of bucket rows (the
     * doc_count vector's value count).
     */
    synchronized void fold(VectorSchemaRoot root, ColumnarTermsCodec.TermsHeader header, int bucketCount) {
        anyFolded = true;
        if (this.header == null) {
            this.header = header;
            initColumns();
        }
        // Per-batch "other" doc count folds in exactly as StreamingTermsReducer.accept does.
        otherDocCount += header.otherDocCount;

        BigIntVector docCountVec = AggColumnarSchema.bigInt(root, AggColumnarSchema.DOC_COUNT);
        BigIntVector docCountErrorVec = AggColumnarSchema.bigInt(root, AggColumnarSchema.DOC_COUNT_ERROR);
        VarBinaryVector termVarBinaryVec = null;
        BigIntVector termBigIntVec = null;
        if (plan.getTermKeyKind() == AggColumnarPlan.TermKeyKind.LONG) {
            termBigIntVec = AggColumnarSchema.bigInt(root, AggColumnarSchema.TERM);
        } else {
            termVarBinaryVec = AggColumnarSchema.varBinary(root, AggColumnarSchema.TERM);
        }

        // Bind each metric accumulator to this batch's vectors once, not per row.
        for (MetricAccumulator m : metrics) {
            m.bind(root);
        }

        for (int r = 0; r < bucketCount; r++) {
            Object key;
            if (termBigIntVec != null) {
                key = termBigIntVec.get(r);
            } else {
                key = new BytesRef(termVarBinaryVec.get(r));
            }
            long dc = docCountVec.get(r);
            boolean showErr = docCountErrorVec.isNull(r) == false;
            long dcErr = showErr ? docCountErrorVec.get(r) : 0L;

            Integer existing = termToOrdinal.get(key);
            if (existing != null) {
                mergeInto(existing, r, dc);
            } else if (termToOrdinal.size() < topN) {
                admit(key, r, dc, showErr, dcErr);
            } else {
                int minOrd = heap[0]; // heap root = min-docCount survivor
                if (dc > docCount[minOrd]) {
                    // Evict min survivor, admit this term.
                    otherDocCount += docCount[minOrd];
                    evict(minOrd);
                    admit(key, r, dc, showErr, dcErr);
                } else {
                    otherDocCount += dc;
                }
            }
        }
    }

    /** Admit a brand-new survivor for {@code key}, seeding all columns from batch row {@code r}. */
    private void admit(Object key, int r, long dc, boolean showErr, long dcErr) {
        int ord = allocateOrdinal();
        termToOrdinal.put(key, ord);
        termKey[ord] = key;
        docCount[ord] = dc;
        showError[ord] = showErr;
        docCountError[ord] = dcErr;
        for (MetricAccumulator m : metrics) {
            m.seed(ord, r);
        }
        heapInsert(ord);
    }

    /** Merge batch row {@code r} into the existing survivor at {@code ord}. */
    private void mergeInto(int ord, int r, long dc) {
        docCount[ord] += dc;
        for (MetricAccumulator m : metrics) {
            m.merge(ord, r);
        }
        // docCount only ever increases, so a merged survivor can only sift down (away from root).
        heapSiftDown(heapPos[ord]);
        // doc_count_error is intentionally not merged per-survivor — reconstructed at finalize,
        // matching StreamingTermsReducer / InternalTerms.reduce semantics.
    }

    private void evict(int minOrd) {
        termToOrdinal.remove(termKey[minOrd]);
        termKey[minOrd] = null;
        heapRemoveRoot(minOrd);
        for (MetricAccumulator m : metrics) {
            m.evict(minOrd);
        }
        freeOrdinals.add(minOrd);
    }

    // ---- indexed binary min-heap over survivor ordinals, keyed by docCount ----

    private void heapInsert(int ord) {
        int i = heapSize++;
        heap[i] = ord;
        heapPos[ord] = i;
        heapSiftUp(i);
    }

    /** Remove heap root (must equal {@code expectedOrd}); move last element up and sift down. */
    private void heapRemoveRoot(int expectedOrd) {
        assert heap[0] == expectedOrd : "evicting non-root survivor";
        heapPos[expectedOrd] = -1;
        int last = --heapSize;
        if (last > 0) {
            heap[0] = heap[last];
            heapPos[heap[0]] = 0;
            heapSiftDown(0);
        }
    }

    private void heapSiftUp(int i) {
        int ord = heap[i];
        long dc = docCount[ord];
        while (i > 0) {
            int parent = (i - 1) >>> 1;
            if (docCount[heap[parent]] <= dc) {
                break;
            }
            heap[i] = heap[parent];
            heapPos[heap[i]] = i;
            i = parent;
        }
        heap[i] = ord;
        heapPos[ord] = i;
    }

    private void heapSiftDown(int i) {
        int ord = heap[i];
        long dc = docCount[ord];
        int half = heapSize >>> 1;
        while (i < half) {
            int child = (i << 1) + 1;
            int right = child + 1;
            if (right < heapSize && docCount[heap[right]] < docCount[heap[child]]) {
                child = right;
            }
            if (docCount[heap[child]] >= dc) {
                break;
            }
            heap[i] = heap[child];
            heapPos[heap[i]] = i;
            i = child;
        }
        heap[i] = ord;
        heapPos[ord] = i;
    }

    private int allocateOrdinal() {
        if (freeOrdinals.isEmpty() == false) {
            return freeOrdinals.remove(freeOrdinals.size() - 1);
        }
        int ord = nextFreshOrdinal++;
        ensureCapacity(ord + 1);
        return ord;
    }

    private void initColumns() {
        List<AggColumnarPlan.MetricEntry> entries = plan.getMetrics();
        metrics = new MetricAccumulator[entries.size()];
        for (int i = 0; i < entries.size(); i++) {
            metrics[i] = MetricAccumulator.forEntry(entries.get(i));
        }
        // Seed with a modest capacity; grows toward topN only if that many unique terms arrive.
        ensureCapacity(Math.min(topN, 128));
    }

    private void ensureCapacity(int need) {
        if (need <= docCount.length) {
            return;
        }
        int newCap = Math.max(need, docCount.length + (docCount.length >> 1) + 1);
        newCap = Math.min(newCap, topN);
        newCap = Math.max(newCap, need);
        docCount = growLong(docCount, newCap);
        docCountError = growLong(docCountError, newCap);
        showError = growBool(showError, newCap);
        termKey = growObj(termKey, newCap);
        heap = growInt(heap, newCap);
        heapPos = growInt(heapPos, newCap);
        for (MetricAccumulator m : metrics) {
            m.ensureCapacity(newCap);
        }
    }

    private static int[] growInt(int[] a, int cap) {
        int[] n = new int[cap];
        System.arraycopy(a, 0, n, 0, a.length);
        return n;
    }

    private static long[] growLong(long[] a, int cap) {
        long[] n = new long[cap];
        System.arraycopy(a, 0, n, 0, a.length);
        return n;
    }

    private static boolean[] growBool(boolean[] a, int cap) {
        boolean[] n = new boolean[cap];
        System.arraycopy(a, 0, n, 0, a.length);
        return n;
    }

    private static Object[] growObj(Object[] a, int cap) {
        Object[] n = new Object[cap];
        System.arraycopy(a, 0, n, 0, a.length);
        return n;
    }

    /**
     * Materialize the merged survivors as a single {@code InternalTerms}, delegating final topN
     * selection / min-doc-count / ordering to {@code InternalTerms.reduce} on a one-element list —
     * exactly what {@code StreamingTermsReducer.finalize} does. Returns {@code null} if nothing was
     * folded.
     */
    @Override
    public synchronized InternalAggregation finalizeAggregation(ReduceContext ctx) {
        if (anyFolded == false || header == null) {
            return null;
        }
        InternalAggregation merged;
        if (plan.getTermKeyKind() == AggColumnarPlan.TermKeyKind.LONG) {
            List<LongTerms.Bucket> buckets = new ArrayList<>(termToOrdinal.size());
            for (int ord : termToOrdinal.values()) {
                long term = (Long) termKey[ord];
                buckets.add(
                    ColumnarTermsCodec.buildLongBucket(
                        term,
                        docCount[ord],
                        buildSubAggs(ord),
                        showError[ord],
                        docCountError[ord],
                        header.format
                    )
                );
            }
            merged = ColumnarTermsCodec.buildLongTerms(finalizeHeader(), buckets);
        } else {
            List<StringTerms.Bucket> buckets = new ArrayList<>(termToOrdinal.size());
            for (int ord : termToOrdinal.values()) {
                BytesRef term = (BytesRef) termKey[ord];
                buckets.add(
                    ColumnarTermsCodec.buildStringBucket(
                        term,
                        docCount[ord],
                        buildSubAggs(ord),
                        showError[ord],
                        docCountError[ord],
                        header.format
                    )
                );
            }
            merged = ColumnarTermsCodec.buildStringTerms(finalizeHeader(), buckets);
        }
        return merged.reduce(Collections.singletonList(merged), ctx);
    }

    /**
     * Header used to construct the synthetic merged terms. Carries the accumulated
     * {@code otherDocCount} and, like {@code StreamingTermsReducer.finalize}, a zero agg-level
     * {@code docCountError} — the reduce recomputes error from the (single) merged agg.
     */
    private ColumnarTermsCodec.TermsHeader finalizeHeader() {
        return new ColumnarTermsCodec.TermsHeader(
            header.name,
            header.metadata,
            header.reduceOrder,
            header.order,
            header.requiredSize,
            header.minDocCount,
            0L,
            header.format,
            header.shardSize,
            header.showTermDocCountError,
            otherDocCount
        );
    }

    private InternalAggregations buildSubAggs(int ord) {
        if (metrics.length == 0) {
            return InternalAggregations.EMPTY;
        }
        List<InternalAggregation> out = new ArrayList<>(metrics.length);
        for (int i = 0; i < metrics.length; i++) {
            out.add(metrics[i].build(ord, plan.getMetrics().get(i).name));
        }
        return InternalAggregations.from(out);
    }

    @Override
    public synchronized void release() {
        termToOrdinal.clear();
        freeOrdinals.clear();
        termKey = new Object[0];
        docCount = new long[0];
        docCountError = new long[0];
        showError = new boolean[0];
        heap = new int[0];
        heapPos = new int[0];
        heapSize = 0;
        if (metrics != null) {
            for (MetricAccumulator m : metrics) {
                m.release();
            }
            metrics = null;
        }
        header = null;
    }

    /** For tests/assertions: number of survivors currently held. */
    synchronized int size() {
        return termToOrdinal.size();
    }

    /** For tests/assertions: accumulated "other" doc count. */
    synchronized long otherDocCount() {
        return otherDocCount;
    }

    // ---- per-metric ordinal-indexed accumulators (mirror of ColumnarAggReader.MetricReader) ----

    private abstract static class MetricAccumulator {
        /** Cache this batch's typed vector(s) for the accumulator's column(s). */
        abstract void bind(VectorSchemaRoot root);

        abstract void ensureCapacity(int cap);

        /** Seed a freshly-admitted survivor at {@code ord} from batch row {@code r}. */
        abstract void seed(int ord, int r);

        /** Merge batch row {@code r} into the survivor at {@code ord}. */
        abstract void merge(int ord, int r);

        /** Drop the survivor at {@code ord} (release any off-heap state). */
        abstract void evict(int ord);

        abstract InternalAggregation build(int ord, String aggName);

        abstract void release();

        static MetricAccumulator forEntry(AggColumnarPlan.MetricEntry e) {
            switch (e.kind) {
                case CARDINALITY:
                    return new CardinalityAcc(e.name + AggColumnarSchema.SUFFIX_HLL);
                case MAX:
                    return new MaxAcc(e.name + AggColumnarSchema.SUFFIX_MAX);
                case MIN:
                    return new MinAcc(e.name + AggColumnarSchema.SUFFIX_MIN);
                case SUM:
                    return new SumAcc(e.name + AggColumnarSchema.SUFFIX_SUM_SCALAR);
                case AVG:
                    return new AvgAcc(e.name + AggColumnarSchema.SUFFIX_SUM, e.name + AggColumnarSchema.SUFFIX_COUNT);
                case VALUE_COUNT:
                    return new ValueCountAcc(e.name + AggColumnarSchema.SUFFIX_COUNT);
                default:
                    throw new IllegalStateException("unknown kind " + e.kind);
            }
        }
    }

    private static final class MaxAcc extends MetricAccumulator {
        private final String col;
        private double[] value = new double[0];
        private Float8Vector vec;

        MaxAcc(String col) {
            this.col = col;
        }

        @Override
        void bind(VectorSchemaRoot root) {
            vec = AggColumnarSchema.float8(root, col);
        }

        @Override
        void ensureCapacity(int cap) {
            value = grow(value, cap);
        }

        @Override
        void seed(int ord, int r) {
            value[ord] = vec.get(r);
        }

        @Override
        void merge(int ord, int r) {
            value[ord] = Math.max(value[ord], vec.get(r));
        }

        @Override
        void evict(int ord) {}

        @Override
        InternalAggregation build(int ord, String aggName) {
            return new InternalMax(aggName, value[ord], DocValueFormat.RAW, Collections.emptyMap());
        }

        @Override
        void release() {
            value = new double[0];
        }
    }

    private static final class MinAcc extends MetricAccumulator {
        private final String col;
        private double[] value = new double[0];
        private Float8Vector vec;

        MinAcc(String col) {
            this.col = col;
        }

        @Override
        void bind(VectorSchemaRoot root) {
            vec = AggColumnarSchema.float8(root, col);
        }

        @Override
        void ensureCapacity(int cap) {
            value = grow(value, cap);
        }

        @Override
        void seed(int ord, int r) {
            value[ord] = vec.get(r);
        }

        @Override
        void merge(int ord, int r) {
            value[ord] = Math.min(value[ord], vec.get(r));
        }

        @Override
        void evict(int ord) {}

        @Override
        InternalAggregation build(int ord, String aggName) {
            return new InternalMin(aggName, value[ord], DocValueFormat.RAW, Collections.emptyMap());
        }

        @Override
        void release() {
            value = new double[0];
        }
    }

    private static final class SumAcc extends MetricAccumulator {
        private final String col;
        private double[] value = new double[0];
        private double[] delta = new double[0];
        private final CompensatedSum scratch = new CompensatedSum(0, 0);
        private Float8Vector vec;

        SumAcc(String col) {
            this.col = col;
        }

        @Override
        void bind(VectorSchemaRoot root) {
            vec = AggColumnarSchema.float8(root, col);
        }

        @Override
        void ensureCapacity(int cap) {
            value = grow(value, cap);
            delta = grow(delta, cap);
        }

        @Override
        void seed(int ord, int r) {
            // Match InternalSum.reduce: a fresh Kahan tally that has added the first value.
            scratch.reset(0, 0);
            scratch.add(vec.get(r));
            value[ord] = scratch.value();
            delta[ord] = scratch.delta();
        }

        @Override
        void merge(int ord, int r) {
            scratch.reset(value[ord], delta[ord]);
            scratch.add(vec.get(r));
            value[ord] = scratch.value();
            delta[ord] = scratch.delta();
        }

        @Override
        void evict(int ord) {}

        @Override
        InternalAggregation build(int ord, String aggName) {
            return new InternalSum(aggName, value[ord], DocValueFormat.RAW, Collections.emptyMap());
        }

        @Override
        void release() {
            value = new double[0];
            delta = new double[0];
        }
    }

    private static final class AvgAcc extends MetricAccumulator {
        private final String sumCol;
        private final String countCol;
        private double[] value = new double[0];
        private double[] delta = new double[0];
        private long[] count = new long[0];
        private final CompensatedSum scratch = new CompensatedSum(0, 0);
        private Float8Vector sumVec;
        private BigIntVector countVec;

        AvgAcc(String sumCol, String countCol) {
            this.sumCol = sumCol;
            this.countCol = countCol;
        }

        @Override
        void bind(VectorSchemaRoot root) {
            sumVec = AggColumnarSchema.float8(root, sumCol);
            countVec = AggColumnarSchema.bigInt(root, countCol);
        }

        @Override
        void ensureCapacity(int cap) {
            value = grow(value, cap);
            delta = grow(delta, cap);
            count = growL(count, cap);
        }

        @Override
        void seed(int ord, int r) {
            scratch.reset(0, 0);
            scratch.add(sumVec.get(r));
            value[ord] = scratch.value();
            delta[ord] = scratch.delta();
            count[ord] = countVec.get(r);
        }

        @Override
        void merge(int ord, int r) {
            scratch.reset(value[ord], delta[ord]);
            scratch.add(sumVec.get(r));
            value[ord] = scratch.value();
            delta[ord] = scratch.delta();
            count[ord] += countVec.get(r);
        }

        @Override
        void evict(int ord) {}

        @Override
        InternalAggregation build(int ord, String aggName) {
            return new InternalAvg(aggName, value[ord], count[ord], DocValueFormat.RAW, Collections.emptyMap());
        }

        @Override
        void release() {
            value = new double[0];
            delta = new double[0];
            count = new long[0];
        }
    }

    private static final class ValueCountAcc extends MetricAccumulator {
        private final String col;
        private long[] count = new long[0];
        private BigIntVector vec;

        ValueCountAcc(String col) {
            this.col = col;
        }

        @Override
        void bind(VectorSchemaRoot root) {
            vec = AggColumnarSchema.bigInt(root, col);
        }

        @Override
        void ensureCapacity(int cap) {
            count = growL(count, cap);
        }

        @Override
        void seed(int ord, int r) {
            count[ord] = vec.get(r);
        }

        @Override
        void merge(int ord, int r) {
            count[ord] += vec.get(r);
        }

        @Override
        void evict(int ord) {}

        @Override
        InternalAggregation build(int ord, String aggName) {
            return new InternalValueCount(aggName, count[ord], Collections.emptyMap());
        }

        @Override
        void release() {
            count = new long[0];
        }
    }

    private static final class CardinalityAcc extends MetricAccumulator {
        private final String col;
        private AbstractHyperLogLogPlusPlus[] sketches = new AbstractHyperLogLogPlusPlus[0];
        private VarBinaryVector vec;

        CardinalityAcc(String col) {
            this.col = col;
        }

        @Override
        void bind(VectorSchemaRoot root) {
            vec = AggColumnarSchema.varBinary(root, col);
        }

        @Override
        void ensureCapacity(int cap) {
            if (cap <= sketches.length) {
                return;
            }
            AbstractHyperLogLogPlusPlus[] n = new AbstractHyperLogLogPlusPlus[cap];
            System.arraycopy(sketches, 0, n, 0, sketches.length);
            sketches = n;
        }

        @Override
        void seed(int ord, int r) {
            AbstractHyperLogLogPlusPlus incoming = decode(r);
            if (incoming == null) {
                sketches[ord] = null;
                return;
            }
            // One survivor-owned 1-bucket sketch; merge the incoming registers into it so the
            // survivor never aliases the decoded transient (which is freed after this batch).
            HyperLogLogPlusPlus dst = new HyperLogLogPlusPlus(incoming.precision(), BigArrays.NON_RECYCLING_INSTANCE, 1);
            dst.merge(0, incoming, 0);
            incoming.close();
            sketches[ord] = dst;
        }

        @Override
        void merge(int ord, int r) {
            AbstractHyperLogLogPlusPlus incoming = decode(r);
            if (incoming == null) {
                return;
            }
            HyperLogLogPlusPlus dst = (HyperLogLogPlusPlus) sketches[ord];
            if (dst == null) {
                dst = new HyperLogLogPlusPlus(incoming.precision(), BigArrays.NON_RECYCLING_INSTANCE, 1);
                sketches[ord] = dst;
            }
            dst.merge(0, incoming, 0);
            incoming.close();
        }

        @Override
        void evict(int ord) {
            if (sketches[ord] != null) {
                sketches[ord].close();
                sketches[ord] = null;
            }
        }

        @Override
        InternalAggregation build(int ord, String aggName) {
            return ColumnarMetricCodec.buildCardinality(aggName, sketches[ord], Collections.emptyMap());
        }

        @Override
        void release() {
            for (int i = 0; i < sketches.length; i++) {
                if (sketches[i] != null) {
                    sketches[i].close();
                    sketches[i] = null;
                }
            }
            sketches = new AbstractHyperLogLogPlusPlus[0];
        }

        private AbstractHyperLogLogPlusPlus decode(int r) {
            byte[] payload = vec.get(r);
            if (payload == null || payload.length == 0) {
                return null;
            }
            try (StreamInput in = new BytesStreamInput(payload)) {
                return AbstractHyperLogLogPlusPlus.readFrom(in, ColumnarMetricCodec.nonRecyclingBigArrays());
            } catch (IOException e) {
                throw new IllegalStateException("Failed to decode HLL payload during fold", e);
            }
        }
    }

    private static double[] grow(double[] a, int cap) {
        if (cap <= a.length) {
            return a;
        }
        double[] n = new double[cap];
        System.arraycopy(a, 0, n, 0, a.length);
        return n;
    }

    private static long[] growL(long[] a, int cap) {
        if (cap <= a.length) {
            return a;
        }
        long[] n = new long[cap];
        System.arraycopy(a, 0, n, 0, a.length);
        return n;
    }
}
