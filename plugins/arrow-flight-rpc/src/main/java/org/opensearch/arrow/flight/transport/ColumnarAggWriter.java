/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.ColumnarTermsCodec;
import org.opensearch.search.aggregations.bucket.terms.ColumnarTermsShardResult;
import org.opensearch.search.aggregations.bucket.terms.InternalMappedTerms;
import org.opensearch.search.aggregations.bucket.terms.InternalMultiTerms;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;
import java.util.List;

/**
 * Writes a {@link QuerySearchResult} containing a streamable terms aggregation as a
 * typed Arrow {@link VectorSchemaRoot}. Column layout is governed by {@link AggColumnarPlan}
 * + {@link AggColumnarSchema}.
 *
 * <p>Strategy per batch:
 * <ol>
 *   <li>Extract the top-level terms agg from the QSR. Swap its aggs out for {@link
 *       InternalAggregations#EMPTY} (O(1) mutation) and serialize the QSR shell into
 *       the {@code header} column (row 0).</li>
 *   <li>Serialize the terms non-bucket state into the same header blob (appended).</li>
 *   <li>Emit one row per bucket into the typed columns. Sub-agg values are pulled via
 *       public getters / {@link InternalCardinality#getCounts()} — no redundant per-bucket
 *       NamedWriteable framing.</li>
 * </ol>
 *
 * <p>The writer owns its own {@link VectorSchemaRoot} for the life of the stream because
 * Arrow Flight locks the schema on first batch; FlightServerChannel reuses it across batches.
 */
final class ColumnarAggWriter implements AutoCloseable {

    private final AggColumnarPlan plan;
    private final VectorSchemaRoot root;

    // Vector handles cached to avoid per-batch string lookups.
    private final VarBinaryVector headerVec;
    private final VarBinaryVector termVarBinaryVec; // populated only for STRING key kind
    private final BigIntVector termBigIntVec;       // populated only for LONG key kind
    private final BigIntVector docCountVec;
    private final BigIntVector docCountErrorVec;
    private final MetricVectors[] metricVecs;

    ColumnarAggWriter(AggColumnarPlan plan, BufferAllocator allocator) {
        this.plan = plan;
        this.root = AggColumnarSchema.createRoot(AggColumnarSchema.build(plan), allocator);
        this.headerVec = AggColumnarSchema.varBinary(root, AggColumnarSchema.HEADER);
        if (plan.getTermKeyKind() == AggColumnarPlan.TermKeyKind.LONG) {
            this.termVarBinaryVec = null;
            this.termBigIntVec = AggColumnarSchema.bigInt(root, AggColumnarSchema.TERM);
        } else {
            // STRING and MULTI both use a VarBinary term column (MULTI stores the
            // writeGenericValue-list blob) — must mirror AggColumnarSchema.build.
            this.termVarBinaryVec = AggColumnarSchema.varBinary(root, AggColumnarSchema.TERM);
            this.termBigIntVec = null;
        }
        this.docCountVec = AggColumnarSchema.bigInt(root, AggColumnarSchema.DOC_COUNT);
        this.docCountErrorVec = AggColumnarSchema.bigInt(root, AggColumnarSchema.DOC_COUNT_ERROR);

        List<AggColumnarPlan.MetricEntry> metrics = plan.getMetrics();
        this.metricVecs = new MetricVectors[metrics.size()];
        for (int i = 0; i < metrics.size(); i++) {
            metricVecs[i] = MetricVectors.forEntry(root, metrics.get(i));
        }
    }

    VectorSchemaRoot getRoot() {
        return root;
    }

    /**
     * Serialize one QSR batch into the root's columns. Resets vectors first so successive
     * batches don't retain the prior contents.
     */
    void write(QuerySearchResult qsr) throws IOException {
        // Reset vectors from any prior batch. VectorSchemaRoot doesn't do this automatically.
        resetVectors();

        InternalAggregations aggs = qsr.aggregations().expand();
        InternalTerms<?, ?> terms = findTopLevelTerms(aggs);
        if (terms == null) {
            throw new IllegalStateException("Columnar writer invoked on non-eligible agg tree");
        }
        boolean isMulti = terms instanceof InternalMultiTerms;

        // Step 1: Write header (QSR-without-aggs + terms non-bucket state) into row 0 of the
        // header column. Temporarily swap aggs -> EMPTY to avoid redundant serialization of
        // the bucket list through the standard path.
        try (BytesStreamOutput headerOut = new BytesStreamOutput()) {
            qsr.aggregations(InternalAggregations.EMPTY);
            try {
                qsr.writeTo(headerOut);
            } finally {
                // Restore so the shard-side QSR isn't mutated beyond this call.
                qsr.aggregations(aggs);
            }
            if (isMulti) {
                ColumnarTermsCodec.writeMultiTermsHeader((InternalMultiTerms) terms, headerOut);
            } else {
                ColumnarTermsCodec.writeTermsHeader((InternalMappedTerms<?, ?>) terms, headerOut);
            }
            BytesReference hdrBytes = headerOut.bytes();
            byte[] hdrArr = BytesReference.toBytes(hdrBytes);
            headerVec.setSafe(0, hdrArr, 0, hdrArr.length);
        }

        // Step 2: Per-bucket columns.
        List<?> buckets = terms.getBuckets();
        int rowCount = buckets.size();
        for (int r = 0; r < rowCount; r++) {
            long docCount;
            boolean showErr;
            long bucketErr;
            InternalAggregations subs;

            if (isMulti) {
                InternalMultiTerms.Bucket mb = (InternalMultiTerms.Bucket) buckets.get(r);
                // Encode the composite termValues list as a writeGenericValue blob in the term column.
                try (BytesStreamOutput keyOut = new BytesStreamOutput()) {
                    keyOut.writeCollection(mb.getTermValues(), StreamOutput::writeGenericValue);
                    byte[] keyBytes = BytesReference.toBytes(keyOut.bytes());
                    termVarBinaryVec.setSafe(r, keyBytes, 0, keyBytes.length);
                }
                docCount = mb.getDocCount();
                showErr = mb.showDocCountError();
                // Same guard as the InternalTerms.Bucket branch below.
                bucketErr = showErr ? mb.getDocCountError() : 0L;
                subs = (InternalAggregations) mb.getAggregations();
            } else {
                InternalTerms.Bucket<?> b = (InternalTerms.Bucket<?>) buckets.get(r);
                if (termVarBinaryVec != null) {
                    BytesRef key = toBytesRef(b.getKey());
                    termVarBinaryVec.setSafe(r, key.bytes, key.offset, key.length);
                } else {
                    termBigIntVec.setSafe(r, ((Number) b.getKey()).longValue());
                }
                docCount = b.getDocCount();
                showErr = b.showDocCountError();
                // InternalTerms.Bucket.getDocCountError() throws IllegalStateException when
                // show_term_doc_count_error is false (the default for terms aggs). Only call it
                // when the bucket actually tracks the error — otherwise we'd crash serialization
                // of every streaming terms result that didn't explicitly opt in.
                bucketErr = showErr ? b.getDocCountError() : 0L;
                subs = (InternalAggregations) b.getAggregations();
            }

            docCountVec.setSafe(r, docCount);
            if (showErr) {
                docCountErrorVec.setSafe(r, bucketErr);
            } else {
                docCountErrorVec.setNull(r);
            }

            List<? extends Aggregation> subsList = subs == null ? List.of() : subs.asList();
            if (subsList.size() != metricVecs.length) {
                throw new IllegalStateException(
                    "Bucket " + r + " sub-agg count " + subsList.size() + " disagrees with plan " + metricVecs.length
                );
            }
            for (int i = 0; i < metricVecs.length; i++) {
                metricVecs[i].write(r, subsList.get(i));
            }
        }

        // Arrow requires valueCount be set explicitly after a round of setSafe calls.
        // Header column's value count must cover the full row span so row 0's bytes land;
        // unset rows become null cells.
        int totalRows = Math.max(rowCount, 1);
        headerVec.setValueCount(totalRows);
        if (termVarBinaryVec != null) termVarBinaryVec.setValueCount(rowCount);
        else termBigIntVec.setValueCount(rowCount);
        docCountVec.setValueCount(rowCount);
        docCountErrorVec.setValueCount(rowCount);
        for (MetricVectors m : metricVecs)
            m.setValueCount(rowCount);
        root.setRowCount(totalRows);
    }

    /**
     * Serialize a shard-side columnar carrier directly from its column arrays — no per-bucket
     * object walk. Twin of {@link #write(QuerySearchResult)}: same header shell + schema, but the
     * key/docCount/metric values come from the carrier's primitive columns (which the aggregator
     * read straight from ordinal-indexed metric state) instead of {@code InternalTerms.Bucket} +
     * {@code InternalMax}/... objects.
     *
     * @param qsr the result shell to serialize into the header (its aggs are swapped to EMPTY, as
     *            in {@link #write}); the carrier supplies all bucket/metric columns
     * @param carrier the columnar payload
     */
    void writeFromColumns(QuerySearchResult qsr, ColumnarTermsShardResult carrier) throws IOException {
        resetVectors();

        // Header: QSR shell (aggs stripped) + terms non-bucket state from the carrier's header.
        InternalAggregations aggs = qsr.aggregations() == null ? null : qsr.aggregations().expand();
        try (BytesStreamOutput headerOut = new BytesStreamOutput()) {
            qsr.aggregations(InternalAggregations.EMPTY);
            try {
                qsr.writeTo(headerOut);
            } finally {
                qsr.aggregations(aggs);
            }
            ColumnarTermsCodec.writeTermsHeader(carrier.header(), headerOut);
            byte[] hdrArr = BytesReference.toBytes(headerOut.bytes());
            headerVec.setSafe(0, hdrArr, 0, hdrArr.length);
        }

        int rowCount = carrier.rowCount();
        boolean showErr = carrier.showTermDocCountError();
        long[] docCounts = carrier.docCounts();
        long[] bucketErrors = carrier.bucketDocCountErrors();

        for (int r = 0; r < rowCount; r++) {
            if (termBigIntVec != null) {
                termBigIntVec.setSafe(r, carrier.longKeys()[r]);
            } else {
                BytesRef key = carrier.bytesKeys()[r];
                termVarBinaryVec.setSafe(r, key.bytes, key.offset, key.length);
            }
            docCountVec.setSafe(r, docCounts[r]);
            if (showErr) {
                docCountErrorVec.setSafe(r, bucketErrors[r]);
            } else {
                docCountErrorVec.setNull(r);
            }
        }

        List<ColumnarTermsShardResult.MetricColumn> cols = carrier.metrics();
        if (cols.size() != metricVecs.length) {
            throw new IllegalStateException("carrier metric count " + cols.size() + " disagrees with plan " + metricVecs.length);
        }
        for (int i = 0; i < metricVecs.length; i++) {
            metricVecs[i].writeColumn(cols.get(i), rowCount);
        }

        int totalRows = Math.max(rowCount, 1);
        headerVec.setValueCount(totalRows);
        if (termVarBinaryVec != null) termVarBinaryVec.setValueCount(rowCount);
        else termBigIntVec.setValueCount(rowCount);
        docCountVec.setValueCount(rowCount);
        docCountErrorVec.setValueCount(rowCount);
        for (MetricVectors m : metricVecs)
            m.setValueCount(rowCount);
        root.setRowCount(totalRows);
    }

    private static InternalTerms<?, ?> findTopLevelTerms(InternalAggregations aggs) {
        if (aggs == null) return null;
        List<? extends Aggregation> top = aggs.asList();
        if (top.size() != 1) return null;
        Aggregation only = top.get(0);
        if (only instanceof InternalMappedTerms<?, ?> mt) return mt;
        if (only instanceof InternalMultiTerms mt) return mt;
        return null;
    }

    private static BytesRef toBytesRef(Object key) {
        if (key instanceof BytesRef br) return br;
        if (key instanceof String s) return new BytesRef(s);
        throw new IllegalStateException("Unexpected string-key type: " + key.getClass());
    }

    private void resetVectors() {
        // Arrow vectors must be reset between batches so setSafe rewrites from index 0.
        headerVec.reset();
        if (termVarBinaryVec != null) termVarBinaryVec.reset();
        else termBigIntVec.reset();
        docCountVec.reset();
        docCountErrorVec.reset();
        for (MetricVectors m : metricVecs)
            m.reset();
    }

    @Override
    public void close() {
        root.close();
    }

    /**
     * Per-metric vector accessor, dispatched once per plan.
     */
    private abstract static class MetricVectors {
        abstract void write(int row, Aggregation agg) throws IOException;

        /** Bulk-write an entire metric column (shard-side columnar emit path). */
        abstract void writeColumn(ColumnarTermsShardResult.MetricColumn col, int rowCount) throws IOException;

        abstract void setValueCount(int n);

        abstract void reset();

        static MetricVectors forEntry(VectorSchemaRoot root, AggColumnarPlan.MetricEntry e) {
            switch (e.kind) {
                case CARDINALITY:
                    return new CardinalityVectors(AggColumnarSchema.varBinary(root, e.name + AggColumnarSchema.SUFFIX_HLL));
                case MAX:
                    return new ScalarVectors(AggColumnarSchema.float8(root, e.name + AggColumnarSchema.SUFFIX_MAX), e.kind);
                case MIN:
                    return new ScalarVectors(AggColumnarSchema.float8(root, e.name + AggColumnarSchema.SUFFIX_MIN), e.kind);
                case SUM:
                    return new ScalarVectors(AggColumnarSchema.float8(root, e.name + AggColumnarSchema.SUFFIX_SUM_SCALAR), e.kind);
                case AVG:
                    return new AvgVectors(
                        AggColumnarSchema.float8(root, e.name + AggColumnarSchema.SUFFIX_SUM),
                        AggColumnarSchema.bigInt(root, e.name + AggColumnarSchema.SUFFIX_COUNT)
                    );
                case VALUE_COUNT:
                    return new ValueCountVectors(AggColumnarSchema.bigInt(root, e.name + AggColumnarSchema.SUFFIX_COUNT));
                default:
                    throw new IllegalStateException("unknown kind " + e.kind);
            }
        }
    }

    private static final class CardinalityVectors extends MetricVectors {
        private final VarBinaryVector hllVec;

        CardinalityVectors(VarBinaryVector hllVec) {
            this.hllVec = hllVec;
        }

        @Override
        void write(int row, Aggregation agg) throws IOException {
            InternalCardinality card = (InternalCardinality) agg;
            if (card.getCounts() == null) {
                hllVec.setSafe(row, new byte[0], 0, 0);
                return;
            }
            try (BytesStreamOutput tmp = new BytesStreamOutput()) {
                // AbstractHyperLogLogPlusPlus.writeTo already uses the bulk register fast-path
                // we added in Session 4.
                card.getCounts().writeTo(0, tmp);
                byte[] payload = BytesReference.toBytes(tmp.bytes());
                hllVec.setSafe(row, payload, 0, payload.length);
            }
        }

        @Override
        void writeColumn(ColumnarTermsShardResult.MetricColumn col, int rowCount) throws IOException {
            // Serialize each survivor's sketch clone once, here on the Flight fast path (null = empty
            // bucket -> zero-length cell, which the reader/folder treats as "no counts").
            for (int r = 0; r < rowCount; r++) {
                org.opensearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus sketch = col.hll[r];
                if (sketch == null) {
                    hllVec.setSafe(r, new byte[0], 0, 0);
                    continue;
                }
                try (BytesStreamOutput tmp = new BytesStreamOutput()) {
                    sketch.writeTo(0, tmp);
                    byte[] payload = BytesReference.toBytes(tmp.bytes());
                    hllVec.setSafe(r, payload, 0, payload.length);
                }
            }
        }

        @Override
        void setValueCount(int n) {
            hllVec.setValueCount(n);
        }

        @Override
        void reset() {
            hllVec.reset();
        }
    }

    private static final class ScalarVectors extends MetricVectors {
        private final Float8Vector valueVec;
        private final AggColumnarPlan.MetricKind kind;

        ScalarVectors(Float8Vector valueVec, AggColumnarPlan.MetricKind kind) {
            this.valueVec = valueVec;
            this.kind = kind;
        }

        @Override
        void write(int row, Aggregation agg) {
            double v;
            switch (kind) {
                case MAX:
                    v = ((InternalMax) agg).getValue();
                    break;
                case MIN:
                    v = ((InternalMin) agg).getValue();
                    break;
                case SUM:
                    v = ((InternalSum) agg).getValue();
                    break;
                default:
                    throw new IllegalStateException();
            }
            valueVec.setSafe(row, v);
        }

        @Override
        void writeColumn(ColumnarTermsShardResult.MetricColumn col, int rowCount) {
            for (int r = 0; r < rowCount; r++) {
                valueVec.setSafe(r, col.doubles[r]);
            }
        }

        @Override
        void setValueCount(int n) {
            valueVec.setValueCount(n);
        }

        @Override
        void reset() {
            valueVec.reset();
        }
    }

    private static final class AvgVectors extends MetricVectors {
        private final Float8Vector sumVec;
        private final BigIntVector countVec;

        AvgVectors(Float8Vector sumVec, BigIntVector countVec) {
            this.sumVec = sumVec;
            this.countVec = countVec;
        }

        @Override
        void write(int row, Aggregation agg) {
            InternalAvg avg = (InternalAvg) agg;
            sumVec.setSafe(row, avg.getSum());
            countVec.setSafe(row, avg.getCount());
        }

        @Override
        void writeColumn(ColumnarTermsShardResult.MetricColumn col, int rowCount) {
            for (int r = 0; r < rowCount; r++) {
                sumVec.setSafe(r, col.doubles[r]);
                countVec.setSafe(r, col.longs[r]);
            }
        }

        @Override
        void setValueCount(int n) {
            sumVec.setValueCount(n);
            countVec.setValueCount(n);
        }

        @Override
        void reset() {
            sumVec.reset();
            countVec.reset();
        }
    }

    private static final class ValueCountVectors extends MetricVectors {
        private final BigIntVector countVec;

        ValueCountVectors(BigIntVector countVec) {
            this.countVec = countVec;
        }

        @Override
        void write(int row, Aggregation agg) {
            countVec.setSafe(row, ((InternalValueCount) agg).getValue());
        }

        @Override
        void writeColumn(ColumnarTermsShardResult.MetricColumn col, int rowCount) {
            for (int r = 0; r < rowCount; r++) {
                countVec.setSafe(r, col.longs[r]);
            }
        }

        @Override
        void setValueCount(int n) {
            countVec.setValueCount(n);
        }

        @Override
        void reset() {
            countVec.reset();
        }
    }

}
