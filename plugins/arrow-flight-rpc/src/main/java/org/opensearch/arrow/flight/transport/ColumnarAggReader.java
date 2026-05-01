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
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.ColumnarTermsCodec;
import org.opensearch.search.aggregations.bucket.terms.InternalMultiTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus;
import org.opensearch.search.aggregations.metrics.ColumnarMetricCodec;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Reads Arrow columnar batches written by {@link ColumnarAggWriter} back into a
 * {@link QuerySearchResult} whose aggregations tree contains the expected
 * {@link StringTerms}/{@link LongTerms} shape.
 */
final class ColumnarAggReader {

    private final AggColumnarPlan plan;
    private final NamedWriteableRegistry registry;

    ColumnarAggReader(AggColumnarPlan plan, NamedWriteableRegistry registry) {
        this.plan = plan;
        this.registry = registry;
    }

    QuerySearchResult read(VectorSchemaRoot root) throws IOException {
        VarBinaryVector headerVec = AggColumnarSchema.varBinary(root, AggColumnarSchema.HEADER);
        BigIntVector docCountVec = AggColumnarSchema.bigInt(root, AggColumnarSchema.DOC_COUNT);
        BigIntVector docCountErrorVec = AggColumnarSchema.bigInt(root, AggColumnarSchema.DOC_COUNT_ERROR);

        VarBinaryVector termVarBinaryVec = null;
        BigIntVector termBigIntVec = null;
        if (plan.getTermKeyKind() == AggColumnarPlan.TermKeyKind.LONG) {
            termBigIntVec = AggColumnarSchema.bigInt(root, AggColumnarSchema.TERM);
        } else {
            // STRING or MULTI: VarBinary.
            termVarBinaryVec = AggColumnarSchema.varBinary(root, AggColumnarSchema.TERM);
        }

        MetricReader[] metricReaders = new MetricReader[plan.getMetrics().size()];
        for (int i = 0; i < metricReaders.length; i++) {
            metricReaders[i] = MetricReader.forEntry(root, plan.getMetrics().get(i));
        }

        byte[] headerBytes = headerVec.get(0);
        if (headerBytes == null || headerBytes.length == 0) {
            throw new IOException("Columnar batch missing header payload at row 0");
        }

        QuerySearchResult qsr;
        ColumnarTermsCodec.TermsHeader termsHeader = null;
        ColumnarTermsCodec.MultiTermsHeader multiHeader = null;
        try (StreamInput raw = new BytesStreamInput(headerBytes); StreamInput in = new NamedWriteableAwareStreamInput(raw, registry)) {
            qsr = new QuerySearchResult(in);
            if (plan.getTermKeyKind() == AggColumnarPlan.TermKeyKind.MULTI) {
                multiHeader = ColumnarTermsCodec.readMultiTermsHeader(in);
            } else {
                termsHeader = ColumnarTermsCodec.readTermsHeader(in);
            }
        }

        // Figure out bucket count. Use doc_count vector length — the header column has
        // its value count inflated to at least 1 but buckets live in the per-bucket vectors.
        int bucketCount = docCountVec.getValueCount();

        if (plan.getTermKeyKind() == AggColumnarPlan.TermKeyKind.MULTI) {
            List<InternalMultiTerms.Bucket> buckets = new ArrayList<>(bucketCount);
            for (int r = 0; r < bucketCount; r++) {
                byte[] keyBytes = termVarBinaryVec.get(r);
                List<Object> termValues;
                try (StreamInput in = new BytesStreamInput(keyBytes)) {
                    termValues = in.readList(StreamInput::readGenericValue);
                }
                long docCount = docCountVec.get(r);
                long bucketDocCountError = docCountErrorVec.isNull(r) ? 0L : docCountErrorVec.get(r);
                InternalAggregations subs = readSubAggs(r, metricReaders);
                buckets.add(
                    ColumnarTermsCodec.buildMultiTermsBucket(
                        termValues,
                        docCount,
                        subs,
                        multiHeader.showTermDocCountError,
                        bucketDocCountError,
                        multiHeader.termFormats
                    )
                );
            }
            InternalMultiTerms rebuilt = ColumnarTermsCodec.buildMultiTerms(multiHeader, buckets);
            qsr.aggregations(InternalAggregations.from(List.of(rebuilt)));
        } else if (plan.getTermKeyKind() == AggColumnarPlan.TermKeyKind.STRING) {
            List<StringTerms.Bucket> buckets = new ArrayList<>(bucketCount);
            for (int r = 0; r < bucketCount; r++) {
                BytesRef term = new BytesRef(termVarBinaryVec.get(r));
                long docCount = docCountVec.get(r);
                long bucketDocCountError = docCountErrorVec.isNull(r) ? 0L : docCountErrorVec.get(r);
                InternalAggregations subs = readSubAggs(r, metricReaders);
                buckets.add(
                    ColumnarTermsCodec.buildStringBucket(
                        term,
                        docCount,
                        subs,
                        termsHeader.showTermDocCountError,
                        bucketDocCountError,
                        termsHeader.format
                    )
                );
            }
            StringTerms rebuilt = ColumnarTermsCodec.buildStringTerms(termsHeader, buckets);
            qsr.aggregations(InternalAggregations.from(List.of(rebuilt)));
        } else {
            List<LongTerms.Bucket> buckets = new ArrayList<>(bucketCount);
            for (int r = 0; r < bucketCount; r++) {
                long term = termBigIntVec.get(r);
                long docCount = docCountVec.get(r);
                long bucketDocCountError = docCountErrorVec.isNull(r) ? 0L : docCountErrorVec.get(r);
                InternalAggregations subs = readSubAggs(r, metricReaders);
                buckets.add(
                    ColumnarTermsCodec.buildLongBucket(
                        term,
                        docCount,
                        subs,
                        termsHeader.showTermDocCountError,
                        bucketDocCountError,
                        termsHeader.format
                    )
                );
            }
            LongTerms rebuilt = ColumnarTermsCodec.buildLongTerms(termsHeader, buckets);
            qsr.aggregations(InternalAggregations.from(List.of(rebuilt)));
        }

        return qsr;
    }

    private InternalAggregations readSubAggs(int row, MetricReader[] readers) throws IOException {
        if (readers.length == 0) return InternalAggregations.EMPTY;
        List<InternalAggregation> out = new ArrayList<>(readers.length);
        for (int i = 0; i < readers.length; i++) {
            out.add(readers[i].read(row, plan.getMetrics().get(i).name));
        }
        return InternalAggregations.from(out);
    }

    /** Per-metric column reader. Mirror of MetricVectors in the writer. */
    private abstract static class MetricReader {
        abstract InternalAggregation read(int row, String aggName) throws IOException;

        static MetricReader forEntry(VectorSchemaRoot root, AggColumnarPlan.MetricEntry e) {
            switch (e.kind) {
                case CARDINALITY:
                    return new CardinalityReader(AggColumnarSchema.varBinary(root, e.name + AggColumnarSchema.SUFFIX_HLL));
                case MAX:
                    return new MaxReader(AggColumnarSchema.float8(root, e.name + AggColumnarSchema.SUFFIX_MAX));
                case MIN:
                    return new MinReader(AggColumnarSchema.float8(root, e.name + AggColumnarSchema.SUFFIX_MIN));
                case SUM:
                    return new SumReader(AggColumnarSchema.float8(root, e.name + AggColumnarSchema.SUFFIX_SUM_SCALAR));
                case AVG:
                    return new AvgReader(
                        AggColumnarSchema.float8(root, e.name + AggColumnarSchema.SUFFIX_SUM),
                        AggColumnarSchema.bigInt(root, e.name + AggColumnarSchema.SUFFIX_COUNT)
                    );
                case VALUE_COUNT:
                    return new ValueCountReader(AggColumnarSchema.bigInt(root, e.name + AggColumnarSchema.SUFFIX_COUNT));
                default:
                    throw new IllegalStateException("unknown kind " + e.kind);
            }
        }
    }

    private static final class CardinalityReader extends MetricReader {
        private final VarBinaryVector hllVec;

        CardinalityReader(VarBinaryVector hllVec) {
            this.hllVec = hllVec;
        }

        @Override
        InternalAggregation read(int row, String aggName) throws IOException {
            byte[] payload = hllVec.get(row);
            if (payload == null || payload.length == 0) {
                return ColumnarMetricCodec.buildCardinality(aggName, null, Collections.emptyMap());
            }
            try (StreamInput in = new BytesStreamInput(payload)) {
                AbstractHyperLogLogPlusPlus counts = AbstractHyperLogLogPlusPlus.readFrom(in, ColumnarMetricCodec.nonRecyclingBigArrays());
                return ColumnarMetricCodec.buildCardinality(aggName, counts, Collections.emptyMap());
            }
        }
    }

    private static final class MaxReader extends MetricReader {
        private final Float8Vector v;

        MaxReader(Float8Vector v) {
            this.v = v;
        }

        @Override
        InternalAggregation read(int row, String aggName) {
            return new InternalMax(aggName, v.get(row), org.opensearch.search.DocValueFormat.RAW, Collections.emptyMap());
        }
    }

    private static final class MinReader extends MetricReader {
        private final Float8Vector v;

        MinReader(Float8Vector v) {
            this.v = v;
        }

        @Override
        InternalAggregation read(int row, String aggName) {
            return new InternalMin(aggName, v.get(row), org.opensearch.search.DocValueFormat.RAW, Collections.emptyMap());
        }
    }

    private static final class SumReader extends MetricReader {
        private final Float8Vector v;

        SumReader(Float8Vector v) {
            this.v = v;
        }

        @Override
        InternalAggregation read(int row, String aggName) {
            return new InternalSum(aggName, v.get(row), org.opensearch.search.DocValueFormat.RAW, Collections.emptyMap());
        }
    }

    private static final class AvgReader extends MetricReader {
        private final Float8Vector sumVec;
        private final BigIntVector countVec;

        AvgReader(Float8Vector sumVec, BigIntVector countVec) {
            this.sumVec = sumVec;
            this.countVec = countVec;
        }

        @Override
        InternalAggregation read(int row, String aggName) {
            return new InternalAvg(
                aggName,
                sumVec.get(row),
                countVec.get(row),
                org.opensearch.search.DocValueFormat.RAW,
                Collections.emptyMap()
            );
        }
    }

    private static final class ValueCountReader extends MetricReader {
        private final BigIntVector countVec;

        ValueCountReader(BigIntVector countVec) {
            this.countVec = countVec;
        }

        @Override
        InternalAggregation read(int row, String aggName) {
            return new InternalValueCount(aggName, countVec.get(row), Collections.emptyMap());
        }
    }
}
