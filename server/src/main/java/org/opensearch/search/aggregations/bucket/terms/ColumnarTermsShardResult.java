/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.BytesRef;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus;
import org.opensearch.search.aggregations.metrics.ColumnarMetricSink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Emit-only carrier for the shard-side columnar terms path (Arrow end-to-end, step 2).
 *
 * <p>A streaming terms aggregator whose shape is columnar-eligible (root-level single-valued terms,
 * LONG/STRING key, the six metric sub-aggs, count/key order) builds this instead of an
 * {@code InternalTerms} with per-bucket {@code Bucket} + {@code InternalMax}/... objects. It holds
 * the survivors as parallel primitive columns read straight from the aggregator's ordinal-indexed
 * state (via {@link ColumnarMetricSink}) plus the non-bucket header fields
 * ({@link ColumnarTermsCodec.TermsHeader}-shaped). The Arrow plugin's {@code ColumnarAggWriter}
 * turns these columns into a {@code VectorSchemaRoot} at send; the object materialization the old
 * path did (buckets + metric aggs, then re-read back into vectors) is gone.
 *
 * <p><b>Emit-only / Flight-only.</b> This carrier exists solely to travel from
 * {@code buildAggregations} to the columnar writer within one node, over Flight transport. It never
 * participates in reduce, XContent, or generic serialization: {@link #doWriteTo} throws, and the
 * writer swaps it out of the {@code QuerySearchResult} (aggs &rarr; EMPTY) before serializing the
 * result header, exactly as the {@code InternalTerms} columnar path already does. Streaming search
 * always uses Flight; a hard guard is safer than maintaining a second serialization format for a
 * path that is never taken.
 *
 * @opensearch.internal
 */
public class ColumnarTermsShardResult extends InternalAggregation {

    /** Metric column: kind + name + the column payload, one entry per survivor row. */
    public static final class MetricColumn {
        public final String name;
        public final ColumnarMetricSink.Kind kind;
        // Exactly one of these is populated per kind (mirrors AggColumnarSchema column sets):
        // MAX/MIN/SUM -> doubles; AVG -> doubles(sum) + longs(count); VALUE_COUNT -> longs;
        // CARDINALITY -> hll (serialized sketch per row, zero-length = empty).
        public final double[] doubles;
        public final long[] longs;
        // CARDINALITY: independent HLL sketch clones, one per row (null entry = empty bucket).
        // Held as live objects (not bytes) so the Flight writer serializes each at most once and the
        // non-Flight fallback wraps them without decode.
        public final AbstractHyperLogLogPlusPlus[] hll;

        MetricColumn(String name, ColumnarMetricSink.Kind kind, double[] doubles, long[] longs, AbstractHyperLogLogPlusPlus[] hll) {
            this.name = name;
            this.kind = kind;
            this.doubles = doubles;
            this.longs = longs;
            this.hll = hll;
        }

        /** max/min/sum: a single double column. */
        public static MetricColumn scalar(String name, ColumnarMetricSink.Kind kind, double[] values) {
            return new MetricColumn(name, kind, values, null, null);
        }

        /** avg: sum + count columns. */
        public static MetricColumn avg(String name, double[] sums, long[] counts) {
            return new MetricColumn(name, ColumnarMetricSink.Kind.AVG, sums, counts, null);
        }

        /** value_count: a single long column. */
        public static MetricColumn valueCount(String name, long[] counts) {
            return new MetricColumn(name, ColumnarMetricSink.Kind.VALUE_COUNT, null, counts, null);
        }

        /** cardinality: one HLL sketch clone per row (null = empty). */
        public static MetricColumn cardinality(String name, AbstractHyperLogLogPlusPlus[] sketches) {
            return new MetricColumn(name, ColumnarMetricSink.Kind.CARDINALITY, null, null, sketches);
        }
    }

    /** Non-bucket header state, mirroring {@link ColumnarTermsCodec.TermsHeader}. */
    private final BucketOrder reduceOrder;
    private final BucketOrder order;
    private final int requiredSize;
    private final long minDocCount;
    private final DocValueFormat format;
    private final int shardSize;
    private final boolean showTermDocCountError;
    private final long otherDocCount;
    private final long docCountError;

    /** LONG key: term values; null for STRING. */
    private final long[] longKeys;
    /** STRING key: term values; null for LONG. */
    private final BytesRef[] bytesKeys;
    private final long[] docCounts;
    private final long[] bucketDocCountErrors; // valid only when showTermDocCountError
    private final List<MetricColumn> metrics;
    private final int rowCount;

    /** Lazily-built real terms for non-Flight consumers (serialize/reduce/XContent). */
    private volatile InternalMappedTerms<?, ?> materialized;

    public ColumnarTermsShardResult(
        String name,
        Map<String, Object> metadata,
        BucketOrder reduceOrder,
        BucketOrder order,
        int requiredSize,
        long minDocCount,
        DocValueFormat format,
        int shardSize,
        boolean showTermDocCountError,
        long otherDocCount,
        long docCountError,
        long[] longKeys,
        BytesRef[] bytesKeys,
        long[] docCounts,
        long[] bucketDocCountErrors,
        List<MetricColumn> metrics,
        int rowCount
    ) {
        super(name, metadata);
        this.reduceOrder = reduceOrder;
        this.order = order;
        this.requiredSize = requiredSize;
        this.minDocCount = minDocCount;
        this.format = format;
        this.shardSize = shardSize;
        this.showTermDocCountError = showTermDocCountError;
        this.otherDocCount = otherDocCount;
        this.docCountError = docCountError;
        this.longKeys = longKeys;
        this.bytesKeys = bytesKeys;
        this.docCounts = docCounts;
        this.bucketDocCountErrors = bucketDocCountErrors;
        this.metrics = metrics;
        this.rowCount = rowCount;
    }

    /** True for LONG keys, false for STRING keys. */
    public boolean isLongKey() {
        return longKeys != null;
    }

    public long[] longKeys() {
        return longKeys;
    }

    public BytesRef[] bytesKeys() {
        return bytesKeys;
    }

    public long[] docCounts() {
        return docCounts;
    }

    public long[] bucketDocCountErrors() {
        return bucketDocCountErrors;
    }

    public List<MetricColumn> metrics() {
        return metrics;
    }

    public int rowCount() {
        return rowCount;
    }

    /** Header bundle for the writer, in the same shape {@link ColumnarTermsCodec} reads/writes. */
    public ColumnarTermsCodec.TermsHeader header() {
        return new ColumnarTermsCodec.TermsHeader(
            getName(),
            getMetadata(),
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            docCountError,
            format,
            shardSize,
            showTermDocCountError,
            otherDocCount
        );
    }

    public boolean showTermDocCountError() {
        return showTermDocCountError;
    }

    /**
     * True iff every sub-aggregator implements {@link ColumnarMetricSink}, so this shape can emit
     * columnar. An empty sub-aggregator set is eligible (a bare terms agg with no metrics). Any
     * non-metric sub-agg (nested bucket agg, script, etc.) makes it ineligible.
     */
    public static boolean subAggsEligible(Aggregator[] subAggregators) {
        for (Aggregator sub : subAggregators) {
            if ((sub instanceof ColumnarMetricSink) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Assemble the metric columns for {@code rowCount} survivors whose bucket ordinals are
     * {@code bucketOrds[0..rowCount)} (parallel to the key/docCount columns the caller built), by
     * reading each sub-aggregator's ordinal-indexed state through {@link ColumnarMetricSink}. The
     * caller must have verified {@link #subAggsEligible}.
     */
    public static List<MetricColumn> buildMetricColumns(Aggregator[] subAggregators, long[] bucketOrds, int rowCount) throws IOException {
        List<MetricColumn> columns = new ArrayList<>(subAggregators.length);
        for (Aggregator sub : subAggregators) {
            ColumnarMetricSink sink = (ColumnarMetricSink) sub;
            ColumnarMetricSink.Kind kind = sink.columnarKind();
            String metricName = sub.name();
            switch (kind) {
                case MAX:
                case MIN:
                case SUM: {
                    double[] doubles = new double[rowCount];
                    ScalarSink cell = new ScalarSink();
                    for (int r = 0; r < rowCount; r++) {
                        sink.writeColumnarValue(bucketOrds[r], cell);
                        doubles[r] = cell.d;
                    }
                    columns.add(MetricColumn.scalar(metricName, kind, doubles));
                    break;
                }
                case AVG: {
                    double[] sums = new double[rowCount];
                    long[] counts = new long[rowCount];
                    ScalarSink cell = new ScalarSink();
                    for (int r = 0; r < rowCount; r++) {
                        sink.writeColumnarValue(bucketOrds[r], cell);
                        sums[r] = cell.d;
                        counts[r] = cell.l;
                    }
                    columns.add(MetricColumn.avg(metricName, sums, counts));
                    break;
                }
                case VALUE_COUNT: {
                    long[] longs = new long[rowCount];
                    ScalarSink cell = new ScalarSink();
                    for (int r = 0; r < rowCount; r++) {
                        sink.writeColumnarValue(bucketOrds[r], cell);
                        longs[r] = cell.l;
                    }
                    columns.add(MetricColumn.valueCount(metricName, longs));
                    break;
                }
                case CARDINALITY: {
                    AbstractHyperLogLogPlusPlus[] hll = new AbstractHyperLogLogPlusPlus[rowCount];
                    ScalarSink cell = new ScalarSink();
                    for (int r = 0; r < rowCount; r++) {
                        sink.writeColumnarValue(bucketOrds[r], cell);
                        hll[r] = cell.sketch;
                    }
                    columns.add(MetricColumn.cardinality(metricName, hll));
                    break;
                }
                default:
                    throw new IllegalStateException("unknown columnar metric kind " + kind);
            }
        }
        return columns;
    }

    /** Single-cell {@link ColumnarMetricSink.ValueSink} reused across rows within one column. */
    private static final class ScalarSink implements ColumnarMetricSink.ValueSink {
        double d;
        long l;
        AbstractHyperLogLogPlusPlus sketch;

        @Override
        public void putDouble(double value) {
            this.d = value;
        }

        @Override
        public void putSumCount(double sum, long count) {
            this.d = sum;
            this.l = count;
        }

        @Override
        public void putLong(long value) {
            this.l = value;
        }

        @Override
        public void putHll(AbstractHyperLogLogPlusPlus sketch) {
            this.sketch = sketch;
        }
    }

    // ---- InternalAggregation surface: emit-only, so everything below is unreachable-by-design ----

    @Override
    public String getWriteableName() {
        // The Flight columnar writer intercepts this carrier by type before serialization, so the
        // real InternalTerms writeable-name is never consulted on the fast path. For the fallback
        // (same-node shards, non-Flight transport) materialize() produces a real InternalTerms which
        // owns serialization; this name only appears if something serializes the carrier directly,
        // which materialize() prevents by delegating doWriteTo. Report the materialized type's name.
        return materialize().getWriteableName();
    }

    /**
     * Lazily build the equivalent {@link InternalMappedTerms} from the columns. Used by every
     * non-Flight consumer — same-node shards (circuit-breaker size accounting + in-process reduce)
     * and any non-Flight transport — because those paths serialize/reduce/render the aggregation
     * rather than routing it through the columnar writer. The Flight fast path reads the columns
     * directly and never triggers this. Cost is one O(rowCount) materialization, identical to what
     * the pre-columnar object path already paid per shard.
     */
    private synchronized InternalMappedTerms<?, ?> materialize() {
        if (materialized != null) {
            return materialized;
        }
        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(
            minDocCount,
            0,
            requiredSize,
            shardSize
        );
        if (isLongKey()) {
            List<LongTerms.Bucket> buckets = new ArrayList<>(rowCount);
            for (int r = 0; r < rowCount; r++) {
                buckets.add(
                    new LongTerms.Bucket(
                        longKeys[r],
                        docCounts[r],
                        materializeSubAggs(r),
                        showTermDocCountError,
                        showTermDocCountError ? bucketDocCountErrors[r] : 0L,
                        format
                    )
                );
            }
            materialized = new LongTerms(
                getName(),
                reduceOrder,
                order,
                getMetadata(),
                format,
                shardSize,
                showTermDocCountError,
                otherDocCount,
                buckets,
                docCountError,
                thresholds
            );
        } else {
            List<StringTerms.Bucket> buckets = new ArrayList<>(rowCount);
            for (int r = 0; r < rowCount; r++) {
                buckets.add(
                    new StringTerms.Bucket(
                        bytesKeys[r],
                        docCounts[r],
                        materializeSubAggs(r),
                        showTermDocCountError,
                        showTermDocCountError ? bucketDocCountErrors[r] : 0L,
                        format
                    )
                );
            }
            materialized = new StringTerms(
                getName(),
                reduceOrder,
                order,
                getMetadata(),
                format,
                shardSize,
                showTermDocCountError,
                otherDocCount,
                buckets,
                docCountError,
                thresholds
            );
        }
        return materialized;
    }

    /** Rebuild the metric sub-aggs for bucket row {@code r} from the columns. */
    private org.opensearch.search.aggregations.InternalAggregations materializeSubAggs(int r) {
        if (metrics.isEmpty()) {
            return org.opensearch.search.aggregations.InternalAggregations.EMPTY;
        }
        List<InternalAggregation> subs = new ArrayList<>(metrics.size());
        for (MetricColumn col : metrics) {
            subs.add(materializeMetric(col, r));
        }
        return org.opensearch.search.aggregations.InternalAggregations.from(subs);
    }

    private InternalAggregation materializeMetric(MetricColumn col, int r) {
        switch (col.kind) {
            case MAX:
                return new org.opensearch.search.aggregations.metrics.InternalMax(col.name, col.doubles[r], format, java.util.Map.of());
            case MIN:
                return new org.opensearch.search.aggregations.metrics.InternalMin(col.name, col.doubles[r], format, java.util.Map.of());
            case SUM:
                return new org.opensearch.search.aggregations.metrics.InternalSum(col.name, col.doubles[r], format, java.util.Map.of());
            case AVG:
                return new org.opensearch.search.aggregations.metrics.InternalAvg(
                    col.name,
                    col.doubles[r],
                    col.longs[r],
                    format,
                    java.util.Map.of()
                );
            case VALUE_COUNT:
                return new org.opensearch.search.aggregations.metrics.InternalValueCount(col.name, col.longs[r], java.util.Map.of());
            case CARDINALITY:
                return materializeCardinality(col, r);
            default:
                throw new IllegalStateException("unknown columnar metric kind " + col.kind);
        }
    }

    private InternalAggregation materializeCardinality(MetricColumn col, int r) {
        // The carrier holds a live sketch clone — wrap it directly, no decode.
        return org.opensearch.search.aggregations.metrics.ColumnarMetricCodec.buildCardinality(col.name, col.hll[r], java.util.Map.of());
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        // Fallback path only (same-node shards / non-Flight): serialize as the materialized terms.
        // InternalAggregation.writeTo already wrote name+metadata; InternalTerms.doWriteTo writes the
        // body (order/size/buckets), matching what a real InternalMappedTerms emits after name+metadata.
        materialize().doWriteTo(out);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        // Reduce as the materialized terms, substituting any sibling carriers with their materialized
        // form so mixed local/remote batches reduce uniformly.
        List<InternalAggregation> mapped = new ArrayList<>(aggregations.size());
        for (InternalAggregation agg : aggregations) {
            mapped.add(agg instanceof ColumnarTermsShardResult c ? c.materialize() : agg);
        }
        return materialize().reduce(mapped, reduceContext);
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        // Matches InternalMultiBucketAggregation (parent of all InternalTerms): always true.
        return true;
    }

    @Override
    public Object getProperty(List<String> path) {
        return materialize().getProperty(path);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return materialize().doXContentBody(builder, params);
    }
}
