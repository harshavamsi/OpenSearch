/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.ColumnarTermsShardResult;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.metrics.ColumnarMetricSink;
import org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Equivalence: the shard-side columnar emit path ({@link ColumnarTermsShardResult} +
 * {@link ColumnarAggWriter#writeFromColumns}) must produce the same Arrow batch as the object emit
 * path ({@code InternalTerms} + {@link ColumnarAggWriter#write}), verified by reading both back
 * through {@link ColumnarAggReader} and comparing the reconstructed aggregations.
 */
public class ColumnarShardEmitEquivalenceTests extends OpenSearchTestCase {

    private RootAllocator allocator;
    private NamedWriteableRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator(Long.MAX_VALUE);
        registry = new NamedWriteableRegistry(
            Arrays.asList(
                new NamedWriteableRegistry.Entry(DocValueFormat.class, DocValueFormat.RAW.getWriteableName(), in -> DocValueFormat.RAW)
            )
        );
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testLongTermsAllMetricsEmitEquivalence() throws Exception {
        // Object path source.
        List<LongTerms.Bucket> buckets = Arrays.asList(
            longBucket(10, 100, metrics(3.0, -1.0, 12.0, 24.0, 6, 111)),
            longBucket(20, 50, metrics(9.0, 2.0, 8.0, 16.0, 4, 222))
        );
        LongTerms terms = new LongTerms(
            "t",
            BucketOrder.key(true),
            BucketOrder.count(false),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            1000,
            false,
            7L,
            buckets,
            0L,
            new TermsAggregator.BucketCountThresholds(1, 0, 10, 1000)
        );
        // Carrier path source: same keys/docCounts/metrics as columns.
        ColumnarTermsShardResult carrier = longCarrier(
            new long[] { 10, 20 },
            new long[] { 100, 50 },
            7L,
            new double[] { 3.0, 9.0 },
            new double[] { -1.0, 2.0 },
            new double[] { 12.0, 8.0 },
            new double[] { 24.0, 16.0 },
            new long[] { 6, 4 },
            new long[] { 111, 222 }
        );
        assertEmitEquivalent(terms, carrier);
    }

    public void testStringTermsSumOnlyEmitEquivalence() throws Exception {
        List<StringTerms.Bucket> buckets = Arrays.asList(
            stringBucket("a", 100, new InternalSum("sm", 12.0, DocValueFormat.RAW, Collections.emptyMap())),
            stringBucket("b", 50, new InternalSum("sm", 8.0, DocValueFormat.RAW, Collections.emptyMap()))
        );
        StringTerms terms = new StringTerms(
            "t",
            BucketOrder.key(true),
            BucketOrder.count(false),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            1000,
            false,
            3L,
            buckets,
            0L,
            new TermsAggregator.BucketCountThresholds(1, 0, 10, 1000)
        );
        ColumnarTermsShardResult.MetricColumn sumCol = ColumnarTermsShardResult.MetricColumn.scalar(
            "sm",
            ColumnarMetricSink.Kind.SUM,
            new double[] { 12.0, 8.0 }
        );
        ColumnarTermsShardResult carrier = new ColumnarTermsShardResult(
            "t",
            Collections.emptyMap(),
            BucketOrder.key(true),
            BucketOrder.count(false),
            10,
            1L,
            DocValueFormat.RAW,
            1000,
            false,
            3L,
            0L,
            null,
            new BytesRef[] { new BytesRef("a"), new BytesRef("b") },
            new long[] { 100, 50 },
            null,
            Collections.singletonList(sumCol),
            2
        );
        assertEmitEquivalent(terms, carrier);
    }

    private void assertEmitEquivalent(InternalAggregation objectTerms, ColumnarTermsShardResult carrier) throws Exception {
        String objectForm = emitAndRead(wrap(objectTerms), null);
        String carrierForm = emitAndRead(wrap(carrier), carrier);
        assertEquals(objectForm, carrierForm);
    }

    /** Write via the object or carrier path, read back, and render a canonical string. */
    private String emitAndRead(QuerySearchResult qsr, ColumnarTermsShardResult carrier) throws Exception {
        AggColumnarPlan plan = AggColumnarPlan.detect(qsr).orElseThrow();
        try (ColumnarAggWriter writer = new ColumnarAggWriter(plan, allocator)) {
            if (carrier != null) {
                writer.writeFromColumns(qsr, carrier);
            } else {
                writer.write(qsr);
            }
            VectorSchemaRoot root = writer.getRoot();
            AggColumnarPlan planFromSchema = ColumnarPlanFromSchema.build(root.getSchema());
            ColumnarAggReader reader = new ColumnarAggReader(planFromSchema, registry);
            QuerySearchResult out = reader.read(root);
            return render(out.aggregations().expand());
        }
    }

    private String render(InternalAggregations aggs) {
        org.opensearch.search.aggregations.bucket.terms.Terms terms = (org.opensearch.search.aggregations.bucket.terms.Terms) aggs.asList()
            .get(0);
        List<String> lines = new ArrayList<>();
        for (org.opensearch.search.aggregations.bucket.terms.Terms.Bucket b : terms.getBuckets()) {
            List<String> metricLines = new ArrayList<>();
            for (Aggregation sub : b.getAggregations().asList()) {
                metricLines.add(renderMetric(sub));
            }
            Collections.sort(metricLines);
            lines.add("key=" + b.getKeyAsString() + " dc=" + b.getDocCount() + " " + metricLines);
        }
        Collections.sort(lines);
        return lines.toString();
    }

    private String renderMetric(Aggregation sub) {
        if (sub instanceof InternalMax m) return sub.getName() + ":max=" + m.getValue();
        if (sub instanceof InternalMin m) return sub.getName() + ":min=" + m.getValue();
        if (sub instanceof InternalSum m) return sub.getName() + ":sum=" + m.getValue();
        if (sub instanceof InternalAvg m) return sub.getName() + ":avg=" + m.getValue();
        if (sub instanceof InternalValueCount m) return sub.getName() + ":vc=" + m.getValue();
        if (sub instanceof InternalCardinality m) return sub.getName() + ":card=" + m.getValue();
        return sub.getName() + ":?";
    }

    private QuerySearchResult wrap(InternalAggregation agg) {
        QuerySearchResult qsr = new QuerySearchResult(
            new org.opensearch.search.internal.ShardSearchContextId(org.opensearch.common.UUIDs.base64UUID(), 1L),
            new org.opensearch.search.SearchShardTarget(
                "node",
                new org.opensearch.core.index.shard.ShardId("idx", "uuid", 0),
                null,
                org.opensearch.action.OriginalIndices.NONE
            ),
            null
        );
        org.apache.lucene.search.TopDocs topDocs = new org.apache.lucene.search.TopDocs(
            new org.apache.lucene.search.TotalHits(0, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO),
            new org.apache.lucene.search.ScoreDoc[0]
        );
        qsr.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, Float.NaN), new DocValueFormat[0]);
        qsr.aggregations(InternalAggregations.from(Collections.singletonList(agg)));
        return qsr;
    }

    private LongTerms.Bucket longBucket(long term, long docCount, InternalAggregations subs) {
        return new LongTerms.Bucket(term, docCount, subs, false, 0L, DocValueFormat.RAW);
    }

    private StringTerms.Bucket stringBucket(String term, long docCount, InternalAggregation sub) {
        return new StringTerms.Bucket(
            new BytesRef(term),
            docCount,
            InternalAggregations.from(Collections.singletonList(sub)),
            false,
            0L,
            DocValueFormat.RAW
        );
    }

    private InternalAggregations metrics(double max, double min, double sum, double avgSum, long count, long cardBase) {
        return InternalAggregations.from(
            Arrays.asList(
                new InternalMax("mx", max, DocValueFormat.RAW, Collections.emptyMap()),
                new InternalMin("mn", min, DocValueFormat.RAW, Collections.emptyMap()),
                new InternalSum("sm", sum, DocValueFormat.RAW, Collections.emptyMap()),
                new InternalAvg("av", avgSum, count, DocValueFormat.RAW, Collections.emptyMap()),
                new InternalValueCount("vc", count, Collections.emptyMap()),
                card(cardBase, 15)
            )
        );
    }

    private InternalCardinality card(long base, int n) {
        HyperLogLogPlusPlus hll = new HyperLogLogPlusPlus(14, BigArrays.NON_RECYCLING_INSTANCE, 1);
        for (long i = 0; i < n; i++) {
            hll.collect(0, org.opensearch.common.util.BitMixer.mix64(base + i));
        }
        return org.opensearch.search.aggregations.metrics.ColumnarMetricCodec.buildCardinality("cd", hll, Collections.emptyMap());
    }

    private ColumnarTermsShardResult longCarrier(
        long[] keys,
        long[] docCounts,
        long otherDocCount,
        double[] max,
        double[] min,
        double[] sum,
        double[] avgSum,
        long[] count,
        long[] cardBase
    ) throws Exception {
        List<ColumnarTermsShardResult.MetricColumn> cols = new ArrayList<>();
        cols.add(ColumnarTermsShardResult.MetricColumn.scalar("mx", ColumnarMetricSink.Kind.MAX, max));
        cols.add(ColumnarTermsShardResult.MetricColumn.scalar("mn", ColumnarMetricSink.Kind.MIN, min));
        cols.add(ColumnarTermsShardResult.MetricColumn.scalar("sm", ColumnarMetricSink.Kind.SUM, sum));
        cols.add(ColumnarTermsShardResult.MetricColumn.avg("av", avgSum, count));
        cols.add(ColumnarTermsShardResult.MetricColumn.valueCount("vc", count));
        org.opensearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus[] hll =
            new org.opensearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus[keys.length];
        for (int i = 0; i < keys.length; i++) {
            hll[i] = buildSketch(cardBase[i], 15);
        }
        cols.add(ColumnarTermsShardResult.MetricColumn.cardinality("cd", hll));
        return new ColumnarTermsShardResult(
            "t",
            Collections.emptyMap(),
            BucketOrder.key(true),
            BucketOrder.count(false),
            10,
            1L,
            DocValueFormat.RAW,
            1000,
            false,
            otherDocCount,
            0L,
            keys,
            null,
            docCounts,
            null,
            cols,
            keys.length
        );
    }

    private org.opensearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus buildSketch(long base, int n) {
        HyperLogLogPlusPlus hll = new HyperLogLogPlusPlus(14, BigArrays.NON_RECYCLING_INSTANCE, 1);
        for (long i = 0; i < n; i++) {
            hll.collect(0, org.opensearch.common.util.BitMixer.mix64(base + i));
        }
        return hll;
    }
}
