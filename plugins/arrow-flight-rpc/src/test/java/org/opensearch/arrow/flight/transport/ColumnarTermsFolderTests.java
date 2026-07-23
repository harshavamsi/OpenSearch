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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregation.ReduceContext;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StreamingTermsReducer;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Equivalence tests: the coordinator-side {@link ColumnarTermsFolder} must produce the same final
 * {@link org.opensearch.search.aggregations.bucket.terms.InternalTerms} as the object path
 * ({@link ColumnarAggReader} + {@link StreamingTermsReducer}) when fed the same Arrow batches
 * produced by {@link ColumnarAggWriter}, across LONG/STRING keys and the six metric sub-aggs,
 * plus displacement / otherDocCount.
 */
public class ColumnarTermsFolderTests extends OpenSearchTestCase {

    private RootAllocator allocator;
    private MockBigArrays bigArrays;
    private NamedWriteableRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator(Long.MAX_VALUE);
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
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

    public void testLongTermsNoMetricsEquivalence() throws Exception {
        List<LongTerms> shards = Arrays.asList(
            longTerms(longBucket(1, 100), longBucket(2, 50), longBucket(3, 10)),
            longTerms(longBucket(2, 40), longBucket(3, 30), longBucket(4, 5))
        );
        assertEquivalent(shards, 10);
    }

    public void testStringTermsAllMetricsEquivalence() throws Exception {
        List<StringTerms> shards = Arrays.asList(
            stringTerms(
                stringBucket("a", 100, allMetrics(3.0, -1.0, 12.0, 24.0, 6, 111)),
                stringBucket("b", 50, allMetrics(9.0, 2.0, 8.0, 16.0, 4, 222))
            ),
            stringTerms(
                stringBucket("a", 40, allMetrics(7.0, -3.0, 5.0, 10.0, 2, 111)),
                stringBucket("c", 25, allMetrics(1.0, 1.0, 1.0, 1.0, 1, 333))
            )
        );
        assertEquivalent(shards, 10);
    }

    public void testDisplacementAndOtherDocCountEquivalence() throws Exception {
        // topN = 2 forces displacement; both paths must evict the same survivors and accrue the
        // same otherDocCount.
        List<LongTerms> shards = Arrays.asList(
            longTerms(longBucket(1, 100), longBucket(2, 90), longBucket(3, 5)),
            longTerms(longBucket(3, 80), longBucket(4, 70), longBucket(2, 1))
        );
        assertEquivalent(shards, 2);
    }

    public void testLongTermsCardinalityEquivalence() throws Exception {
        List<LongTerms> shards = Arrays.asList(
            longTerms(longBucket(1, 100, card(1000, 0, 40)), longBucket(2, 50, card(1000, 40, 20))),
            longTerms(longBucket(1, 60, card(1000, 20, 40)), longBucket(2, 30, card(1000, 50, 10)))
        );
        assertEquivalent(shards, 10);
    }

    /**
     * Heavy displacement: many terms across many shards through a small topN. Exercises the
     * indexed min-heap (insert/merge-sift-down/evict-root) at scale and confirms the fold still
     * matches the object path exactly — the heap must pick the same displacement victims a linear
     * min-scan would.
     */
    public void testHeavyDisplacementEquivalence() throws Exception {
        java.util.Random rnd = new java.util.Random(42);
        int topN = 50;
        int termUniverse = 400;
        List<LongTerms> shards = new java.util.ArrayList<>();
        for (int s = 0; s < 8; s++) {
            List<LongTerms.Bucket> buckets = new java.util.ArrayList<>();
            // A shifting subset of terms per shard so keys both overlap (merge) and differ (displace).
            for (int t = 0; t < 120; t++) {
                long term = rnd.nextInt(termUniverse);
                long dc = 1 + rnd.nextInt(1000);
                buckets.add(longBucket(term, dc));
            }
            // dedup keys within a shard (a shard emits each term once) keeping max dc
            java.util.Map<Long, Long> byKey = new java.util.LinkedHashMap<>();
            for (LongTerms.Bucket b : buckets) {
                byKey.merge((Long) b.getKey(), b.getDocCount(), Math::max);
            }
            List<LongTerms.Bucket> deduped = new java.util.ArrayList<>();
            for (var e : byKey.entrySet()) {
                deduped.add(longBucket(e.getKey(), e.getValue()));
            }
            shards.add(longTerms(deduped.toArray(new LongTerms.Bucket[0])));
        }
        assertEquivalent(shards, topN);
    }

    // ---- equivalence driver ----

    private <T extends InternalAggregation> void assertEquivalent(List<T> shards, int topN) throws Exception {
        // Object path: writer -> reader -> StreamingTermsReducer.
        InternalAggregation objectResult = runObjectPath(shards, topN);
        // Fold path: writer -> ColumnarTermsFolder.
        InternalAggregation foldResult = runFoldPath(shards, topN);

        assertEquals("class", objectResult.getClass(), foldResult.getClass());
        assertEquals("string form", normalize(objectResult), normalize(foldResult));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private InternalAggregation runObjectPath(List<? extends InternalAggregation> shards, int topN) throws Exception {
        StreamingTermsReducer reducer = new StreamingTermsReducer<>(topN, partialContext());
        for (InternalAggregation shard : shards) {
            AggColumnarPlan plan = AggColumnarPlan.detect(wrap(shard)).orElseThrow();
            try (ColumnarAggWriter writer = new ColumnarAggWriter(plan, allocator)) {
                writer.write(wrap(shard));
                VectorSchemaRoot root = writer.getRoot();
                AggColumnarPlan planFromSchema = ColumnarPlanFromSchema.build(root.getSchema());
                ColumnarAggReader reader = new ColumnarAggReader(planFromSchema, registry);
                QuerySearchResult qsr = reader.read(root);
                InternalAggregations aggs = qsr.aggregations().expand();
                reducer.accept((org.opensearch.search.aggregations.bucket.terms.InternalTerms) aggs.asList().get(0));
            }
        }
        return reducer.finalize(finalContext());
    }

    private InternalAggregation runFoldPath(List<? extends InternalAggregation> shards, int topN) throws Exception {
        ColumnarTermsFolder folder = null;
        for (InternalAggregation shard : shards) {
            AggColumnarPlan plan = AggColumnarPlan.detect(wrap(shard)).orElseThrow();
            try (ColumnarAggWriter writer = new ColumnarAggWriter(plan, allocator)) {
                writer.write(wrap(shard));
                VectorSchemaRoot root = writer.getRoot();
                AggColumnarPlan planFromSchema = ColumnarPlanFromSchema.build(root.getSchema());
                if (folder == null) {
                    folder = new ColumnarTermsFolder(planFromSchema, topN);
                }
                org.opensearch.search.aggregations.bucket.terms.ColumnarTermsCodec.TermsHeader header = readHeader(root);
                int bucketCount = AggColumnarSchema.bigInt(root, AggColumnarSchema.DOC_COUNT).getValueCount();
                folder.fold(root, header, bucketCount);
            }
        }
        InternalAggregation out = folder.finalizeAggregation(finalContext());
        folder.release();
        return out;
    }

    private org.opensearch.search.aggregations.bucket.terms.ColumnarTermsCodec.TermsHeader readHeader(VectorSchemaRoot root)
        throws Exception {
        byte[] headerBytes = AggColumnarSchema.varBinary(root, AggColumnarSchema.HEADER).get(0);
        try (
            org.opensearch.core.common.io.stream.StreamInput raw = new org.opensearch.core.common.io.stream.BytesStreamInput(headerBytes);
            org.opensearch.core.common.io.stream.StreamInput in = new org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput(
                raw,
                registry
            )
        ) {
            new QuerySearchResult(in);
            return org.opensearch.search.aggregations.bucket.terms.ColumnarTermsCodec.readTermsHeader(in);
        }
    }

    /**
     * Canonical string form for comparison: buckets sorted by key so map-order differences in the
     * two paths don't produce spurious mismatches, with each bucket's metrics rendered.
     */
    private String normalize(InternalAggregation agg) {
        List<? extends org.opensearch.search.aggregations.bucket.terms.Terms.Bucket> buckets =
            ((org.opensearch.search.aggregations.bucket.terms.Terms) agg).getBuckets();
        List<String> lines = new ArrayList<>();
        for (org.opensearch.search.aggregations.bucket.terms.Terms.Bucket b : buckets) {
            StringBuilder sb = new StringBuilder();
            sb.append("key=").append(b.getKeyAsString()).append(" dc=").append(b.getDocCount());
            List<String> metricLines = new ArrayList<>();
            for (Aggregation sub : b.getAggregations().asList()) {
                metricLines.add(renderMetric(sub));
            }
            Collections.sort(metricLines);
            sb.append(" ").append(metricLines);
            lines.add(sb.toString());
        }
        Collections.sort(lines);
        long other = ((org.opensearch.search.aggregations.bucket.terms.InternalTerms<?, ?>) agg).getSumOfOtherDocCounts();
        return "other=" + other + " " + lines;
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

    // ---- builders ----

    private LongTerms longTerms(LongTerms.Bucket... buckets) {
        return new LongTerms(
            "t",
            BucketOrder.count(false),
            BucketOrder.count(false),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            1000,
            false,
            0L,
            Arrays.asList(buckets),
            0L,
            new TermsAggregator.BucketCountThresholds(1, 0, 1000, 1000)
        );
    }

    private StringTerms stringTerms(StringTerms.Bucket... buckets) {
        return new StringTerms(
            "t",
            BucketOrder.count(false),
            BucketOrder.count(false),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            1000,
            false,
            0L,
            Arrays.asList(buckets),
            0L,
            new TermsAggregator.BucketCountThresholds(1, 0, 1000, 1000)
        );
    }

    private LongTerms.Bucket longBucket(long term, long docCount, InternalAggregation... subs) {
        return new LongTerms.Bucket(term, docCount, subAggs(subs), false, 0L, DocValueFormat.RAW);
    }

    private StringTerms.Bucket stringBucket(String term, long docCount, InternalAggregation... subs) {
        return new StringTerms.Bucket(new BytesRef(term), docCount, subAggs(subs), false, 0L, DocValueFormat.RAW);
    }

    private InternalAggregations subAggs(InternalAggregation... subs) {
        if (subs.length == 0) {
            return InternalAggregations.EMPTY;
        }
        return InternalAggregations.from(Arrays.asList(subs));
    }

    private InternalAggregation[] allMetrics(double max, double min, double sum, double avgSum, long count, long cardBase) {
        return new InternalAggregation[] {
            new InternalMax("mx", max, DocValueFormat.RAW, Collections.emptyMap()),
            new InternalMin("mn", min, DocValueFormat.RAW, Collections.emptyMap()),
            new InternalSum("sm", sum, DocValueFormat.RAW, Collections.emptyMap()),
            new InternalAvg("av", avgSum, count, DocValueFormat.RAW, Collections.emptyMap()),
            new InternalValueCount("vc", count, Collections.emptyMap()),
            card(1000, cardBase, 15) };
    }

    /** Build an InternalCardinality with {@code n} distinct hashes starting at {@code base}. */
    private InternalCardinality card(int precisionSeed, long base, int n) {
        HyperLogLogPlusPlus hll = new HyperLogLogPlusPlus(14, BigArrays.NON_RECYCLING_INSTANCE, 1);
        for (long i = 0; i < n; i++) {
            hll.collect(0, org.opensearch.common.util.BitMixer.mix64(base + i));
        }
        return org.opensearch.search.aggregations.metrics.ColumnarMetricCodec.buildCardinality("cd", hll, Collections.emptyMap());
    }

    private ReduceContext partialContext() {
        return InternalAggregation.ReduceContext.forPartialReduction(bigArrays, null, () -> null);
    }

    private ReduceContext finalContext() {
        return InternalAggregation.ReduceContext.forFinalReduction(bigArrays, null, b -> {}, PipelineAggregator.PipelineTree.EMPTY);
    }
}
