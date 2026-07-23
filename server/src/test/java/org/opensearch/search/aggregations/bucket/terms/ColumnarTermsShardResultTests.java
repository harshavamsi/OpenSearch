/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.ColumnarMetricSink;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Regression tests for the non-Flight fallback of {@link ColumnarTermsShardResult}. The Flight
 * columnar writer intercepts the carrier by type, but same-node shards and non-Flight transport
 * serialize / reduce the aggregation directly — which must materialize the equivalent
 * {@link InternalMappedTerms} rather than throw. This path was missed by the writer-level
 * equivalence tests and broke config G (coordinator-local shards tripped an emit-only guard).
 */
public class ColumnarTermsShardResultTests extends OpenSearchTestCase {

    private MockBigArrays bigArrays;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    /** The exact path that broke G: serialize the carrier, read it back as a real terms agg. */
    public void testSerializeRoundTripMaterializesTerms() throws Exception {
        ColumnarTermsShardResult carrier = longCarrierWithMaxSum();
        NamedWriteableRegistry registry = new NamedWriteableRegistry(
            Arrays.asList(
                new NamedWriteableRegistry.Entry(InternalAggregation.class, LongTerms.NAME, LongTerms::new),
                new NamedWriteableRegistry.Entry(InternalAggregation.class, MaxAggregationBuilder.NAME, InternalMax::new),
                new NamedWriteableRegistry.Entry(InternalAggregation.class, SumAggregationBuilder.NAME, InternalSum::new),
                new NamedWriteableRegistry.Entry(DocValueFormat.class, DocValueFormat.RAW.getWriteableName(), in -> DocValueFormat.RAW)
            )
        );

        InternalAggregation readBack;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeNamedWriteable(carrier);
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                readBack = in.readNamedWriteable(InternalAggregation.class);
            }
        }

        assertTrue("carrier must deserialize as LongTerms", readBack instanceof LongTerms);
        LongTerms terms = (LongTerms) readBack;
        assertEquals(2, terms.getBuckets().size());
        LongTerms.Bucket b0 = terms.getBuckets().get(0);
        assertEquals(10L, b0.getKeyAsNumber().longValue());
        assertEquals(100L, b0.getDocCount());
        assertEquals(3.0, ((InternalMax) b0.getAggregations().get("mx")).getValue(), 0.0);
        assertEquals(12.0, ((InternalSum) b0.getAggregations().get("sm")).getValue(), 0.0);
    }

    /** In-process reduce (same-node shard path) must fold carriers like real terms. */
    public void testReduceMaterializesAndMerges() {
        ColumnarTermsShardResult a = longCarrierWithMaxSum();
        ColumnarTermsShardResult b = longCarrierWithMaxSum();
        InternalAggregation.ReduceContext ctx = InternalAggregation.ReduceContext.forFinalReduction(
            bigArrays,
            null,
            x -> {},
            PipelineAggregator.PipelineTree.EMPTY
        );
        InternalAggregation reduced = a.reduce(Arrays.asList(a, b), ctx);
        assertTrue(reduced instanceof LongTerms);
        LongTerms terms = (LongTerms) reduced;
        // Two identical shards → doc counts double, max stays, sum doubles.
        LongTerms.Bucket top = terms.getBuckets().get(0);
        assertEquals(200L, top.getDocCount());
        assertEquals(3.0, ((InternalMax) top.getAggregations().get("mx")).getValue(), 0.0);
        assertEquals(24.0, ((InternalSum) top.getAggregations().get("sm")).getValue(), 0.0);
    }

    private ColumnarTermsShardResult longCarrierWithMaxSum() {
        List<ColumnarTermsShardResult.MetricColumn> cols = Arrays.asList(
            ColumnarTermsShardResult.MetricColumn.scalar("mx", ColumnarMetricSink.Kind.MAX, new double[] { 3.0, 9.0 }),
            ColumnarTermsShardResult.MetricColumn.scalar("sm", ColumnarMetricSink.Kind.SUM, new double[] { 12.0, 8.0 })
        );
        return new ColumnarTermsShardResult(
            "t",
            Collections.emptyMap(),
            BucketOrder.count(false),
            BucketOrder.count(false),
            10,
            1L,
            DocValueFormat.RAW,
            1000,
            false,
            7L,
            0L,
            new long[] { 10, 20 },
            null,
            new long[] { 100, 50 },
            null,
            cols,
            2
        );
    }
}
