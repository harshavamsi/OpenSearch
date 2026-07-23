/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.ShardAggregationEngine;
import org.opensearch.analytics.spi.ShardAggregationEngine.ColumnKind;
import org.opensearch.analytics.spi.ShardAggregationEngine.InputColumn;
import org.opensearch.analytics.spi.ShardAggregationEngineHolder;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.lucene.DocValuesAggregationExecutor;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Keyword group-by through the doc_values engine path: SortedDocValues ordinals bulk-decoded
 * ({@code ordValues}) and materialized to a Utf8 column, aggregated by a compiled
 * {@code SUM(metric) GROUP BY term} plan — the shape backing ClickBench's string queries
 * (terms on SearchPhrase/URL/Title with metric sub-aggs).
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class DvKeywordAggregationTests extends OpenSearchTestCase {

    private static final int ROWS = 500_000;
    private static final int TERM_CARD = 2000;
    private static final int METRIC_CARD = 10_000;

    private static String term(int i) {
        return "term-" + ((i * 2654435761L >>> 14) % TERM_CARD);
    }

    private static long metric(int i) {
        return (i * 6364136223846793005L >>> 17) % METRIC_CARD;
    }

    public void testKeywordGroupBySum() throws Exception {
        Map<String, long[]> reference = new HashMap<>(); // term -> [count, sum]
        for (int i = 0; i < ROWS; i++) {
            long[] agg = reference.computeIfAbsent(term(i), k -> new long[2]);
            agg[0]++;
            agg[1] += metric(i);
        }

        Path indexDir = createTempDir("dv-kw");
        try (MMapDirectory dir = new MMapDirectory(indexDir)) {
            IndexWriterConfig iwc = new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec()).setRAMBufferSizeMB(256);
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                Document doc = new Document();
                SortedDocValuesField fTerm = new SortedDocValuesField("phrase", new BytesRef(""));
                NumericDocValuesField fMetric = new NumericDocValuesField("metric", 0);
                doc.add(fTerm);
                doc.add(fMetric);
                for (int i = 0; i < ROWS; i++) {
                    fTerm.setBytesValue(new BytesRef(term(i)));
                    fMetric.setLongValue(metric(i));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }

            NativeBridge.initTokioRuntimeManager(2);
            Path spillDir = createTempDir("dv-kw-spill");
            long runtimePtr = NativeBridge.createGlobalRuntime(512L * 1024 * 1024, 0L, spillDir.toString(), 256L * 1024 * 1024);
            assertTrue(runtimePtr != 0);
            NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);
            try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE); DirectoryReader reader = DirectoryReader.open(dir)) {
                DataFusionService service = org.mockito.Mockito.mock(DataFusionService.class);
                org.mockito.Mockito.when(service.getNativeRuntime()).thenReturn(runtimeHandle);
                java.lang.reflect.Constructor<?> ctor = Class.forName("org.opensearch.be.datafusion.DatafusionShardAggregationEngine")
                    .getDeclaredConstructor(DataFusionService.class);
                ctor.setAccessible(true);
                ShardAggregationEngine engine = (ShardAggregationEngine) ctor.newInstance(service);
                ShardAggregationEngineHolder.install(engine);

                byte[] planBytes = engine.compileFragment(buildKeywordAggFragment());

                DocValuesAggregationExecutor executor = new DocValuesAggregationExecutor();
                Map<String, long[]> got = new HashMap<>();
                EngineResultStream stream = executor.execute(
                    new IndexSearcher(reader),
                    new MatchAllDocsQuery(),
                    planBytes,
                    List.of(new InputColumn("phrase", ColumnKind.KEYWORD), new InputColumn("metric", ColumnKind.LONG)),
                    alloc,
                    0L
                );
                try {
                    Iterator<EngineResultBatch> it = stream.iterator();
                    while (it.hasNext()) {
                        try (VectorSchemaRoot root = it.next().getArrowRoot()) {
                            FieldVector terms = root.getVector(0);
                            BigIntVector counts = (BigIntVector) root.getVector(1);
                            BigIntVector sums = (BigIntVector) root.getVector(2);
                            for (int i = 0; i < root.getRowCount(); i++) {
                                String key = String.valueOf(terms.getObject(i));
                                long[] agg = got.computeIfAbsent(key, k -> new long[2]);
                                agg[0] += counts.get(i);
                                agg[1] += sums.get(i);
                            }
                        }
                    }
                } finally {
                    stream.close();
                }

                assertEquals("group count", reference.size(), got.size());
                for (Map.Entry<String, long[]> e : reference.entrySet()) {
                    long[] expected = e.getValue();
                    long[] actual = got.get(e.getKey());
                    assertNotNull("missing term " + e.getKey(), actual);
                    assertEquals("count for " + e.getKey(), expected[0], actual[0]);
                    assertEquals("sum for " + e.getKey(), expected[1], actual[1]);
                }
                System.out.printf(
                    "[dv-kw] keyword group-by over %d rows -> %d terms OK; direct=%d fallback=%d%n",
                    ROWS,
                    got.size(),
                    executor.directBatches(),
                    executor.fallbackBatches()
                );
            } finally {
                runtimeHandle.close();
            }
        }
    }

    /**
     * Ordinal-first path (v2 AggSpec entry, keyword group key probed from the index):
     * per-segment ordinal group-by + term materialization of RESULT groups + cross-segment
     * merge. Multi-segment index (no forceMerge) so the merge phase is actually exercised.
     */
    public void testKeywordGroupBySumOrdinalFirst() throws Exception {
        Map<String, long[]> reference = new HashMap<>();
        for (int i = 0; i < ROWS; i++) {
            long[] agg = reference.computeIfAbsent(term(i), k -> new long[2]);
            agg[0]++;
            agg[1] += metric(i);
        }

        Path indexDir = createTempDir("dv-kw-ord");
        try (MMapDirectory dir = new MMapDirectory(indexDir)) {
            IndexWriterConfig iwc = new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec())
                .setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                Document doc = new Document();
                SortedDocValuesField fTerm = new SortedDocValuesField("phrase", new BytesRef(""));
                NumericDocValuesField fMetric = new NumericDocValuesField("metric", 0);
                doc.add(fTerm);
                doc.add(fMetric);
                for (int i = 0; i < ROWS; i++) {
                    fTerm.setBytesValue(new BytesRef(term(i)));
                    fMetric.setLongValue(metric(i));
                    writer.addDocument(doc);
                    // Segment cut every 100k docs: per-segment ordinal spaces differ, so the
                    // phase-2 term merge is meaningfully exercised.
                    if (i % 100_000 == 99_999) {
                        writer.flush();
                    }
                }
            }

            NativeBridge.initTokioRuntimeManager(2);
            Path spillDir = createTempDir("dv-kw-ord-spill");
            long runtimePtr = NativeBridge.createGlobalRuntime(512L * 1024 * 1024, 0L, spillDir.toString(), 256L * 1024 * 1024);
            assertTrue(runtimePtr != 0);
            NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);
            try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE); DirectoryReader reader = DirectoryReader.open(dir)) {
                assertTrue("need multiple segments to exercise the merge", reader.leaves().size() > 1);
                DataFusionService service = org.mockito.Mockito.mock(DataFusionService.class);
                org.mockito.Mockito.when(service.getNativeRuntime()).thenReturn(runtimeHandle);
                java.lang.reflect.Constructor<?> ctor = Class.forName("org.opensearch.be.datafusion.DatafusionShardAggregationEngine")
                    .getDeclaredConstructor(DataFusionService.class);
                ctor.setAccessible(true);
                ShardAggregationEngineHolder.install((ShardAggregationEngine) ctor.newInstance(service));

                DocValuesAggregationExecutor executor = new DocValuesAggregationExecutor();
                ShardAggregationEngine.AggSpec spec = new ShardAggregationEngine.AggSpec(
                    List.of("phrase", "metric"),
                    List.of("phrase"),
                    List.of(
                        new ShardAggregationEngine.AggCall(ShardAggregationEngine.AggFunction.COUNT, null, "cnt"),
                        new ShardAggregationEngine.AggCall(ShardAggregationEngine.AggFunction.SUM, "metric", "total")
                    )
                );
                Map<String, long[]> got = new HashMap<>();
                EngineResultStream stream = executor.execute(new IndexSearcher(reader), new MatchAllDocsQuery(), spec, alloc, 0L);
                try {
                    Iterator<EngineResultBatch> it = stream.iterator();
                    while (it.hasNext()) {
                        try (VectorSchemaRoot root = it.next().getArrowRoot()) {
                            FieldVector terms = root.getVector(0);
                            BigIntVector counts = (BigIntVector) root.getVector(1);
                            BigIntVector sums = (BigIntVector) root.getVector(2);
                            for (int i = 0; i < root.getRowCount(); i++) {
                                String key = String.valueOf(terms.getObject(i));
                                long[] agg = got.computeIfAbsent(key, k -> new long[2]);
                                agg[0] += counts.get(i);
                                agg[1] += sums.get(i);
                            }
                        }
                    }
                } finally {
                    stream.close();
                }

                assertEquals("group count", reference.size(), got.size());
                for (Map.Entry<String, long[]> e : reference.entrySet()) {
                    long[] expected = e.getValue();
                    long[] actual = got.get(e.getKey());
                    assertNotNull("missing term " + e.getKey(), actual);
                    assertEquals("count for " + e.getKey(), expected[0], actual[0]);
                    assertEquals("sum for " + e.getKey(), expected[1], actual[1]);
                }
                System.out.printf(
                    "[dv-kw-ord] ordinal-first over %d rows / %d segments -> %d terms OK%n",
                    ROWS,
                    reader.leaves().size(),
                    got.size()
                );
            } finally {
                runtimeHandle.close();
            }
        }
    }

    /** {@code SELECT phrase, COUNT(*), SUM(metric) FROM <stage-input phrase,metric> GROUP BY phrase}. */
    private static RelNode buildKeywordAggFragment() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelOptCluster cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), rexBuilder);
        RelDataType varchar = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RelDataType bigint = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        RelDataType rowType = typeFactory.builder().add("phrase", varchar).add("metric", bigint).build();
        OpenSearchStageInputScan scan = new OpenSearchStageInputScan(cluster, cluster.traitSet(), 0, rowType, List.of(), List.of());
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        AggregateCall sum = AggregateCall.create(SqlStdOperatorTable.SUM, false, List.of(1), -1, bigint, "total");
        return LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(count, sum));
    }
}
