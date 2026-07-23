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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.ShardAggregationEngine;
import org.opensearch.analytics.spi.ShardAggregationEngineHolder;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.lucene.DocValuesAggregationExecutor;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * End-to-end proof of the wire-v3 plan path: an aggregate shape the structured
 * {@code AggSpec} CANNOT express — {@code COUNT(DISTINCT metric) GROUP BY key} — compiled
 * by {@link DatafusionShardAggregationEngine#compileFragment} from a rebased Calcite
 * fragment (stage-input scan leaf, the exact rebase output shape
 * {@code LuceneFragmentConvertor.extractGeneralDvShape} produces) and executed by
 * {@link DocValuesAggregationExecutor}'s plan-bytes entry over a real Lucene index.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class DvPlanFragmentTests extends OpenSearchTestCase {

    private static final int ROWS = 500_000;
    private static final int KEY_CARD = 1000;
    private static final int METRIC_CARD = 5000;

    private static long key(int i) {
        return (i * 2654435761L >>> 15) % KEY_CARD;
    }

    private static long metric(int i) {
        return (i * 6364136223846793005L >>> 17) % METRIC_CARD;
    }

    public void testCountDistinctViaPlanBytes() throws Exception {
        // ---- Reference: per-key distinct metric counts ----
        Map<Long, Set<Long>> distinct = new HashMap<>();
        for (int i = 0; i < ROWS; i++) {
            distinct.computeIfAbsent(key(i), k -> new HashSet<>()).add(metric(i));
        }

        Path indexDir = createTempDir("dv-plan");
        try (MMapDirectory dir = new MMapDirectory(indexDir)) {
            IndexWriterConfig iwc = new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec()).setRAMBufferSizeMB(256);
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                Document doc = new Document();
                NumericDocValuesField fKey = new NumericDocValuesField("key", 0);
                NumericDocValuesField fMetric = new NumericDocValuesField("metric", 0);
                doc.add(fKey);
                doc.add(fMetric);
                for (int i = 0; i < ROWS; i++) {
                    fKey.setLongValue(key(i));
                    fMetric.setLongValue(metric(i));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }

            NativeBridge.initTokioRuntimeManager(2);
            Path spillDir = createTempDir("dv-plan-spill");
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

                // ---- Build the rebased fragment: COUNT(DISTINCT metric) GROUP BY key over a
                // stage-input scan — the exact shape extractGeneralDvShape emits ----
                byte[] planBytes = engine.compileFragment(buildCountDistinctFragment());

                DocValuesAggregationExecutor executor = new DocValuesAggregationExecutor();
                Map<Long, Long> got = new HashMap<>();
                EngineResultStream stream = executor.execute(
                    new IndexSearcher(reader),
                    new MatchAllDocsQuery(),
                    planBytes,
                    List.of(
                        new ShardAggregationEngine.InputColumn("key", ShardAggregationEngine.ColumnKind.LONG),
                        new ShardAggregationEngine.InputColumn("metric", ShardAggregationEngine.ColumnKind.LONG)
                    ),
                    alloc,
                    0L
                );
                try {
                    Iterator<EngineResultBatch> it = stream.iterator();
                    while (it.hasNext()) {
                        try (VectorSchemaRoot root = it.next().getArrowRoot()) {
                            BigIntVector keys = (BigIntVector) root.getVector(0);
                            BigIntVector counts = (BigIntVector) root.getVector(1);
                            for (int i = 0; i < root.getRowCount(); i++) {
                                got.merge(keys.get(i), counts.get(i), Long::sum);
                            }
                        }
                    }
                } finally {
                    stream.close();
                }

                assertEquals("group count", distinct.size(), got.size());
                for (Map.Entry<Long, Set<Long>> e : distinct.entrySet()) {
                    assertEquals("distinct count for key " + e.getKey(), Long.valueOf(e.getValue().size()), got.get(e.getKey()));
                }
                System.out.printf(
                    "[dv-plan] COUNT(DISTINCT) over %d rows -> %d groups OK; direct=%d fallback=%d%n",
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

    /** {@code SELECT key, COUNT(DISTINCT metric) FROM <stage-input key,metric> GROUP BY key}. */
    private static RelNode buildCountDistinctFragment() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelOptCluster cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), rexBuilder);
        RelDataType bigint = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        RelDataType rowType = typeFactory.builder().add("key", bigint).add("metric", bigint).build();
        OpenSearchStageInputScan scan = new OpenSearchStageInputScan(cluster, cluster.traitSet(), 0, rowType, List.of(), List.of());
        AggregateCall distinctCount = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            true,
            List.of(1),
            -1,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "dc"
        );
        return LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(distinctCount));
    }
}
