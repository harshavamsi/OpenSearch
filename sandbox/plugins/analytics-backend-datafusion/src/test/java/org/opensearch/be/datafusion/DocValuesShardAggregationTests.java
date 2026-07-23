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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
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
import org.opensearch.analytics.spi.ShardAggregationEngine;
import org.opensearch.analytics.spi.ShardAggregationEngine.AggCall;
import org.opensearch.analytics.spi.ShardAggregationEngine.AggFunction;
import org.opensearch.analytics.spi.ShardAggregationEngine.AggSpec;
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
 * End-to-end proof of the doc_values analytics path: a real Lucene index's doc_values are
 * decoded into Arrow batches by {@link DocValuesAggregationExecutor} (direct MemorySegment
 * decode) and aggregated by the DataFusion-backed {@link ShardAggregationEngine} installed
 * through the framework holder — the exact seam the Lucene analytics backend uses when the
 * planner routes a group-by fragment to a lucene-format shard.
 *
 * <p>Runs in this module because the test classpath is flat (analytics-backend-lucene is a
 * testImplementation dep), mirroring the runtime arrangement where the holder bridges the
 * two sibling plugins.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class DocValuesShardAggregationTests extends OpenSearchTestCase {

    private static final int ROWS = 1_000_000;
    private static final int KEY_CARD = 5000;
    private static final int METRIC_CARD = 65536;

    private static long key(int i) {
        return i < 2 ? i : (i * 2654435761L >>> 13) % KEY_CARD;
    }

    private static long metric(int i) {
        return i < 2 ? i : (i * 6364136223846793005L >>> 17) % METRIC_CARD;
    }

    public void testGroupByCountSumViaEngineSeam() throws Exception {
        // ---- Reference ----
        Map<Long, long[]> reference = new HashMap<>(); // key -> [count, sum]
        for (int i = 0; i < ROWS; i++) {
            long[] agg = reference.computeIfAbsent(key(i), k -> new long[2]);
            agg[0]++;
            agg[1] += metric(i);
        }

        // ---- Index: two numeric DV columns, merged segment, mmap, real codec ----
        Path indexDir = createTempDir("dv-agg");
        try (MMapDirectory dir = new MMapDirectory(indexDir)) {
            IndexWriterConfig iwc = new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec()).setRAMBufferSizeMB(256);
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                Document doc = new Document();
                NumericDocValuesField fKey = new NumericDocValuesField("key", 0);
                NumericDocValuesField fMetric = new NumericDocValuesField("metric", 0);
                LongPoint pKey = new LongPoint("key", 0);
                doc.add(fKey);
                doc.add(fMetric);
                doc.add(pKey);
                for (int i = 0; i < ROWS; i++) {
                    fKey.setLongValue(key(i));
                    fMetric.setLongValue(metric(i));
                    pKey.setLongValue(key(i));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }

            // ---- Native runtime + engine install (what DataFusionPlugin.createComponents does) ----
            NativeBridge.initTokioRuntimeManager(2);
            Path spillDir = createTempDir("dv-agg-spill");
            long runtimePtr = NativeBridge.createGlobalRuntime(512L * 1024 * 1024, 0L, spillDir.toString(), 256L * 1024 * 1024);
            assertTrue(runtimePtr != 0);
            NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);
            try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE); DirectoryReader reader = DirectoryReader.open(dir)) {
                ShardAggregationEngineHolder.install(new TestEngineFactory(runtimeHandle).create());

                // ---- Execute through the seam: match_all, GROUP BY key + COUNT + SUM(metric) ----
                DocValuesAggregationExecutor executor = new DocValuesAggregationExecutor();
                AggSpec spec = new AggSpec(
                    List.of("key", "metric"),
                    List.of("key"),
                    List.of(new AggCall(AggFunction.COUNT, null, "cnt"), new AggCall(AggFunction.SUM, "metric", "total"))
                );
                IndexSearcher searcher = new IndexSearcher(reader);
                Map<Long, long[]> got = new HashMap<>();
                EngineResultStream stream = executor.execute(searcher, new MatchAllDocsQuery(), spec, alloc, 0L);
                try {
                    Iterator<EngineResultBatch> it = stream.iterator();
                    while (it.hasNext()) {
                        EngineResultBatch batch = it.next();
                        // Batches returned by next() are caller-owned (Flight closes them after
                        // transfer on the production path) — close after reading.
                        try (VectorSchemaRoot root = batch.getArrowRoot()) {
                            BigIntVector keys = (BigIntVector) root.getVector(0);
                            BigIntVector cnts = (BigIntVector) root.getVector(1);
                            BigIntVector sums = (BigIntVector) root.getVector(2);
                            for (int i = 0; i < root.getRowCount(); i++) {
                                long[] agg = got.computeIfAbsent(keys.get(i), k -> new long[2]);
                                agg[0] += cnts.get(i);
                                agg[1] += sums.get(i);
                            }
                        }
                    }
                } finally {
                    stream.close();
                }

                // ---- Assert ----
                assertEquals("group count", reference.size(), got.size());
                for (Map.Entry<Long, long[]> e : reference.entrySet()) {
                    long[] expected = e.getValue();
                    long[] actual = got.get(e.getKey());
                    assertNotNull("missing key " + e.getKey(), actual);
                    assertEquals("count for key " + e.getKey(), expected[0], actual[0]);
                    assertEquals("sum for key " + e.getKey(), expected[1], actual[1]);
                }
                assertTrue(
                    "direct MemorySegment decode must engage (direct=" + executor.directBatches() + ")",
                    executor.directBatches() > 0
                );
                assertEquals("no fallback expected on this fixture", 0, executor.fallbackBatches());
                System.out.printf(
                    "[dv-agg] %d rows -> %d groups; directBatches=%d fallbackBatches=%d%n",
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
     * Builds the production engine implementation against a test-owned runtime.
     * {@code DatafusionShardAggregationEngine} is package-private and its constructor wants a
     * {@code DataFusionService}; tests own a bare runtime handle instead, so this mirrors its
     * open() using the same native calls. Kept minimal and in-sync with the production class.
     */
    private record TestEngineFactory(NativeRuntimeHandle runtimeHandle) {
        ShardAggregationEngine create() throws Exception {
            java.lang.reflect.Constructor<?> ctor = Class.forName("org.opensearch.be.datafusion.DatafusionShardAggregationEngine")
                .getDeclaredConstructor(DataFusionService.class);
            ctor.setAccessible(true);
            DataFusionService service = org.mockito.Mockito.mock(DataFusionService.class);
            org.mockito.Mockito.when(service.getNativeRuntime()).thenReturn(runtimeHandle);
            return (ShardAggregationEngine) ctor.newInstance(service);
        }
    }
}
