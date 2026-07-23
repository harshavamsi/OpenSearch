/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
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
import org.opensearch.analytics.spi.ShardAggregationEngine.AggCall;
import org.opensearch.analytics.spi.ShardAggregationEngine.AggFunction;
import org.opensearch.analytics.spi.ShardAggregationEngine.AggSpec;
import org.opensearch.analytics.spi.ShardAggregationEngineHolder;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.ReaderHandle;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.be.lucene.DocValuesAggregationExecutor;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.ReorganizingLongHash;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.exec.MonoFileWriterSet;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.arrow.c.Data.importField;

/**
 * PPL-shaped shard comparison through the PRODUCTION scan-provider classes: the exact code a
 * cluster data node runs when a PPL {@code stats ... by key} fragment lands on a shard.
 *
 * <ul>
 *   <li><b>Path P:</b> parquet shard scan — {@code ReaderHandle} + {@code executeQueryAsync}
 *       (what the DataFusion backend's fragment execution does).</li>
 *   <li><b>Path D:</b> {@link DocValuesAggregationExecutor} feeding the
 *       {@link DatafusionShardAggregationEngine} installed via the framework holder (what the
 *       Lucene backend's fragment execution does after this branch's planner integration).</li>
 * </ul>
 *
 * <p><b>Path J (three-way leg):</b> doc_values WITHOUT Arrow — heap {@code longValues} bulk
 * decode + {@link ReorganizingLongHash} group-by with parallel count/sum arrays: the shape of
 * this branch's streaming Java aggregator (classic OpenSearch tier). No Arrow, no DataFusion.
 *
 * <p>Not covered here (identical on both sides in a cluster): Flight transport, coordinator
 * final reduce, PPL parse/plan. Same 20M-row LCG fixture as
 * {@link DatafusionParquetVsDocValuesBenchTests}, checksummed against generator drift.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class PplPathParquetVsDocValuesBenchTests extends OpenSearchTestCase {

    private static final int ROWS = 20_000_000;
    private static final int WARMUP = 2;
    private static final int MEASURED = 5;

    private static final Path PARQUET_DIR = Path.of("/tmp/pvd-bench");
    private static final String PARQUET_FILE = "data.parquet";

    private static final long LCG_MUL = 6364136223846793005L;
    private static final long LCG_INC = 1442695040888963407L;
    private static final int KEY_LOW_CARD = 100;
    private static final int KEY_HIGH_CARD = 2_097_152;
    private static final int METRIC_CARD = 65_536;
    private static final long KEY_LOW_SUM = 990_001_029L;
    private static final long KEY_HIGH_SUM = 20_971_506_285_803L;
    private static final long METRIC_SUM = 655_349_845_155L;

    private static long lcg(long i) {
        return i * LCG_MUL + LCG_INC;
    }

    private static long keyLow(int i) {
        return i < 2 ? i : (lcg(i) >>> 33) % KEY_LOW_CARD;
    }

    private static long keyHigh(int i) {
        return i < 2 ? i : (lcg(i) >>> 20) % KEY_HIGH_CARD;
    }

    private static long metric(int i) {
        return i < 2 ? i : (lcg(i) >>> 10) % METRIC_CARD;
    }

    /** One PPL-shaped query: {@code stats <agg> by <key>} in both engines' native form. */
    private record QuerySpec(String name, String sql, AggSpec aggSpec, boolean sum, int groupCard) {
    }

    private static final QuerySpec Q1 = new QuerySpec(
        "stats count() by key_low",
        "SELECT key_low, COUNT(*) AS cnt FROM test_table GROUP BY key_low",
        new AggSpec(List.of("key_low"), List.of("key_low"), List.of(new AggCall(AggFunction.COUNT, null, "cnt"))),
        false,
        KEY_LOW_CARD
    );
    private static final QuerySpec Q2 = new QuerySpec(
        "stats count() by key_high",
        "SELECT key_high, COUNT(*) AS cnt FROM test_table GROUP BY key_high",
        new AggSpec(List.of("key_high"), List.of("key_high"), List.of(new AggCall(AggFunction.COUNT, null, "cnt"))),
        false,
        KEY_HIGH_CARD
    );
    private static final QuerySpec Q3 = new QuerySpec(
        "stats sum(metric) by key_low",
        "SELECT key_low, SUM(metric) AS total FROM test_table GROUP BY key_low",
        new AggSpec(List.of("key_low", "metric"), List.of("key_low"), List.of(new AggCall(AggFunction.SUM, "metric", "total"))),
        true,
        KEY_LOW_CARD
    );

    public void testPplShardPathParquetVsDocValues() throws Exception {
        assumeTrue(
            "parquet fixture present (generate with /tmp/pvd-bench/gen_parquet.py)",
            Files.exists(PARQUET_DIR.resolve(PARQUET_FILE))
        );

        // ---- References ----
        long[] refCntLow = new long[KEY_LOW_CARD];
        long[] refCntHigh = new long[KEY_HIGH_CARD];
        long[] refSumLow = new long[KEY_LOW_CARD];
        long sumLow = 0, sumHigh = 0, sumMetric = 0;
        for (int i = 0; i < ROWS; i++) {
            long kl = keyLow(i), kh = keyHigh(i), m = metric(i);
            refCntLow[(int) kl]++;
            refCntHigh[(int) kh]++;
            refSumLow[(int) kl] += m;
            sumLow += kl;
            sumHigh += kh;
            sumMetric += m;
        }
        assertEquals("generator drift: key_low", KEY_LOW_SUM, sumLow);
        assertEquals("generator drift: key_high", KEY_HIGH_SUM, sumHigh);
        assertEquals("generator drift: metric", METRIC_SUM, sumMetric);

        // ---- Lucene index ----
        Path indexDir = createTempDir("ppl-pvd-lucene");
        long tIndex = System.nanoTime();
        try (MMapDirectory dir = new MMapDirectory(indexDir)) {
            IndexWriterConfig iwc = new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec()).setRAMBufferSizeMB(512);
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                Document doc = new Document();
                NumericDocValuesField fLow = new NumericDocValuesField("key_low", 0);
                NumericDocValuesField fHigh = new NumericDocValuesField("key_high", 0);
                NumericDocValuesField fMetric = new NumericDocValuesField("metric", 0);
                doc.add(fLow);
                doc.add(fHigh);
                doc.add(fMetric);
                for (int i = 0; i < ROWS; i++) {
                    fLow.setLongValue(keyLow(i));
                    fHigh.setLongValue(keyHigh(i));
                    fMetric.setLongValue(metric(i));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
        }
        System.out.printf(
            Locale.ROOT,
            "[setup] indexed %d docs in %ds%n",
            ROWS,
            TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - tIndex)
        );

        // ---- Runtime + engine (mirrors DataFusionPlugin.createComponents) ----
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("ppl-pvd-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(2048L * 1024 * 1024, 0L, spillDir.toString(), 512L * 1024 * 1024);
        assertTrue(runtimePtr != 0);
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);

        long tieredStorePtr = NativeStoreTestHelper.createTieredObjectStore(0L, 0L);
        NativeStoreHandle storeHandle = new NativeStoreHandle(
            NativeStoreTestHelper.getObjectStoreBoxPtr(tieredStorePtr),
            NativeStoreTestHelper::destroyObjectStoreBoxPtr
        );
        ReaderHandle readerHandle = new ReaderHandle(
            PARQUET_DIR.toString(),
            List.of(MonoFileWriterSet.of(".", 0L, PARQUET_FILE, ROWS)),
            storeHandle,
            List.of(),
            List.of()
        );

        StringBuilder table = new StringBuilder();
        table.append(
            String.format(
                Locale.ROOT,
                "%n=== PPL shard path, production classes: %d rows, %d warmup + %d measured, median ms ===%n",
                ROWS,
                WARMUP,
                MEASURED
            )
        );
        table.append(String.format(Locale.ROOT, "%-28s %12s %16s %16s%n", "query", "parquet", "dv+arrow(prov)", "dv-java(no arrow)"));

        try (
            RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
            Arena configArena = Arena.ofConfined();
            MMapDirectory dir = new MMapDirectory(indexDir);
            DirectoryReader reader = DirectoryReader.open(dir)
        ) {
            // Install the production engine exactly as DataFusionPlugin does.
            DataFusionService service = org.mockito.Mockito.mock(DataFusionService.class);
            org.mockito.Mockito.when(service.getNativeRuntime()).thenReturn(runtimeHandle);
            java.lang.reflect.Constructor<?> ctor = Class.forName("org.opensearch.be.datafusion.DatafusionShardAggregationEngine")
                .getDeclaredConstructor(DataFusionService.class);
            ctor.setAccessible(true);
            ShardAggregationEngineHolder.install((org.opensearch.analytics.spi.ShardAggregationEngine) ctor.newInstance(service));

            IndexSearcher searcher = new IndexSearcher(reader);

            // Mustang as-deployed: WireConfigSnapshot defaults (target_partitions=4).
            MemorySegment config = configArena.allocate(WireConfigSnapshot.BYTE_SIZE);
            WireConfigSnapshot.builder().build().writeTo(config);

            long totalDirect = 0, totalFallback = 0;
            for (QuerySpec q : List.of(Q1, Q2, Q3)) {
                long[] refCnt = q == Q2 ? refCntHigh : (q.sum() ? refSumLow : refCntLow);
                long expectedGroups = Arrays.stream(q == Q2 ? refCntHigh : refCntLow).filter(c -> c > 0).count();
                long expectedTotal = Arrays.stream(refCnt).sum();

                // Correctness once per query per path.
                long[] gotP = new long[refCnt.length];
                drainParquet(readerHandle, runtimeHandle, alloc, q, config.address(), root -> accumulate(root, gotP));
                assertArrayEquals("parquet result mismatch " + q.name(), refCnt, gotP);
                long[] gotD = new long[refCnt.length];
                DocValuesAggregationExecutor verifyExec = new DocValuesAggregationExecutor();
                drainDocValues(verifyExec, searcher, alloc, q, root -> accumulate(root, gotD));
                assertArrayEquals("doc_values result mismatch " + q.name(), refCnt, gotD);
                assertTrue("direct decode must engage for " + q.name(), verifyExec.directBatches() > 0);
                long[] gotJ = new long[refCnt.length];
                runJavaAggregation(reader, q, gotJ);
                assertArrayEquals("java (no-arrow) result mismatch " + q.name(), refCnt, gotJ);
                System.out.printf(Locale.ROOT, "[correctness] %s: parquet == dv+arrow == dv-java == reference%n", q.name());

                long[] p = new long[MEASURED];
                long[] d = new long[MEASURED];
                long[] j = new long[MEASURED];
                for (int iter = 0; iter < WARMUP + MEASURED; iter++) {
                    long tP = timeParquet(readerHandle, runtimeHandle, alloc, q, config.address(), expectedGroups, expectedTotal);
                    DocValuesAggregationExecutor executor = new DocValuesAggregationExecutor();
                    long tD = timeDocValues(executor, searcher, alloc, q, expectedGroups, expectedTotal);
                    totalDirect += executor.directBatches();
                    totalFallback += executor.fallbackBatches();
                    long tJ = timeJavaAggregation(reader, q, expectedGroups, expectedTotal);
                    if (iter >= WARMUP) {
                        p[iter - WARMUP] = tP;
                        d[iter - WARMUP] = tD;
                        j[iter - WARMUP] = tJ;
                    }
                    System.out.printf(
                        Locale.ROOT,
                        "[%s iter %d%s] parquet=%dms dv+arrow=%dms dv-java=%dms%n",
                        q.name(),
                        iter,
                        iter < WARMUP ? " warmup" : "",
                        TimeUnit.NANOSECONDS.toMillis(tP),
                        TimeUnit.NANOSECONDS.toMillis(tD),
                        TimeUnit.NANOSECONDS.toMillis(tJ)
                    );
                }
                table.append(
                    String.format(
                        Locale.ROOT,
                        "%-28s %12d %16d %16d%n",
                        q.name(),
                        TimeUnit.NANOSECONDS.toMillis(median(p)),
                        TimeUnit.NANOSECONDS.toMillis(median(d)),
                        TimeUnit.NANOSECONDS.toMillis(median(j))
                    )
                );
            }
            table.append(
                String.format(
                    Locale.ROOT,
                    "doc_values decode engagement across all runs: %d direct, %d fallback%n",
                    totalDirect,
                    totalFallback
                )
            );
            System.out.print(table);
            assertTrue("direct decode must dominate", totalDirect > 0 && totalFallback == 0);
        } finally {
            readerHandle.close();
            storeHandle.close();
            NativeStoreTestHelper.destroyTieredObjectStore(tieredStorePtr);
            runtimeHandle.close();
        }
    }

    // ---- Path D: the production scan-provider path ----

    private void drainDocValues(
        DocValuesAggregationExecutor executor,
        IndexSearcher searcher,
        RootAllocator alloc,
        QuerySpec q,
        Consumer<VectorSchemaRoot> perBatch
    ) throws Exception {
        EngineResultStream stream = executor.execute(searcher, new MatchAllDocsQuery(), q.aggSpec(), alloc, 0L);
        try {
            Iterator<EngineResultBatch> it = stream.iterator();
            while (it.hasNext()) {
                try (VectorSchemaRoot root = it.next().getArrowRoot()) {
                    perBatch.accept(root);
                }
            }
        } finally {
            stream.close();
        }
    }

    private long timeDocValues(
        DocValuesAggregationExecutor executor,
        IndexSearcher searcher,
        RootAllocator alloc,
        QuerySpec q,
        long expectedGroups,
        long expectedTotal
    ) throws Exception {
        long[] acc = new long[2];
        long t0 = System.nanoTime();
        drainDocValues(executor, searcher, alloc, q, root -> tally(root, acc));
        long elapsed = System.nanoTime() - t0;
        assertEquals("doc_values group count " + q.name(), expectedGroups, acc[0]);
        assertEquals("doc_values total " + q.name(), expectedTotal, acc[1]);
        return elapsed;
    }

    // ---- Path J: doc_values WITHOUT Arrow — the classic streaming-Java aggregator shape ----

    /**
     * Heap bulk decode ({@code longValues} into {@code long[]}) + {@link ReorganizingLongHash}
     * group-by with parallel count/sum arrays — mirrors {@code StreamNumericTermsAggregator}'s
     * shard collect (run-length memo included). No Arrow buffers, no native engine.
     */
    private void runJavaAggregation(DirectoryReader reader, QuerySpec q, long[] into) throws Exception {
        int batch = DocValuesAggregationExecutor.BATCH_SIZE;
        int[] docs = new int[batch];
        long[] keys = new long[batch];
        long[] metrics = q.sum() ? new long[batch] : null;
        String keyField = q.aggSpec().groupColumns().get(0);
        try (ReorganizingLongHash hash = new ReorganizingLongHash(BigArrays.NON_RECYCLING_INSTANCE)) {
            long[] counts = new long[1024];
            long[] sums = q.sum() ? new long[1024] : null;
            long[] ordKeys = new long[1024];
            for (var leaf : reader.leaves()) {
                var keyDv = leaf.reader().getNumericDocValues(keyField);
                var metricDv = q.sum() ? leaf.reader().getNumericDocValues(q.aggSpec().inputColumns().get(1)) : null;
                int maxDoc = leaf.reader().maxDoc();
                for (int start = 0; start < maxDoc; start += batch) {
                    int len = Math.min(batch, maxDoc - start);
                    for (int i = 0; i < len; i++) {
                        docs[i] = start + i;
                    }
                    keyDv.longValues(len, docs, 0, keys, 0, 0L);
                    if (metricDv != null) {
                        metricDv.longValues(len, docs, 0, metrics, 0, 0L);
                    }
                    long lastKey = 0;
                    long lastOrd = -1;
                    for (int i = 0; i < len; i++) {
                        long k = keys[i];
                        long ord;
                        if (lastOrd >= 0 && k == lastKey) {
                            ord = lastOrd;
                        } else {
                            ord = hash.add(k);
                            if (ord < 0) {
                                ord = -1 - ord;
                            }
                            lastKey = k;
                            lastOrd = ord;
                        }
                        int o = (int) ord;
                        if (o >= counts.length) {
                            int newLen = Integer.highestOneBit(o) << 1;
                            counts = Arrays.copyOf(counts, newLen);
                            ordKeys = Arrays.copyOf(ordKeys, newLen);
                            if (sums != null) {
                                sums = Arrays.copyOf(sums, newLen);
                            }
                        }
                        counts[o]++;
                        ordKeys[o] = k;
                        if (sums != null) {
                            sums[o] += metrics[i];
                        }
                    }
                }
            }
            long size = hash.size();
            for (long ord = 0; ord < size; ord++) {
                int o = (int) ord;
                into[(int) ordKeys[o]] += (sums != null) ? sums[o] : counts[o];
            }
        }
    }

    private long timeJavaAggregation(DirectoryReader reader, QuerySpec q, long expectedGroups, long expectedTotal) throws Exception {
        // Aggregate into a plain result array (same shape as the reference) and verify totals.
        long[] into = new long[q == Q2 ? KEY_HIGH_CARD : KEY_LOW_CARD];
        long t0 = System.nanoTime();
        runJavaAggregation(reader, q, into);
        long elapsed = System.nanoTime() - t0;
        long groups = Arrays.stream(into).filter(v -> v > 0).count();
        assertEquals("java group count " + q.name(), expectedGroups, groups);
        assertEquals("java total " + q.name(), expectedTotal, Arrays.stream(into).sum());
        return elapsed;
    }

    // ---- Path P: the production parquet path ----

    private long timeParquet(
        ReaderHandle readerHandle,
        NativeRuntimeHandle runtimeHandle,
        RootAllocator alloc,
        QuerySpec q,
        long configPtr,
        long expectedGroups,
        long expectedTotal
    ) {
        long[] acc = new long[2];
        long t0 = System.nanoTime();
        drainParquet(readerHandle, runtimeHandle, alloc, q, configPtr, root -> tally(root, acc));
        long elapsed = System.nanoTime() - t0;
        assertEquals("parquet group count " + q.name(), expectedGroups, acc[0]);
        assertEquals("parquet total " + q.name(), expectedTotal, acc[1]);
        return elapsed;
    }

    private void drainParquet(
        ReaderHandle readerHandle,
        NativeRuntimeHandle runtimeHandle,
        RootAllocator alloc,
        QuerySpec q,
        long configPtr,
        Consumer<VectorSchemaRoot> perBatch
    ) {
        byte[] substrait = NativeBridge.sqlToSubstrait(readerHandle.getPointer(), "test_table", q.sql(), runtimeHandle.get());
        long streamPtr = asyncCall(
            listener -> NativeBridge.executeQueryAsync(
                readerHandle.getPointer(),
                "test_table",
                substrait,
                runtimeHandle.get(),
                0L,
                configPtr,
                listener
            )
        );
        try (
            StreamHandle stream = new StreamHandle(streamPtr, runtimeHandle);
            CDataDictionaryProvider dict = new CDataDictionaryProvider()
        ) {
            long schemaAddr = asyncCall(listener -> NativeBridge.streamGetSchema(stream.getPointer(), listener));
            Schema schema = new Schema(importField(alloc, ArrowSchema.wrap(schemaAddr), dict).getChildren(), null);
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
                while (true) {
                    long arrayAddr = asyncCall(listener -> NativeBridge.streamNext(runtimeHandle.get(), stream.getPointer(), listener));
                    if (arrayAddr == 0) {
                        break;
                    }
                    Data.importIntoVectorSchemaRoot(alloc, ArrowArray.wrap(arrayAddr), root, dict);
                    perBatch.accept(root);
                }
            }
        }
    }

    // ---- Shared ----

    private static void accumulate(VectorSchemaRoot root, long[] into) {
        BigIntVector keys = (BigIntVector) root.getVector(0);
        BigIntVector vals = (BigIntVector) root.getVector(1);
        for (int i = 0; i < root.getRowCount(); i++) {
            into[(int) keys.get(i)] += vals.get(i);
        }
    }

    private static void tally(VectorSchemaRoot root, long[] acc) {
        int n = root.getRowCount();
        var buf = ((BigIntVector) root.getVector(1)).getDataBuffer();
        for (int i = 0; i < n; i++) {
            acc[1] += buf.getLong((long) i * Long.BYTES);
        }
        acc[0] += n;
    }

    private static long median(long[] values) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return sorted[sorted.length / 2];
    }

    private long asyncCall(Consumer<ActionListener<Long>> call) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        call.accept(new ActionListener<>() {
            @Override
            public void onResponse(Long v) {
                future.complete(v);
            }

            @Override
            public void onFailure(Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future.join();
    }
}
