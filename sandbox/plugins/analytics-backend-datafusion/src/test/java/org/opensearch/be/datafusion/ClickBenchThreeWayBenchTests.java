/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
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

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.arrow.c.Data.importField;

/**
 * Three-way ClickBench subset on the REAL 100M-row dataset: mustang parquet as-is vs
 * doc_values+Arrow (production scan provider) vs doc_values pure-Java. Covers the
 * numeric group-by queries the v1 doc_values path can express exactly:
 *
 * <ul>
 *   <li>q8 — {@code SELECT AdvEngineID, COUNT(*) WHERE AdvEngineID <> 0 GROUP BY AdvEngineID}</li>
 *   <li>q16 — {@code SELECT UserID, COUNT(*) GROUP BY UserID ORDER BY c DESC LIMIT 10}
 *       (~17M groups; dv legs drain all groups and top-10 in Java — same work the
 *       coordinator does with shard partials)</li>
 *   <li>q33′ — {@code SELECT WatchID, ClientIP, COUNT(*), SUM(IsRefresh) GROUP BY WatchID,
 *       ClientIP} (q33 minus AVG — AVG is planner-decomposed upstream in PPL; both engines
 *       run the SAME reduced SQL so the comparison stays apples-to-apples; ~100M groups).
 *       The pure-Java tier has no multi-key group primitive (multi_terms is object-keyed) —
 *       reported as n/a, which is itself the finding.</li>
 * </ul>
 *
 * <p>Requires {@code -Dtests.security.manager=false} (reads column .bin files and the
 * shared Lucene index outside the test sandbox). Fixtures under
 * {@code /local/home/hvamsi/clickbench}: hits.parquet + {@code *.bin} column extracts; the Lucene
 * index is built on first run (~10-20 min) at {@code lucene-index/} next to them and reused.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@TimeoutSuite(millis = 3 * 3600 * 1000)
public class ClickBenchThreeWayBenchTests extends OpenSearchTestCase {

    private static final Path DATA_DIR = Path.of("/local/home/hvamsi/clickbench");
    private static final Path INDEX_DIR = DATA_DIR.resolve("lucene-index");
    private static final String PARQUET_FILE = "hits.parquet";
    private static final int ROWS = 99_997_497;
    private static final int WARMUP = 1;
    // Per user directive: single measured iteration (1 warmup for page-cache/JIT floor).
    // No request cache in play: these harnesses call the engines directly (no OpenSearch
    // query layer), and the DataFusion cache manager is disabled (cache_mgr ptr = 0).
    private static final int MEASURED = 1;

    private static final List<String> INDEXED_COLS = List.of("AdvEngineID", "UserID", "WatchID", "ClientIP", "IsRefresh");
    /** String columns indexed as SortedDocValues from length-prefixed .strbin extracts. */
    private static final List<String> INDEXED_STR_COLS = List.of("SearchPhrase");

    public void testClickBenchThreeWay() throws Exception {
        // Reads fixtures outside the test sandbox — must run with -Dtests.security.manager=false
        // AND explicit opt-in (skipped in plain suite runs where the policy denies the paths).
        assumeTrue("opt-in benchmark: -Dtests.clickbench=true", Boolean.getBoolean("tests.clickbench"));
        assumeTrue("hits.parquet present", Files.exists(DATA_DIR.resolve(PARQUET_FILE)));
        for (String col : INDEXED_COLS) {
            assumeTrue(col + ".bin present", Files.exists(DATA_DIR.resolve(col + ".bin")));
        }
        for (String col : INDEXED_STR_COLS) {
            assumeTrue(col + ".strbin present", Files.exists(DATA_DIR.resolve(col + ".strbin")));
        }

        buildIndexIfMissing();

        NativeBridge.initTokioRuntimeManager(4);
        Path spillDir = createTempDir("cb3-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(48L * 1024 * 1024 * 1024, 0L, spillDir.toString(), 16L * 1024 * 1024 * 1024);
        assertTrue(runtimePtr != 0);
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);

        long tieredStorePtr = NativeStoreTestHelper.createTieredObjectStore(0L, 0L);
        NativeStoreHandle storeHandle = new NativeStoreHandle(
            NativeStoreTestHelper.getObjectStoreBoxPtr(tieredStorePtr),
            NativeStoreTestHelper::destroyObjectStoreBoxPtr
        );
        ReaderHandle readerHandle = new ReaderHandle(
            DATA_DIR.toString(),
            List.of(MonoFileWriterSet.of(".", 0L, PARQUET_FILE, ROWS)),
            storeHandle,
            List.of(),
            List.of()
        );

        StringBuilder table = new StringBuilder();
        table.append(
            String.format(
                Locale.ROOT,
                "%n=== ClickBench three-way (100M rows, default parquet config, %d warmup + %d measured, median ms) ===%n",
                WARMUP,
                MEASURED
            )
        );
        table.append(String.format(Locale.ROOT, "%-44s %10s %16s %16s%n", "query", "parquet", "dv+arrow(prov)", "dv-java"));

        try (
            RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
            Arena configArena = Arena.ofConfined();
            MMapDirectory dir = new MMapDirectory(INDEX_DIR);
            DirectoryReader reader = DirectoryReader.open(dir)
        ) {
            DataFusionService service = org.mockito.Mockito.mock(DataFusionService.class);
            org.mockito.Mockito.when(service.getNativeRuntime()).thenReturn(runtimeHandle);
            java.lang.reflect.Constructor<?> ctor = Class.forName("org.opensearch.be.datafusion.DatafusionShardAggregationEngine")
                .getDeclaredConstructor(DataFusionService.class);
            ctor.setAccessible(true);
            ShardAggregationEngineHolder.install((org.opensearch.analytics.spi.ShardAggregationEngine) ctor.newInstance(service));

            IndexSearcher searcher = new IndexSearcher(reader);
            MemorySegment config = configArena.allocate(WireConfigSnapshot.BYTE_SIZE);
            WireConfigSnapshot.builder().build().writeTo(config);

            // ---- q8 ----
            runThreeWay(
                table,
                "q8 AdvEngineID cnt (filtered)",
                "SELECT \"AdvEngineID\", COUNT(*) FROM test_table WHERE \"AdvEngineID\" <> 0 GROUP BY \"AdvEngineID\"",
                readerHandle,
                runtimeHandle,
                alloc,
                config.address(),
                searcher,
                LongPoint.newRangeQuery("AdvEngineID", 1, Long.MAX_VALUE),
                new AggSpec(List.of("AdvEngineID"), List.of("AdvEngineID"), List.of(new AggCall(AggFunction.COUNT, null, "c"))),
                reader,
                true
            );

            // ---- q16 ----
            runThreeWay(
                table,
                "q16 UserID cnt (~17M groups)",
                "SELECT \"UserID\", COUNT(*) FROM test_table GROUP BY \"UserID\"",
                readerHandle,
                runtimeHandle,
                alloc,
                config.address(),
                searcher,
                new MatchAllDocsQuery(),
                new AggSpec(List.of("UserID"), List.of("UserID"), List.of(new AggCall(AggFunction.COUNT, null, "c"))),
                reader,
                true
            );

            // ---- q33' ----
            runThreeWay(
                table,
                "q33' WatchID+ClientIP cnt+sum (~100M grp)",
                "SELECT \"WatchID\", \"ClientIP\", COUNT(*), SUM(\"IsRefresh\") FROM test_table GROUP BY \"WatchID\", \"ClientIP\"",
                readerHandle,
                runtimeHandle,
                alloc,
                config.address(),
                searcher,
                new MatchAllDocsQuery(),
                new AggSpec(
                    List.of("WatchID", "ClientIP", "IsRefresh"),
                    List.of("WatchID", "ClientIP"),
                    List.of(new AggCall(AggFunction.COUNT, null, "c"), new AggCall(AggFunction.SUM, "IsRefresh", "s"))
                ),
                reader,
                false // Java tier: no multi-key long-hash primitive
            );

            // ---- q13' SearchPhrase group-by (keyword; both engines run the same reduced
            // SQL: no <> '' filter, no LIMIT — group cardinality invariants compare the legs) ----
            runKeywordThreeWay(
                table,
                "q13' SearchPhrase cnt (keyword)",
                "SELECT \"SearchPhrase\", COUNT(*) FROM test_table GROUP BY \"SearchPhrase\"",
                readerHandle,
                runtimeHandle,
                alloc,
                config.address(),
                searcher
            );

            System.out.print(table);
        } finally {
            readerHandle.close();
            storeHandle.close();
            NativeStoreTestHelper.destroyTieredObjectStore(tieredStorePtr);
            runtimeHandle.close();
        }
    }

    /**
     * Runs one query on all three legs; correctness = group-count and grand-total invariants
     * must agree across legs (full-map compare is infeasible at 17M-100M groups).
     */
    private void runThreeWay(
        StringBuilder table,
        String label,
        String sql,
        ReaderHandle readerHandle,
        NativeRuntimeHandle runtimeHandle,
        RootAllocator alloc,
        long configPtr,
        IndexSearcher searcher,
        Query dvQuery,
        AggSpec spec,
        DirectoryReader reader,
        boolean javaLeg
    ) throws Exception {
        byte[] substrait = NativeBridge.sqlToSubstrait(readerHandle.getPointer(), "test_table", sql, runtimeHandle.get());
        int aggCols = spec.aggCalls().size();
        int keyCols = spec.groupColumns().size();

        long[] pTimes = new long[MEASURED];
        long[] dTimes = new long[MEASURED];
        long[] jTimes = new long[MEASURED];
        long[] pInv = null, dInv = null, jInv = null;

        for (int iter = 0; iter < WARMUP + MEASURED; iter++) {
            long[] inv = new long[2]; // [groups, grand total of first agg col]
            long t0 = System.nanoTime();
            drainParquet(readerHandle, runtimeHandle, alloc, substrait, configPtr, root -> tally(root, keyCols, inv));
            long tP = System.nanoTime() - t0;
            pInv = inv.clone();

            inv[0] = inv[1] = 0;
            DocValuesAggregationExecutor executor = new DocValuesAggregationExecutor();
            t0 = System.nanoTime();
            EngineResultStream stream = executor.execute(searcher, dvQuery, spec, alloc, 0L);
            try {
                Iterator<EngineResultBatch> it = stream.iterator();
                while (it.hasNext()) {
                    try (VectorSchemaRoot root = it.next().getArrowRoot()) {
                        tally(root, keyCols, inv);
                    }
                }
            } finally {
                stream.close();
            }
            long tD = System.nanoTime() - t0;
            dInv = inv.clone();

            long tJ = -1;
            if (javaLeg) {
                inv[0] = inv[1] = 0;
                t0 = System.nanoTime();
                javaAggregate(reader, dvQuery, spec, searcher, inv);
                tJ = System.nanoTime() - t0;
                jInv = inv.clone();
            }

            if (iter >= WARMUP) {
                pTimes[iter - WARMUP] = tP;
                dTimes[iter - WARMUP] = tD;
                jTimes[iter - WARMUP] = tJ;
            }
            System.out.printf(
                Locale.ROOT,
                "[%s iter %d%s] parquet=%dms dv+arrow=%dms dv-java=%s (direct=%d fallback=%d)%n",
                label,
                iter,
                iter < WARMUP ? " warmup" : "",
                TimeUnit.NANOSECONDS.toMillis(tP),
                TimeUnit.NANOSECONDS.toMillis(tD),
                tJ < 0 ? "n/a" : TimeUnit.NANOSECONDS.toMillis(tJ) + "ms",
                executor.directBatches(),
                executor.fallbackBatches()
            );
        }

        assertArrayEquals(label + ": dv+arrow invariants differ from parquet", pInv, dInv);
        if (javaLeg) {
            assertArrayEquals(label + ": dv-java invariants differ from parquet", pInv, jInv);
        }
        System.out.printf(Locale.ROOT, "[correctness] %s: groups=%d total=%d agree across legs%n", label, pInv[0], pInv[1]);

        table.append(
            String.format(
                Locale.ROOT,
                "%-44s %10d %16d %16s%n",
                label,
                TimeUnit.NANOSECONDS.toMillis(median(pTimes)),
                TimeUnit.NANOSECONDS.toMillis(median(dTimes)),
                javaLeg ? String.valueOf(TimeUnit.NANOSECONDS.toMillis(median(jTimes))) : "n/a (multi-key)"
            )
        );
    }

    /** groups += rows; grand-total += first agg column (immediately after the key columns). */
    private static void tally(VectorSchemaRoot root, int keyCols, long[] inv) {
        int n = root.getRowCount();
        var buf = ((BigIntVector) root.getVector(keyCols)).getDataBuffer();
        for (int i = 0; i < n; i++) {
            inv[1] += buf.getLong((long) i * Long.BYTES);
        }
        inv[0] += n;
    }

    /** Keyword leg: dv+arrow via compiled plan (Utf8View group-by); no java leg (object-keyed). */
    private void runKeywordThreeWay(
        StringBuilder table,
        String label,
        String sql,
        ReaderHandle readerHandle,
        NativeRuntimeHandle runtimeHandle,
        RootAllocator alloc,
        long configPtr,
        IndexSearcher searcher
    ) throws Exception {
        byte[] substrait = NativeBridge.sqlToSubstrait(readerHandle.getPointer(), "test_table", sql, runtimeHandle.get());
        // v2 spec entry: keyword group key probed from the index routes to the ordinal-first
        // path (per-segment ordinal group-by, terms materialized for result groups only).
        org.opensearch.analytics.spi.ShardAggregationEngine.AggSpec dvSpec =
            new org.opensearch.analytics.spi.ShardAggregationEngine.AggSpec(
                List.of("SearchPhrase"),
                List.of("SearchPhrase"),
                List.of(
                    new org.opensearch.analytics.spi.ShardAggregationEngine.AggCall(
                        org.opensearch.analytics.spi.ShardAggregationEngine.AggFunction.COUNT,
                        null,
                        "cnt"
                    )
                )
            );

        long[] pTimes = new long[MEASURED];
        long[] dTimes = new long[MEASURED];
        long[] pInv = null, dInv = null;
        for (int iter = 0; iter < WARMUP + MEASURED; iter++) {
            long[] inv = new long[2];
            long t0 = System.nanoTime();
            drainParquet(readerHandle, runtimeHandle, alloc, substrait, configPtr, root -> tally(root, 1, inv));
            long tP = System.nanoTime() - t0;
            pInv = inv.clone();

            inv[0] = inv[1] = 0;
            DocValuesAggregationExecutor executor = new DocValuesAggregationExecutor();
            t0 = System.nanoTime();
            EngineResultStream stream = executor.execute(searcher, new MatchAllDocsQuery(), dvSpec, alloc, 0L);
            try {
                Iterator<EngineResultBatch> it = stream.iterator();
                while (it.hasNext()) {
                    try (VectorSchemaRoot root = it.next().getArrowRoot()) {
                        tally(root, 1, inv);
                    }
                }
            } finally {
                stream.close();
            }
            long tD = System.nanoTime() - t0;
            dInv = inv.clone();

            if (iter >= WARMUP) {
                pTimes[iter - WARMUP] = tP;
                dTimes[iter - WARMUP] = tD;
            }
            System.out.printf(
                Locale.ROOT,
                "[%s iter %d%s] parquet=%dms dv+arrow=%dms%n",
                label,
                iter,
                iter < WARMUP ? " warmup" : "",
                TimeUnit.NANOSECONDS.toMillis(tP),
                TimeUnit.NANOSECONDS.toMillis(tD)
            );
        }
        assertArrayEquals(label + ": dv+arrow invariants differ from parquet", pInv, dInv);
        System.out.printf(Locale.ROOT, "[correctness] %s: groups=%d total=%d agree%n", label, pInv[0], pInv[1]);
        table.append(
            String.format(
                Locale.ROOT,
                "%-44s %10d %16d %16s%n",
                label,
                TimeUnit.NANOSECONDS.toMillis(median(pTimes)),
                TimeUnit.NANOSECONDS.toMillis(median(dTimes)),
                "n/a (keyword)"
            )
        );
    }

    /** {@code SELECT SearchPhrase, COUNT(*) FROM <stage-input SearchPhrase> GROUP BY SearchPhrase}. */
    private static org.apache.calcite.rel.RelNode buildKeywordCountFragment() {
        org.apache.calcite.rel.type.RelDataTypeFactory typeFactory = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
        org.apache.calcite.plan.RelOptCluster cluster = org.apache.calcite.plan.RelOptCluster.create(
            new org.apache.calcite.plan.hep.HepPlanner(new org.apache.calcite.plan.hep.HepProgramBuilder().build()),
            new org.apache.calcite.rex.RexBuilder(typeFactory)
        );
        org.apache.calcite.rel.type.RelDataType varchar = typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR),
            true
        );
        org.apache.calcite.rel.type.RelDataType rowType = typeFactory.builder().add("SearchPhrase", varchar).build();
        org.opensearch.analytics.planner.rel.OpenSearchStageInputScan scan =
            new org.opensearch.analytics.planner.rel.OpenSearchStageInputScan(
                cluster,
                cluster.traitSet(),
                0,
                rowType,
                List.of(),
                List.of()
            );
        org.apache.calcite.rel.core.AggregateCall count = org.apache.calcite.rel.core.AggregateCall.create(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT),
            "cnt"
        );
        return org.apache.calcite.rel.logical.LogicalAggregate.create(
            scan,
            List.of(),
            org.apache.calcite.util.ImmutableBitSet.of(0),
            null,
            List.of(count)
        );
    }

    // ---- dv-java leg: heap bulk decode + ReorganizingLongHash (single key + optional sum) ----

    private void javaAggregate(DirectoryReader reader, Query query, AggSpec spec, IndexSearcher searcher, long[] inv) throws IOException {
        int batch = DocValuesAggregationExecutor.BATCH_SIZE;
        int[] docs = new int[batch];
        long[] keys = new long[batch];
        String keyField = spec.groupColumns().get(0);
        var weight = searcher.createWeight(searcher.rewrite(query), org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES, 1f);
        try (ReorganizingLongHash hash = new ReorganizingLongHash(BigArrays.NON_RECYCLING_INSTANCE)) {
            long[] counts = new long[1 << 20];
            for (var leaf : reader.leaves()) {
                var scorer = weight.scorer(leaf);
                if (scorer == null) {
                    continue;
                }
                var keyDv = leaf.reader().getNumericDocValues(keyField);
                var it = scorer.iterator();
                int size = 0;
                for (int doc = it.nextDoc(); doc != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                    docs[size++] = doc;
                    if (size == batch) {
                        keyDv.longValues(size, docs, 0, keys, 0, 0L);
                        counts = addBatch(hash, keys, size, counts);
                        size = 0;
                    }
                }
                if (size > 0) {
                    keyDv.longValues(size, docs, 0, keys, 0, 0L);
                    counts = addBatch(hash, keys, size, counts);
                }
            }
            long groups = hash.size();
            long total = 0;
            for (long ord = 0; ord < groups; ord++) {
                total += counts[(int) ord];
            }
            inv[0] = groups;
            inv[1] = total;
        }
    }

    private static long[] addBatch(ReorganizingLongHash hash, long[] keys, int size, long[] counts) {
        long lastKey = 0;
        long lastOrd = -1;
        for (int i = 0; i < size; i++) {
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
                counts = Arrays.copyOf(counts, Integer.highestOneBit(o) << 1);
            }
            counts[o]++;
        }
        return counts;
    }

    // ---- fixture: build the Lucene index once, reuse across runs ----

    private void buildIndexIfMissing() throws IOException {
        if (Files.exists(INDEX_DIR.resolve("write.lock"))
            || (Files.isDirectory(INDEX_DIR) && Files.list(INDEX_DIR).findAny().isPresent())) {
            System.out.println("[setup] reusing existing Lucene index at " + INDEX_DIR);
            return;
        }
        System.out.println("[setup] building Lucene index (100M docs, 5 dv columns) — one-time cost");
        Files.createDirectories(INDEX_DIR);
        long t0 = System.nanoTime();
        MemorySegment[] cols = new MemorySegment[INDEXED_COLS.size()];
        try (Arena arena = Arena.ofConfined()) {
            for (int c = 0; c < INDEXED_COLS.size(); c++) {
                try (FileChannel ch = FileChannel.open(DATA_DIR.resolve(INDEXED_COLS.get(c) + ".bin"), StandardOpenOption.READ)) {
                    cols[c] = ch.map(FileChannel.MapMode.READ_ONLY, 0, (long) ROWS * Long.BYTES, arena);
                }
            }
            MemorySegment[] strCols = new MemorySegment[INDEXED_STR_COLS.size()];
            long[] strPos = new long[INDEXED_STR_COLS.size()];
            for (int c = 0; c < INDEXED_STR_COLS.size(); c++) {
                Path strbin = DATA_DIR.resolve(INDEXED_STR_COLS.get(c) + ".strbin");
                try (FileChannel ch = FileChannel.open(strbin, StandardOpenOption.READ)) {
                    strCols[c] = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size(), arena);
                }
            }
            try (MMapDirectory dir = new MMapDirectory(INDEX_DIR)) {
                IndexWriterConfig iwc = new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec()).setRAMBufferSizeMB(1024);
                try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                    Document doc = new Document();
                    NumericDocValuesField[] fields = new NumericDocValuesField[INDEXED_COLS.size()];
                    for (int c = 0; c < INDEXED_COLS.size(); c++) {
                        fields[c] = new NumericDocValuesField(INDEXED_COLS.get(c), 0);
                        doc.add(fields[c]);
                    }
                    org.apache.lucene.document.SortedDocValuesField[] strFields =
                        new org.apache.lucene.document.SortedDocValuesField[INDEXED_STR_COLS.size()];
                    for (int c = 0; c < INDEXED_STR_COLS.size(); c++) {
                        strFields[c] = new org.apache.lucene.document.SortedDocValuesField(
                            INDEXED_STR_COLS.get(c),
                            new org.apache.lucene.util.BytesRef("")
                        );
                        doc.add(strFields[c]);
                    }
                    LongPoint advPoint = new LongPoint("AdvEngineID", 0);
                    doc.add(advPoint);
                    byte[] strScratch = new byte[1 << 16];
                    for (int i = 0; i < ROWS; i++) {
                        for (int c = 0; c < INDEXED_COLS.size(); c++) {
                            fields[c].setLongValue(cols[c].getAtIndex(ValueLayout.JAVA_LONG, i));
                        }
                        for (int c = 0; c < INDEXED_STR_COLS.size(); c++) {
                            int len = strCols[c].get(ValueLayout.JAVA_INT_UNALIGNED, strPos[c]);
                            strPos[c] += 4;
                            if (len > strScratch.length) {
                                strScratch = new byte[Integer.highestOneBit(len) << 1];
                            }
                            MemorySegment.copy(strCols[c], strPos[c], MemorySegment.ofArray(strScratch), 0, len);
                            strPos[c] += len;
                            strFields[c].setBytesValue(new org.apache.lucene.util.BytesRef(strScratch, 0, len));
                        }
                        advPoint.setLongValue(cols[0].getAtIndex(ValueLayout.JAVA_LONG, i));
                        writer.addDocument(doc);
                    }
                    writer.forceMerge(1);
                }
            }
        }
        System.out.printf(Locale.ROOT, "[setup] index built in %ds%n", TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - t0));
    }

    // ---- parquet drain (same as ClickBenchParquetBenchTests) ----

    private void drainParquet(
        ReaderHandle readerHandle,
        NativeRuntimeHandle runtimeHandle,
        RootAllocator alloc,
        byte[] substrait,
        long configPtr,
        Consumer<VectorSchemaRoot> perBatch
    ) {
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
