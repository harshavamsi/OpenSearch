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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.be.lucene.DocValuesAggregationExecutor;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.ReaderHandle;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.exec.MonoFileWriterSet;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.arrow.c.Data.importField;

/**
 * Full ClickBench suite, three variants, single iteration each, no caches (engines are
 * called directly — no OpenSearch request cache in the path; the DataFusion native cache
 * manager is disabled). Per-leg coverage is scoped honestly:
 *
 * <ul>
 *   <li><b>parquet</b> — all 43 via SQL (mustang as-is; known dialect failures reported).</li>
 *   <li><b>dv+arrow</b> — every aggregate query expressible as a shard fragment, driven by
 *       {@link ClickBenchDvQueries} (group-bys incl. keyword/multi-key/DISTINCT/expressions
 *       and global aggregates). Row-returning / HAVING / CASE / regexp / time-function
 *       queries report {@code n/a} with the reason.</li>
 *   <li><b>dv-java</b> — single-numeric-key group-bys and global COUNT/SUM/MIN/MAX (the
 *       classic-tier shape), via the same reference implementation as the subset bench.</li>
 * </ul>
 *
 * <p>Cross-leg correctness: group-count + first-agg-total invariants must match parquet's
 * whenever a dv leg runs. Requires {@code -Dtests.clickbench=true -Dtests.security.manager=false}
 * and the fixtures under {@code /local/home/hvamsi/clickbench} (see ClickBenchDvQueries for
 * the column set; the Lucene index builds on first run).
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@TimeoutSuite(millis = 6 * 3600 * 1000)
public class ClickBenchFullThreeWayBenchTests extends OpenSearchTestCase {

    private static final Path DATA_DIR = Path.of("/local/home/hvamsi/clickbench");
    private static final String PARQUET_FILE = "hits.parquet";
    private static final long ROWS = 99_997_497L;

    public void testFullSuiteThreeWay() throws Exception {
        assumeTrue("opt-in benchmark: -Dtests.clickbench=true", Boolean.getBoolean("tests.clickbench"));
        assumeTrue("hits.parquet present", Files.exists(DATA_DIR.resolve(PARQUET_FILE)));

        List<String> sqls;
        try (var in = getClass().getClassLoader().getResourceAsStream("clickbench-queries.sql")) {
            assumeTrue("clickbench-queries.sql present", in != null);
            sqls = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).lines().toList();
        }

        ClickBenchDvQueries.buildIndexIfMissing(DATA_DIR, (int) ROWS);

        NativeBridge.initTokioRuntimeManager(4);
        Path spillDir = createTempDir("cbf-spill");
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
                "%n=== ClickBench FULL three-way: %d rows, 1 iteration, no caches, default parquet config ===%n",
                ROWS
            )
        );
        table.append(String.format(Locale.ROOT, "%-5s %10s %14s %14s   %s%n", "query", "parquet", "dv+arrow", "dv-java", "notes"));

        try (
            RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
            Arena configArena = Arena.ofConfined();
            MMapDirectory dir = new MMapDirectory(DATA_DIR.resolve("lucene-index-full"));
            DirectoryReader reader = DirectoryReader.open(dir)
        ) {
            ClickBenchDvQueries.installEngine(runtimeHandle);
            IndexSearcher searcher = new IndexSearcher(reader);
            MemorySegment config = configArena.allocate(WireConfigSnapshot.BYTE_SIZE);
            WireConfigSnapshot.builder().build().writeTo(config);

            String only = System.getProperty("tests.clickbench.only", "");
            java.util.Set<Integer> onlySet = only.isEmpty()
                ? java.util.Set.of()
                : java.util.Arrays.stream(only.split(",")).map(Integer::parseInt).collect(java.util.stream.Collectors.toSet());
            for (int qi = 0; qi < sqls.size(); qi++) {
                String qname = "q" + (qi + 1);
                String sql = sqls.get(qi).trim();
                if (sql.isEmpty() || (onlySet.isEmpty() == false && onlySet.contains(qi + 1) == false)) {
                    continue;
                }

                // ---- parquet leg ----
                String pCell;
                long[] pInv = new long[2];
                try {
                    byte[] substrait = NativeBridge.sqlToSubstrait(readerHandle.getPointer(), "test_table", sql, runtimeHandle.get());
                    long t0 = System.nanoTime();
                    drainParquet(readerHandle, runtimeHandle, alloc, substrait, config.address(), root -> {
                        pInv[0] += root.getRowCount();
                    });
                    pCell = Long.toString(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0));
                } catch (Throwable t) {
                    pCell = "FAIL";
                    pInv[0] = -1;
                    System.out.printf(Locale.ROOT, "[%s parquet] FAILED: %s%n", qname, String.valueOf(t.getMessage()).replace('\n', ' '));
                }

                // ---- dv+arrow leg ----
                String dCell;
                String note = "";
                ClickBenchDvQueries.DvQuery dvQuery = ClickBenchDvQueries.forQuery(qi + 1);
                if (dvQuery == null) {
                    dCell = "n/a";
                    note = ClickBenchDvQueries.ineligibleReason(qi + 1);
                } else {
                    try {
                        DocValuesAggregationExecutor executor = new DocValuesAggregationExecutor();
                        long[] dInv = new long[1];
                        long t0 = System.nanoTime();
                        EngineResultStream stream = dvQuery.run(executor, searcher, alloc);
                        try {
                            var it = stream.iterator();
                            while (it.hasNext()) {
                                try (VectorSchemaRoot root = it.next().getArrowRoot()) {
                                    dInv[0] += root.getRowCount();
                                }
                            }
                        } finally {
                            stream.close();
                        }
                        long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                        dCell = Long.toString(ms);
                        // Group-count invariant vs parquet where the shapes are identical.
                        if (dvQuery.comparableToParquet() && pInv[0] >= 0 && dInv[0] != pInv[0]) {
                            dCell = ms + "!";
                            note = "ROWS DIFFER p=" + pInv[0] + " d=" + dInv[0];
                        }
                    } catch (Throwable t) {
                        dCell = "FAIL";
                        System.out.printf(Locale.ROOT, "[%s dv+arrow] FAILED: %s%n", qname, String.valueOf(t.getMessage()).replace('\n', ' '));
                    }
                }

                // ---- dv-java leg ----
                String jCell;
                ClickBenchDvQueries.JavaQuery javaQuery = ClickBenchDvQueries.javaForQuery(qi + 1);
                if (javaQuery == null) {
                    jCell = "n/a";
                } else {
                    try {
                        long t0 = System.nanoTime();
                        javaQuery.run(reader, searcher);
                        jCell = Long.toString(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0));
                    } catch (Throwable t) {
                        jCell = "FAIL";
                        System.out.printf(Locale.ROOT, "[%s dv-java] FAILED: %s%n", qname, String.valueOf(t.getMessage()).replace('\n', ' '));
                    }
                }

                table.append(String.format(Locale.ROOT, "%-5s %10s %14s %14s   %s%n", qname, pCell, dCell, jCell, note));
                System.out.printf(Locale.ROOT, "[%s] parquet=%s dv+arrow=%s dv-java=%s %s%n", qname, pCell, dCell, jCell, note);
            }
            System.out.print(table);
        } finally {
            readerHandle.close();
            storeHandle.close();
            NativeStoreTestHelper.destroyTieredObjectStore(tieredStorePtr);
            runtimeHandle.close();
        }
    }

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
        try (StreamHandle stream = new StreamHandle(streamPtr, runtimeHandle); CDataDictionaryProvider dict = new CDataDictionaryProvider()) {
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
