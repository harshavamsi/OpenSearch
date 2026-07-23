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
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.ReaderHandle;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.exec.MonoFileWriterSet;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.arrow.c.Data.importField;

/**
 * Mustang-as-is on real ClickBench: all 43 standard queries against the full 100M-row
 * {@code hits.parquet} through the production parquet execution path
 * ({@code sqlToSubstrait} + {@code executeQueryAsync}, default {@link WireConfigSnapshot}
 * — target_partitions=4). Per-query medians; SQL-unsupported queries reported, not fatal.
 *
 * <p>Fixtures: {@code /local/home/hvamsi/clickbench/hits.parquet} (ClickBench official,
 * 99,997,497 rows) and {@code /local/home/hvamsi/clickbench/queries.sql} (the 43 standard
 * queries, {@code hits} → {@code test_table}).
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@TimeoutSuite(millis = 3 * 3600 * 1000)
public class ClickBenchParquetBenchTests extends OpenSearchTestCase {

    private static final Path DATA_DIR = Path.of("/local/home/hvamsi/clickbench");
    private static final String PARQUET_FILE = "hits.parquet";
    private static final long ROWS = 99_997_497L;
    private static final int WARMUP = 1;
    // Per user directive: single measured iteration (1 warmup for page-cache/JIT floor).
    // No request cache in play: these harnesses call the engines directly (no OpenSearch
    // query layer), and the DataFusion cache manager is disabled (cache_mgr ptr = 0).
    private static final int MEASURED = 1;

    public void testClickBenchAllQueries() throws Exception {
        // Queries ship as a test resource (classpath reads clear the test security manager);
        // hits.parquet is read by native code only, which the security manager doesn't see.
        List<String> queries;
        try (var in = getClass().getClassLoader().getResourceAsStream("clickbench-queries.sql")) {
            assumeTrue("clickbench-queries.sql resource present", in != null);
            queries = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).lines().toList();
        }

        NativeBridge.initTokioRuntimeManager(4);
        Path spillDir = createTempDir("cb-spill");
        // 48GB pool — q33-class group-bys (WatchID+ClientIP, ~100M groups) need tens of GB at 100M rows.
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
                "%n=== ClickBench, mustang parquet path as-is: %d rows, default config, %d warmup + %d measured, median ms ===%n",
                ROWS,
                WARMUP,
                MEASURED
            )
        );
        table.append(String.format(Locale.ROOT, "%-5s %12s %10s   %s%n", "query", "median_ms", "rows_out", "status"));

        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE); Arena configArena = Arena.ofConfined()) {
            MemorySegment config = configArena.allocate(WireConfigSnapshot.BYTE_SIZE);
            WireConfigSnapshot.builder().build().writeTo(config);

            int ok = 0, failed = 0;
            for (int qi = 0; qi < queries.size(); qi++) {
                String sql = queries.get(qi).trim();
                if (sql.isEmpty()) {
                    continue;
                }
                String qname = "q" + (qi + 1);
                try {
                    byte[] substrait = NativeBridge.sqlToSubstrait(readerHandle.getPointer(), "test_table", sql, runtimeHandle.get());
                    long[] times = new long[MEASURED];
                    long rowsOut = 0;
                    for (int iter = 0; iter < WARMUP + MEASURED; iter++) {
                        long[] outRows = new long[1];
                        long t0 = System.nanoTime();
                        drain(readerHandle, runtimeHandle, alloc, substrait, config.address(), root -> outRows[0] += root.getRowCount());
                        long elapsed = System.nanoTime() - t0;
                        rowsOut = outRows[0];
                        if (iter >= WARMUP) {
                            times[iter - WARMUP] = elapsed;
                        }
                    }
                    long med = TimeUnit.NANOSECONDS.toMillis(median(times));
                    table.append(String.format(Locale.ROOT, "%-5s %12d %10d   ok%n", qname, med, rowsOut));
                    System.out.printf(Locale.ROOT, "[%s] median=%dms rows_out=%d%n", qname, med, rowsOut);
                    ok++;
                } catch (Throwable t) {
                    String reason = String.valueOf(t.getMessage()).replace('\n', ' ');
                    table.append(
                        String.format(
                            Locale.ROOT,
                            "%-5s %12s %10s   FAILED: %s%n",
                            qname,
                            "-",
                            "-",
                            reason.substring(0, Math.min(140, reason.length()))
                        )
                    );
                    System.out.printf(Locale.ROOT, "[%s] FAILED: %s%n", qname, reason);
                    failed++;
                }
            }
            table.append(String.format(Locale.ROOT, "passed=%d failed=%d%n", ok, failed));
            System.out.print(table);
            assertTrue("at least half the suite must run", ok > queries.size() / 2);
        } finally {
            readerHandle.close();
            storeHandle.close();
            NativeStoreTestHelper.destroyTieredObjectStore(tieredStorePtr);
            runtimeHandle.close();
        }
    }

    private void drain(
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
