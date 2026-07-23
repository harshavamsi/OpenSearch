/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
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
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.ReorganizingLongHash;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;

/**
 * POC: DataFusion performing a terms-style aggregation (GROUP BY key + COUNT(*)) at the shard
 * level over Arrow batches, benchmarked against the Java-side group-by primitive
 * ({@link ReorganizingLongHash}) that {@code StreamNumericTermsAggregator} uses.
 *
 * <p>Recipe mirrors {@link DatafusionMemtableReduceSinkTests}: batches are fed into a
 * {@link DatafusionMemtableReduceSink}, the Substrait plan (built with Calcite + isthmus)
 * runs GROUP BY over the buffered memtable, and results drain into a capturing sink.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class DatafusionShardTermsAggBenchTests extends OpenSearchTestCase {

    private static final int BATCH_SIZE = 4096;

    // ---------------------------------------------------------------
    // Correctness: 10k rows, 100 distinct keys — DF counts must match a HashMap reference
    // ---------------------------------------------------------------
    public void testGroupByCountMatchesJavaReference() throws Exception {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("df-terms-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(256 * 1024 * 1024, 0L, spillDir.toString(), 128 * 1024 * 1024);
        assertTrue("runtime ptr non-zero", runtimePtr != 0);
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);

        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            int rows = 10_000;
            long[] keys = new long[rows];
            Map<Long, Long> reference = new HashMap<>();
            for (int i = 0; i < rows; i++) {
                // Non-uniform distribution so counts differ per key.
                keys[i] = (long) (i * i) % 100;
                reference.merge(keys[i], 1L, Long::sum);
            }

            TermsCapturingSink downstream = new TermsCapturingSink();
            runDatafusionTermsAgg(alloc, runtimeHandle, keys, downstream, "q-correctness", false);

            assertEquals("distinct group count", reference.size(), downstream.counts.size());
            for (Map.Entry<Long, Long> e : reference.entrySet()) {
                assertEquals("count for key " + e.getKey(), e.getValue(), downstream.counts.get(e.getKey()));
            }

            // [3b] bulk-copy batch builder must produce batches identical to the setSafe builder.
            Schema inputSchema = new Schema(List.of(new Field("key", FieldType.nullable(new ArrowType.Int(64, true)), null)));
            for (int offset = 0; offset < rows; offset += BATCH_SIZE) {
                int len = Math.min(BATCH_SIZE, rows - offset);
                try (
                    VectorSchemaRoot loop = makeBatch(alloc, inputSchema, keys, offset, len);
                    VectorSchemaRoot bulk = makeBatchBulk(alloc, inputSchema, keys, offset, len)
                ) {
                    BigIntVector loopCol = (BigIntVector) loop.getVector(0);
                    BigIntVector bulkCol = (BigIntVector) bulk.getVector(0);
                    assertEquals("bulk row count", loop.getRowCount(), bulk.getRowCount());
                    for (int i = 0; i < len; i++) {
                        assertFalse("bulk batch value must be non-null at " + (offset + i), bulkCol.isNull(i));
                        assertEquals("bulk batch value at " + (offset + i), loopCol.get(i), bulkCol.get(i));
                    }
                }
            }
        } finally {
            runtimeHandle.close();
        }
    }

    // ---------------------------------------------------------------
    // Benchmark: 20M rows, low-card (100 keys) and high-card (2M keys)
    // ---------------------------------------------------------------
    public void testShardTermsAggBenchmark() throws Exception {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("df-terms-bench-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(1024L * 1024 * 1024, 0L, spillDir.toString(), 512L * 1024 * 1024);
        assertTrue("runtime ptr non-zero", runtimePtr != 0);
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);

        final int rows = 20_000_000;
        final int warmup = 2;
        final int measured = 5;

        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            StringBuilder table = new StringBuilder();
            table.append(
                String.format(
                    Locale.ROOT,
                    "%n=== Shard terms agg POC: %,d rows, batch=%d, %d warmup + %d measured, median reported ===%n",
                    rows,
                    BATCH_SIZE,
                    warmup,
                    measured
                )
            );
            table.append(
                String.format(
                    Locale.ROOT,
                    "%-18s %14s %12s %12s %14s %16s %15s%n",
                    "distribution",
                    "df_total(ms)",
                    "df_feed(ms)",
                    "df_agg(ms)",
                    "java_hash(ms)",
                    "batch_build(ms)",
                    "bulk_build(ms)"
                )
            );

            for (String dist : new String[] { "low-card(100)", "high-card(2M)" }) {
                int cardinality = dist.startsWith("low") ? 100 : 2_000_000;
                long[] keys = new long[rows];
                for (int i = 0; i < rows; i++) {
                    keys[i] = i % cardinality;
                }

                long[] dfFeedMs = new long[measured];
                long[] dfAggMs = new long[measured];
                long[] buildMs = new long[measured];
                long[] bulkBuildMs = new long[measured];
                long[] javaMs = new long[measured];

                for (int iter = 0; iter < warmup + measured; iter++) {
                    // [1] + [3] DataFusion path — batches must be rebuilt every iteration because
                    // Data.exportVectorSchemaRoot hands buffer ownership to Rust; build time is
                    // clocked separately inside the feed loop for attribution.
                    TermsCapturingSink downstream = new TermsCapturingSink();
                    long[] timings = runDatafusionTermsAgg(alloc, runtimeHandle, keys, downstream, "q-bench-" + dist + "-" + iter, false);
                    assertEquals("distinct groups (" + dist + ")", cardinality, downstream.counts.size());
                    assertEquals("sum of counts == rows (" + dist + ")", rows, downstream.totalCount);

                    // [3b] Same DF pass with bulk-copied batches (models native doc-values decode
                    // writing straight into the Arrow data buffer). Reuses build slot for timing.
                    TermsCapturingSink bulkDownstream = new TermsCapturingSink();
                    long[] bulkTimings = runDatafusionTermsAgg(
                        alloc,
                        runtimeHandle,
                        keys,
                        bulkDownstream,
                        "q-bench-bulk-" + dist + "-" + iter,
                        true
                    );
                    assertEquals("bulk distinct groups (" + dist + ")", cardinality, bulkDownstream.counts.size());
                    assertEquals("bulk sum of counts == rows (" + dist + ")", rows, bulkDownstream.totalCount);

                    // [2] Java baseline: ReorganizingLongHash over the same long[] — mirrors
                    // StreamNumericTermsAggregator's shard-level group-by cost.
                    long javaStart = System.nanoTime();
                    long distinct;
                    long[] counts = new long[cardinality];
                    try (ReorganizingLongHash hash = new ReorganizingLongHash(BigArrays.NON_RECYCLING_INSTANCE)) {
                        for (long key : keys) {
                            long ord = hash.add(key);
                            if (ord < 0) {
                                ord = -1 - ord;
                            }
                            counts[(int) ord]++;
                        }
                        distinct = hash.size();
                    }
                    long javaNanos = System.nanoTime() - javaStart;
                    assertEquals("java baseline distinct groups (" + dist + ")", cardinality, distinct);

                    if (iter >= warmup) {
                        int m = iter - warmup;
                        buildMs[m] = TimeUnit.NANOSECONDS.toMillis(timings[0]);
                        dfFeedMs[m] = TimeUnit.NANOSECONDS.toMillis(timings[1]);
                        dfAggMs[m] = TimeUnit.NANOSECONDS.toMillis(timings[2]);
                        bulkBuildMs[m] = TimeUnit.NANOSECONDS.toMillis(bulkTimings[0]);
                        javaMs[m] = TimeUnit.NANOSECONDS.toMillis(javaNanos);
                    }
                    System.out.printf(
                        Locale.ROOT,
                        "[iter %d%s] %s: build=%dms bulk_build=%dms feed=%dms agg+drain=%dms java=%dms%n",
                        iter,
                        iter < warmup ? " warmup" : "",
                        dist,
                        TimeUnit.NANOSECONDS.toMillis(timings[0]),
                        TimeUnit.NANOSECONDS.toMillis(bulkTimings[0]),
                        TimeUnit.NANOSECONDS.toMillis(timings[1]),
                        TimeUnit.NANOSECONDS.toMillis(timings[2]),
                        TimeUnit.NANOSECONDS.toMillis(javaNanos)
                    );
                }

                long feed = median(dfFeedMs);
                long agg = median(dfAggMs);
                table.append(
                    String.format(
                        Locale.ROOT,
                        "%-18s %14d %12d %12d %14d %16d %15d%n",
                        dist,
                        feed + agg,
                        feed,
                        agg,
                        median(javaMs),
                        median(buildMs),
                        median(bulkBuildMs)
                    )
                );
            }

            table.append("df_total = feed + agg+drain (excludes batch construction, reported separately)\n");
            table.append("batch_build = per-value setSafe [3]; bulk_build = MemorySegment copy + validity memset [3b]\n");
            System.out.print(table);
        } finally {
            runtimeHandle.close();
        }
    }

    /**
     * Feeds {@code keys} in {@link #BATCH_SIZE} batches through a fresh
     * {@link DatafusionMemtableReduceSink} running GROUP BY key + COUNT(*), draining into
     * {@code downstream}. Batches are built with per-value {@code setSafe} ([3]) or, when
     * {@code bulkBuild} is set, with the bulk-copy builder ([3b]). Returns
     * {@code [batchBuildNanos, feedNanos, reduceAndDrainNanos]}.
     */
    private static long[] runDatafusionTermsAgg(
        BufferAllocator alloc,
        NativeRuntimeHandle runtimeHandle,
        long[] keys,
        TermsCapturingSink downstream,
        String queryId,
        boolean bulkBuild
    ) throws Exception {
        Schema inputSchema = new Schema(List.of(new Field("key", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        byte[] aggPlan = buildGroupByCountSubstraitBytes(DatafusionMemtableReduceSink.INPUT_ID);
        byte[] producerPlan = buildPassthroughSubstraitBytes(DatafusionMemtableReduceSink.INPUT_ID);

        ExchangeSinkContext ctx = new ExchangeSinkContext(
            queryId,
            0,
            0L,
            aggPlan,
            alloc,
            List.of(new ExchangeSinkContext.ChildInput(0, producerPlan)),
            downstream
        );

        long buildNanos = 0;
        long feedNanos = 0;
        long reduceNanos;

        DatafusionMemtableReduceSink sink = new DatafusionMemtableReduceSink(ctx, runtimeHandle);
        try {
            for (int offset = 0; offset < keys.length; offset += BATCH_SIZE) {
                int len = Math.min(BATCH_SIZE, keys.length - offset);
                long t0 = System.nanoTime();
                VectorSchemaRoot batch = bulkBuild
                    ? makeBatchBulk(alloc, inputSchema, keys, offset, len)
                    : makeBatch(alloc, inputSchema, keys, offset, len);
                long t1 = System.nanoTime();
                sink.feed(batch);
                feedNanos += System.nanoTime() - t1;
                buildNanos += t1 - t0;
            }
            long t2 = System.nanoTime();
            PlainActionFuture<Void> reduceDone = PlainActionFuture.newFuture();
            sink.reduce(reduceDone);
            reduceDone.actionGet(120, TimeUnit.SECONDS);
            reduceNanos = System.nanoTime() - t2;
        } finally {
            sink.close();
        }
        return new long[] { buildNanos, feedNanos, reduceNanos };
    }

    /** GROUP BY key + COUNT(*) over the single stage input: SELECT key, COUNT(*) FROM "input-0" GROUP BY key. */
    private static byte[] buildGroupByCountSubstraitBytes(String inputId) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner hepPlanner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(hepPlanner, rexBuilder);

        RelDataType bigintNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        RelDataType rowType = typeFactory.builder().add("key", bigintNullable).build();

        RelNode scan = new DataFusionFragmentConvertor.StageInputTableScan(cluster, cluster.traitSet(), inputId, rowType);

        // COUNT(*) — zero-arg COUNT; return type BIGINT NOT NULL.
        AggregateCall countCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(countCall));

        return new DataFusionFragmentConvertor(loadExtensions()).convertFragment(agg);
    }

    private static byte[] buildPassthroughSubstraitBytes(String inputId) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner hepPlanner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(hepPlanner, rexBuilder);

        RelDataType bigintNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        RelDataType rowType = typeFactory.builder().add("key", bigintNullable).build();

        RelNode scan = new DataFusionFragmentConvertor.StageInputTableScan(cluster, cluster.traitSet(), inputId, rowType);

        return new DataFusionFragmentConvertor(loadExtensions()).convertFragment(scan);
    }

    private static SimpleExtension.ExtensionCollection loadExtensions() {
        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(DatafusionShardTermsAggBenchTests.class.getClassLoader());
            return DefaultExtensionCatalog.DEFAULT_COLLECTION;
        } finally {
            t.setContextClassLoader(prev);
        }
    }

    private static VectorSchemaRoot makeBatch(BufferAllocator alloc, Schema schema, long[] values, int offset, int len) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
        root.allocateNew();
        BigIntVector col = (BigIntVector) root.getVector(0);
        for (int i = 0; i < len; i++) {
            col.setSafe(i, values[offset + i]);
        }
        col.setValueCount(len);
        root.setRowCount(len);
        return root;
    }

    /**
     * [3b] Bulk-copy batch builder — mirrors the planned native doc-values decode: one
     * {@link MemorySegment#copyFrom} of the long[] slice into the vector's data buffer plus a
     * bulk 0xFF fill of the validity buffer, instead of per-value {@code setSafe}.
     */
    private static VectorSchemaRoot makeBatchBulk(BufferAllocator alloc, Schema schema, long[] values, int offset, int len) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
        BigIntVector col = (BigIntVector) root.getVector(0);
        col.allocateNew(len);
        long byteLen = (long) len * Long.BYTES;
        MemorySegment src = MemorySegment.ofArray(values).asSlice((long) offset * Long.BYTES, byteLen);
        MemorySegment dst = MemorySegment.ofAddress(col.getDataBuffer().memoryAddress()).reinterpret(byteLen);
        dst.copyFrom(src);
        // Dense validity: memset the bitmap to all-ones. Trailing bits past `len` are ignored.
        col.getValidityBuffer().setOne(0, (len + 7) / 8);
        col.setValueCount(len);
        root.setRowCount(len);
        return root;
    }

    private static long median(long[] values) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return sorted[sorted.length / 2];
    }

    /** Downstream sink capturing (key -> count) pairs from the drained GROUP BY output. */
    private static final class TermsCapturingSink implements ExchangeSink {
        final Map<Long, Long> counts = new HashMap<>();
        long totalCount;

        @Override
        public synchronized void feed(VectorSchemaRoot batch) {
            try {
                assertEquals("group-by output should have 2 columns (key, cnt)", 2, batch.getFieldVectors().size());
                BigIntVector keyCol = (BigIntVector) batch.getVector(0);
                BigIntVector cntCol = (BigIntVector) batch.getVector(1);
                int rows = batch.getRowCount();
                for (int i = 0; i < rows; i++) {
                    long cnt = cntCol.get(i);
                    counts.merge(keyCol.get(i), cnt, Long::sum);
                    totalCount += cnt;
                }
            } finally {
                batch.close();
            }
        }

        @Override
        public synchronized void close() {
            // Reduce sink does not own the downstream close in this POC.
        }
    }
}
