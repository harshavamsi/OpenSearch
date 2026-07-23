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
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;

/**
 * POC: shard results travel as Arrow batches, the coordinator reduce runs in DataFusion.
 *
 * <p>Stage A simulates N shards, each running GROUP BY key + COUNT(*) in its own DataFusion
 * session over raw {key} batches and emitting partial-aggregate {key, cnt} Arrow batches
 * (what would ride Flight to the coordinator). Stage B runs a coordinator session that
 * final-aggregates the union of those partial batches, two ways:
 * <ul>
 *   <li>variant 1 — explicit-final plan: GROUP BY key + SUM(cnt) over a memtable of the
 *       partial batches;</li>
 *   <li>variant 2 — agg_mode route: the SAME logical COUNT plan bytes on both tiers.
 *       {@code registerPartitionStream} lowers the plan and derives the partial-state schema,
 *       {@code prepareFinalPlan} strips the plan to its FINAL half, and the partial batches
 *       are pushed through the native sender while the output stream drains concurrently.</li>
 * </ul>
 *
 * <p>The benchmark test skips stage A and synthesizes partial outputs directly with a
 * bulk-copy batch builder, isolating pure coordinator-reduce cost against a Java
 * HashMap merge baseline at 48-shard fan-in.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class DatafusionPartialFinalReduceTests extends OpenSearchTestCase {

    private static final int RAW_BATCH_SIZE = 4096;
    private static final int PARTIAL_BATCH_SIZE = 8192;

    private static final Schema RAW_SCHEMA = new Schema(List.of(new Field("key", FieldType.nullable(new ArrowType.Int(64, true)), null)));
    private static final Schema PARTIAL_SCHEMA = new Schema(
        List.of(
            new Field("key", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("cnt", FieldType.nullable(new ArrowType.Int(64, true)), null)
        )
    );

    // ---------------------------------------------------------------
    // Correctness: 4 shards x 250k rows, ~5k-key skewed universe
    // ---------------------------------------------------------------
    public void testPartialFinalCorrectness() throws Exception {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("df-pf-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(512L * 1024 * 1024, 0L, spillDir.toString(), 256L * 1024 * 1024);
        assertTrue("runtime ptr non-zero", runtimePtr != 0);
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);

        final int shards = 4;
        final int rowsPerShard = 250_000;

        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Map<Long, Long> reference = new HashMap<>();
            List<VectorSchemaRoot> retainedPartials = new ArrayList<>();
            try {
                // ---- Stage A: per-shard partial aggregation, retain drained partial batches ----
                for (int shard = 0; shard < shards; shard++) {
                    long[] keys = new long[rowsPerShard];
                    for (int i = 0; i < rowsPerShard; i++) {
                        // Skewed + overlapping: half the rows hit 50 hot keys, half spread over 5k.
                        long r = (i * 2654435761L + shard * 97L) >>> 16;
                        keys[i] = (i % 2 == 0) ? r % 50 : r % 5000;
                        reference.merge(keys[i], 1L, Long::sum);
                    }
                    RetainingSink retainer = new RetainingSink(alloc, retainedPartials);
                    byte[] countPlan = buildGroupByCountSubstraitBytes(RAW_ROW_TYPE);
                    byte[] rawPassthrough = buildPassthroughSubstraitBytes(RAW_ROW_TYPE);
                    ExchangeSinkContext ctx = new ExchangeSinkContext(
                        "q-pf-shard-" + shard,
                        0,
                        0L,
                        countPlan,
                        alloc,
                        List.of(new ExchangeSinkContext.ChildInput(0, rawPassthrough)),
                        retainer
                    );
                    DatafusionMemtableReduceSink sink = new DatafusionMemtableReduceSink(ctx, runtimeHandle);
                    try {
                        for (int offset = 0; offset < rowsPerShard; offset += RAW_BATCH_SIZE) {
                            int len = Math.min(RAW_BATCH_SIZE, rowsPerShard - offset);
                            sink.feed(makeRawBatchBulk(alloc, keys, offset, len));
                        }
                        PlainActionFuture<Void> done = PlainActionFuture.newFuture();
                        sink.reduce(done);
                        done.actionGet(120, TimeUnit.SECONDS);
                    } finally {
                        sink.close();
                    }
                }
                long retainedRows = retainedPartials.stream().mapToLong(VectorSchemaRoot::getRowCount).sum();
                System.out.printf(
                    Locale.ROOT,
                    "[stage A] %d shards produced %d partial batches / %d partial rows (reference groups=%d)%n",
                    shards,
                    retainedPartials.size(),
                    retainedRows,
                    reference.size()
                );

                // ---- Stage B variant 1: explicit-final plan (GROUP BY key + SUM(cnt), memtable) ----
                Map<Long, Long> variant1 = runExplicitFinalMemtableReduce(alloc, runtimeHandle, retainedPartials, "q-pf-final-v1");
                assertEquals("variant1 group count", reference.size(), variant1.size());
                for (Map.Entry<Long, Long> e : reference.entrySet()) {
                    assertEquals("variant1 count for key " + e.getKey(), e.getValue(), variant1.get(e.getKey()));
                }
                System.out.printf(
                    Locale.ROOT,
                    "[stage B v1] explicit-final SUM(cnt) matches Java reference (%d groups)%n",
                    variant1.size()
                );

                // ---- Stage B variant 2: agg_mode route (same COUNT plan bytes on both tiers) ----
                byte[] countPlan = buildGroupByCountSubstraitBytes(RAW_ROW_TYPE);
                try {
                    Map<Long, Long> variant2 = runAggModeStreamingReduce(alloc, runtimeHandle, retainedPartials, countPlan, countPlan);
                    assertEquals("variant2 group count", variant1.size(), variant2.size());
                    for (Map.Entry<Long, Long> e : variant1.entrySet()) {
                        assertEquals("variant2 count for key " + e.getKey(), e.getValue(), variant2.get(e.getKey()));
                    }
                    System.out.printf(Locale.ROOT, "[stage B v2] agg_mode route matches variant 1 (%d groups)%n", variant2.size());
                } catch (Throwable t) {
                    // Feasibility datapoint only — variant 1 carries the benchmark. Surface the exact error.
                    System.out.printf(Locale.ROOT, "[stage B v2] agg_mode same-bytes route FAILED: %s%n", t);
                }

                // ---- Stage B variant 2b: prepareFinalPlan stripping with a final plan whose
                // aggregate references the state column (SUM(cnt) over the partial read) ----
                try {
                    Map<Long, Long> variant2b = runAggModeStreamingReduce(
                        alloc,
                        runtimeHandle,
                        retainedPartials,
                        countPlan,
                        buildGroupBySumSubstraitBytes()
                    );
                    assertEquals("variant2b group count", variant1.size(), variant2b.size());
                    for (Map.Entry<Long, Long> e : variant1.entrySet()) {
                        assertEquals("variant2b count for key " + e.getKey(), e.getValue(), variant2b.get(e.getKey()));
                    }
                    System.out.printf(
                        Locale.ROOT,
                        "[stage B v2b] prepareFinalPlan(SUM) + streaming feed matches variant 1 (%d groups)%n",
                        variant2b.size()
                    );
                } catch (Throwable t) {
                    System.out.printf(Locale.ROOT, "[stage B v2b] prepareFinalPlan(SUM) route FAILED: %s%n", t);
                }
            } finally {
                for (VectorSchemaRoot root : retainedPartials) {
                    root.close();
                }
            }
        } finally {
            runtimeHandle.close();
        }
    }

    // ---------------------------------------------------------------
    // Benchmark: coordinator reduce at 48-shard fan-in, partial batches synthesized
    // ---------------------------------------------------------------
    public void testCoordinatorReduceBenchmark() throws Exception {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("df-pf-bench-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(2048L * 1024 * 1024, 0L, spillDir.toString(), 512L * 1024 * 1024);
        assertTrue("runtime ptr non-zero", runtimePtr != 0);
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);

        final int shards = 48;
        final int warmup = 2;
        final int measured = 5;

        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            StringBuilder table = new StringBuilder();
            table.append(
                String.format(
                    Locale.ROOT,
                    "%n=== Coordinator reduce POC: %d shards, partial {key,cnt} batches of %d rows, %d warmup + %d measured, median (ms) ===%n",
                    shards,
                    PARTIAL_BATCH_SIZE,
                    warmup,
                    measured
                )
            );
            table.append(
                String.format(
                    Locale.ROOT,
                    "%-14s %14s %12s %12s %15s %15s %15s%n",
                    "shape",
                    "df_total",
                    "df_feed",
                    "df_agg",
                    "df_stream",
                    "java_hash",
                    "batch_build"
                )
            );

            for (String shape : new String[] { "wide", "topN-like" }) {
                boolean wide = shape.equals("wide");
                int rowsPerShard = wide ? 100_000 : 10_000;
                int stride = wide ? 20 : 2;

                // Per-shard partial outputs: distinct keys within a shard (stride residue classes),
                // union across shards covers stride * rowsPerShard final groups.
                long[][] shardKeys = new long[shards][];
                long[][] shardCnts = new long[shards][];
                long expectedTotal = 0;
                for (int s = 0; s < shards; s++) {
                    long[] keys = new long[rowsPerShard];
                    long[] cnts = new long[rowsPerShard];
                    int residue = (s * 7) % stride;
                    for (int i = 0; i < rowsPerShard; i++) {
                        keys[i] = (long) i * stride + residue;
                        cnts[i] = 1 + ((i + s) % 100);
                        expectedTotal += cnts[i];
                    }
                    shardKeys[s] = keys;
                    shardCnts[s] = cnts;
                }
                int expectedGroups = rowsPerShard * stride;

                byte[] sumPlan = buildGroupBySumSubstraitBytes();
                byte[] partialPassthrough = buildPassthroughSubstraitBytes(PARTIAL_ROW_TYPE);

                long[] buildMs = new long[measured];
                long[] feedMs = new long[measured];
                long[] aggMs = new long[measured];
                long[] streamMs = new long[measured];
                long[] javaMs = new long[measured];

                for (int iter = 0; iter < warmup + measured; iter++) {
                    // [3] + [1] DF memtable reduce (feed = C-Data export, agg = execute + drain).
                    long t0 = System.nanoTime();
                    List<VectorSchemaRoot> dfBatches = buildPartialBatches(alloc, shardKeys, shardCnts);
                    long t1 = System.nanoTime();
                    SummingSink downstream = new SummingSink();
                    ExchangeSinkContext ctx = new ExchangeSinkContext(
                        "q-pf-bench-" + shape + "-" + iter,
                        0,
                        0L,
                        sumPlan,
                        alloc,
                        List.of(new ExchangeSinkContext.ChildInput(0, partialPassthrough)),
                        downstream
                    );
                    DatafusionMemtableReduceSink sink = new DatafusionMemtableReduceSink(ctx, runtimeHandle);
                    long t2, t3;
                    try {
                        for (VectorSchemaRoot batch : dfBatches) {
                            sink.feed(batch);
                        }
                        t2 = System.nanoTime();
                        PlainActionFuture<Void> done = PlainActionFuture.newFuture();
                        sink.reduce(done);
                        done.actionGet(300, TimeUnit.SECONDS);
                        t3 = System.nanoTime();
                    } finally {
                        sink.close();
                    }
                    assertEquals("df memtable group count (" + shape + ")", expectedGroups, downstream.rows);
                    assertEquals("df memtable total (" + shape + ")", expectedTotal, downstream.sum);

                    // [1s] DF partition-stream reduce: concurrent feed thread + inline drain.
                    long streamNanos = runStreamingReduceTimed(
                        alloc,
                        runtimeHandle,
                        sumPlan,
                        partialPassthrough,
                        shardKeys,
                        shardCnts,
                        expectedGroups,
                        expectedTotal,
                        "q-pf-bench-stream-" + shape + "-" + iter
                    );

                    // [2] Java baseline: HashMap<Long,Long> merge reading Arrow data buffers directly
                    // (approximates the ColumnarTermsFolder Java fold). Map presized to final cardinality.
                    List<VectorSchemaRoot> javaBatches = buildPartialBatches(alloc, shardKeys, shardCnts);
                    long t4 = System.nanoTime();
                    Map<Long, Long> merged = new HashMap<>((int) (expectedGroups / 0.75f) + 1);
                    for (VectorSchemaRoot batch : javaBatches) {
                        int rows = batch.getRowCount();
                        var keyBuf = ((BigIntVector) batch.getVector(0)).getDataBuffer();
                        var cntBuf = ((BigIntVector) batch.getVector(1)).getDataBuffer();
                        for (int i = 0; i < rows; i++) {
                            merged.merge(keyBuf.getLong((long) i * Long.BYTES), cntBuf.getLong((long) i * Long.BYTES), Long::sum);
                        }
                    }
                    long javaNanos = System.nanoTime() - t4;
                    for (VectorSchemaRoot batch : javaBatches) {
                        batch.close();
                    }
                    assertEquals("java merge group count (" + shape + ")", expectedGroups, merged.size());

                    if (iter >= warmup) {
                        int m = iter - warmup;
                        buildMs[m] = TimeUnit.NANOSECONDS.toMillis(t1 - t0);
                        feedMs[m] = TimeUnit.NANOSECONDS.toMillis(t2 - t1);
                        aggMs[m] = TimeUnit.NANOSECONDS.toMillis(t3 - t2);
                        streamMs[m] = TimeUnit.NANOSECONDS.toMillis(streamNanos);
                        javaMs[m] = TimeUnit.NANOSECONDS.toMillis(javaNanos);
                    }
                    System.out.printf(
                        Locale.ROOT,
                        "[iter %d%s] %s: build=%dms feed=%dms agg+drain=%dms stream=%dms java=%dms%n",
                        iter,
                        iter < warmup ? " warmup" : "",
                        shape,
                        TimeUnit.NANOSECONDS.toMillis(t1 - t0),
                        TimeUnit.NANOSECONDS.toMillis(t2 - t1),
                        TimeUnit.NANOSECONDS.toMillis(t3 - t2),
                        TimeUnit.NANOSECONDS.toMillis(streamNanos),
                        TimeUnit.NANOSECONDS.toMillis(javaNanos)
                    );
                }

                long feed = median(feedMs);
                long agg = median(aggMs);
                table.append(
                    String.format(
                        Locale.ROOT,
                        "%-14s %14d %12d %12d %15d %15d %15d%n",
                        shape,
                        feed + agg,
                        feed,
                        agg,
                        median(streamMs),
                        median(javaMs),
                        median(buildMs)
                    )
                );
            }
            table.append("df_total = feed (C-Data export) + agg+drain, memtable path. df_stream = partition-stream path\n");
            table.append("(sink setup + concurrent feed thread + drain), end-to-end. java_hash = HashMap<Long,Long> merge over\n");
            table.append("the same batches via getDataBuffer().getLong (presized to final cardinality). batch_build = bulk-copy\n");
            table.append("builder cost, reported separately (real coordinator receives batches from Flight instead).\n");
            System.out.print(table);
        } finally {
            runtimeHandle.close();
        }
    }

    // ---------------------------------------------------------------
    // Stage B variant 1: memtable + explicit GROUP BY key, SUM(cnt)
    // ---------------------------------------------------------------
    private static Map<Long, Long> runExplicitFinalMemtableReduce(
        BufferAllocator alloc,
        NativeRuntimeHandle runtimeHandle,
        List<VectorSchemaRoot> partials,
        String queryId
    ) {
        CountingMapSink downstream = new CountingMapSink();
        ExchangeSinkContext ctx = new ExchangeSinkContext(
            queryId,
            0,
            0L,
            buildGroupBySumSubstraitBytes(),
            alloc,
            List.of(new ExchangeSinkContext.ChildInput(0, buildPassthroughSubstraitBytes(PARTIAL_ROW_TYPE))),
            downstream
        );
        DatafusionMemtableReduceSink sink = new DatafusionMemtableReduceSink(ctx, runtimeHandle);
        try {
            for (VectorSchemaRoot partial : partials) {
                // feed() consumes the batch (C-Data export hands buffers to Rust) — feed a copy
                // so the retained originals survive for variant 2.
                sink.feed(copyPartialBatch(alloc, partial));
            }
            PlainActionFuture<Void> done = PlainActionFuture.newFuture();
            sink.reduce(done);
            done.actionGet(120, TimeUnit.SECONDS);
        } finally {
            sink.close();
        }
        return downstream.counts;
    }

    // ---------------------------------------------------------------
    // Stage B variant 2: registerPartitionStream + prepareFinalPlan + executeLocalPreparedPlan,
    // same COUNT plan bytes on both tiers; feed thread concurrent with drain.
    // ---------------------------------------------------------------
    private static Map<Long, Long> runAggModeStreamingReduce(
        BufferAllocator alloc,
        NativeRuntimeHandle runtimeHandle,
        List<VectorSchemaRoot> partials,
        byte[] producerPlan,
        byte[] finalPlan
    ) throws Exception {
        DatafusionLocalSession session = new DatafusionLocalSession(runtimeHandle.get());
        DatafusionPartitionSender sender = null;
        StreamHandle outStream = null;
        try {
            // Lowering the producer plan derives the partial-state schema for the input table.
            NativeBridge.RegisteredInput registered = NativeBridge.registerPartitionStream(session.getPointer(), "input-0", producerPlan);
            sender = new DatafusionPartitionSender(registered.pointer());
            Schema declared = ArrowSchemaIpc.fromBytes(registered.schemaIpc());
            System.out.printf(Locale.ROOT, "[stage B v2/v2b] partial-state schema from registerPartitionStream: %s%n", declared);

            NativeBridge.prepareFinalPlan(session.getPointer(), finalPlan);
            long streamPtr = NativeBridge.executeLocalPreparedPlan(session.getPointer(), 0L);
            outStream = new StreamHandle(streamPtr, runtimeHandle);

            // senderSend blocks on a cap-4 channel — feed from a separate thread while we drain.
            final DatafusionPartitionSender feedSender = sender;
            AtomicReference<Throwable> feedFailure = new AtomicReference<>();
            Thread feeder = new Thread(() -> {
                try {
                    for (VectorSchemaRoot partial : partials) {
                        try (VectorSchemaRoot copy = copyPartialBatch(alloc, partial)) {
                            ArrowArray array = ArrowArray.allocateNew(alloc);
                            ArrowSchema arrowSchema = ArrowSchema.allocateNew(alloc);
                            try {
                                Data.exportVectorSchemaRoot(alloc, copy, null, array, arrowSchema);
                                long rc = feedSender.send(array.memoryAddress(), arrowSchema.memoryAddress());
                                if (rc == NativeBridge.SENDER_SEND_RECEIVER_DROPPED) {
                                    return;
                                }
                            } finally {
                                // Success path: Rust consumed the FFI structs, wrapper close is a no-op.
                                array.close();
                                arrowSchema.close();
                            }
                        }
                    }
                } catch (Throwable t) {
                    feedFailure.set(t);
                } finally {
                    feedSender.close();
                }
            }, "df-pf-v2-feeder");
            feeder.start();

            Map<Long, Long> counts = new HashMap<>();
            try (CDataDictionaryProvider dictProvider = new CDataDictionaryProvider()) {
                DatafusionResultStream.BatchIterator it = new DatafusionResultStream.BatchIterator(outStream, alloc, dictProvider);
                while (it.hasNext()) {
                    try (VectorSchemaRoot batch = it.next().getArrowRoot()) {
                        BigIntVector keyCol = (BigIntVector) batch.getVector(0);
                        BigIntVector cntCol = (BigIntVector) batch.getVector(1);
                        for (int i = 0; i < batch.getRowCount(); i++) {
                            counts.merge(keyCol.get(i), cntCol.get(i), Long::sum);
                        }
                    }
                }
            }
            feeder.join(TimeUnit.SECONDS.toMillis(120));
            if (feedFailure.get() != null) {
                throw new IllegalStateException("variant 2 feed thread failed", feedFailure.get());
            }
            return counts;
        } finally {
            if (outStream != null) {
                outStream.close();
            }
            if (sender != null) {
                try {
                    sender.close();
                } catch (Exception ignored) {
                    // Feeder already closed it; NativeHandle close is idempotent but be safe.
                }
            }
            session.close();
        }
    }

    // ---------------------------------------------------------------
    // [1s] Streaming (partition-stream) reduce with the explicit-final SUM plan, timed.
    // ---------------------------------------------------------------
    private static long runStreamingReduceTimed(
        BufferAllocator alloc,
        NativeRuntimeHandle runtimeHandle,
        byte[] sumPlan,
        byte[] partialPassthrough,
        long[][] shardKeys,
        long[][] shardCnts,
        int expectedGroups,
        long expectedTotal,
        String queryId
    ) throws Exception {
        List<VectorSchemaRoot> batches = buildPartialBatches(alloc, shardKeys, shardCnts);
        SummingSink downstream = new SummingSink();
        ExchangeSinkContext ctx = new ExchangeSinkContext(
            queryId,
            0,
            0L,
            sumPlan,
            alloc,
            List.of(new ExchangeSinkContext.ChildInput(0, partialPassthrough)),
            downstream
        );
        long t0 = System.nanoTime();
        DatafusionReduceSink sink = new DatafusionReduceSink(ctx, runtimeHandle);
        long elapsed;
        try {
            PlainActionFuture<Void> done = PlainActionFuture.newFuture();
            // Drain runs inline in reduce(); run it on a helper thread so this thread can feed
            // through the bounded native channel concurrently.
            Thread drainer = new Thread(() -> sink.reduce(done), "df-pf-stream-drainer");
            drainer.start();
            ExchangeSink child = sink.sinkForChild(0);
            for (VectorSchemaRoot batch : batches) {
                child.feed(batch);
            }
            child.close();
            done.actionGet(300, TimeUnit.SECONDS);
            drainer.join(TimeUnit.SECONDS.toMillis(30));
            elapsed = System.nanoTime() - t0;
        } finally {
            sink.close();
        }
        assertEquals("df stream group count", expectedGroups, downstream.rows);
        assertEquals("df stream total", expectedTotal, downstream.sum);
        return elapsed;
    }

    // ---------------------------------------------------------------
    // Substrait plan builders (Calcite -> substrait, recipe from DatafusionShardTermsAggBenchTests)
    // ---------------------------------------------------------------
    private interface RowTypeBuilder {
        RelDataType build(RelDataTypeFactory typeFactory);
    }

    private static final RowTypeBuilder RAW_ROW_TYPE = tf -> {
        RelDataType bigint = tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.BIGINT), true);
        return tf.builder().add("key", bigint).build();
    };

    private static final RowTypeBuilder PARTIAL_ROW_TYPE = tf -> {
        RelDataType bigint = tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.BIGINT), true);
        return tf.builder().add("key", bigint).add("cnt", bigint).build();
    };

    /** SELECT key, COUNT(*) AS cnt FROM "input-0" GROUP BY key — input schema from {@code rowType}. */
    private static byte[] buildGroupByCountSubstraitBytes(RowTypeBuilder rowType) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RelOptCluster cluster = newCluster(typeFactory);
        RelNode scan = new DataFusionFragmentConvertor.StageInputTableScan(
            cluster,
            cluster.traitSet(),
            DatafusionMemtableReduceSink.INPUT_ID,
            rowType.build(typeFactory)
        );
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

    /** SELECT key, SUM(cnt) AS total FROM "input-0" GROUP BY key — over the partial {key,cnt} input. */
    private static byte[] buildGroupBySumSubstraitBytes() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RelOptCluster cluster = newCluster(typeFactory);
        RelNode scan = new DataFusionFragmentConvertor.StageInputTableScan(
            cluster,
            cluster.traitSet(),
            DatafusionMemtableReduceSink.INPUT_ID,
            PARTIAL_ROW_TYPE.build(typeFactory)
        );
        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true),
            "total"
        );
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(sumCall));
        return new DataFusionFragmentConvertor(loadExtensions()).convertFragment(agg);
    }

    /** Passthrough scan of "input-0" with the given row type (producer-side plan for schema derivation). */
    private static byte[] buildPassthroughSubstraitBytes(RowTypeBuilder rowType) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RelOptCluster cluster = newCluster(typeFactory);
        RelNode scan = new DataFusionFragmentConvertor.StageInputTableScan(
            cluster,
            cluster.traitSet(),
            DatafusionMemtableReduceSink.INPUT_ID,
            rowType.build(typeFactory)
        );
        return new DataFusionFragmentConvertor(loadExtensions()).convertFragment(scan);
    }

    private static RelOptCluster newCluster(RelDataTypeFactory typeFactory) {
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner hepPlanner = new HepPlanner(new HepProgramBuilder().build());
        return RelOptCluster.create(hepPlanner, rexBuilder);
    }

    private static SimpleExtension.ExtensionCollection loadExtensions() {
        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(DatafusionPartialFinalReduceTests.class.getClassLoader());
            return DefaultExtensionCatalog.DEFAULT_COLLECTION;
        } finally {
            t.setContextClassLoader(prev);
        }
    }

    // ---------------------------------------------------------------
    // Batch builders (bulk-copy: MemorySegment copy + validity memset)
    // ---------------------------------------------------------------
    private static VectorSchemaRoot makeRawBatchBulk(BufferAllocator alloc, long[] values, int offset, int len) {
        VectorSchemaRoot root = VectorSchemaRoot.create(RAW_SCHEMA, alloc);
        fillBigIntBulk((BigIntVector) root.getVector(0), values, offset, len);
        root.setRowCount(len);
        return root;
    }

    private static VectorSchemaRoot makePartialBatchBulk(BufferAllocator alloc, long[] keys, long[] cnts, int offset, int len) {
        VectorSchemaRoot root = VectorSchemaRoot.create(PARTIAL_SCHEMA, alloc);
        fillBigIntBulk((BigIntVector) root.getVector(0), keys, offset, len);
        fillBigIntBulk((BigIntVector) root.getVector(1), cnts, offset, len);
        root.setRowCount(len);
        return root;
    }

    private static void fillBigIntBulk(BigIntVector col, long[] values, int offset, int len) {
        col.allocateNew(len);
        if (len > 0) {
            long byteLen = (long) len * Long.BYTES;
            MemorySegment src = MemorySegment.ofArray(values).asSlice((long) offset * Long.BYTES, byteLen);
            MemorySegment dst = MemorySegment.ofAddress(col.getDataBuffer().memoryAddress()).reinterpret(byteLen);
            dst.copyFrom(src);
            col.getValidityBuffer().setOne(0, (len + 7) / 8);
        }
        col.setValueCount(len);
    }

    private static List<VectorSchemaRoot> buildPartialBatches(BufferAllocator alloc, long[][] shardKeys, long[][] shardCnts) {
        List<VectorSchemaRoot> batches = new ArrayList<>();
        for (int s = 0; s < shardKeys.length; s++) {
            long[] keys = shardKeys[s];
            long[] cnts = shardCnts[s];
            for (int offset = 0; offset < keys.length; offset += PARTIAL_BATCH_SIZE) {
                int len = Math.min(PARTIAL_BATCH_SIZE, keys.length - offset);
                batches.add(makePartialBatchBulk(alloc, keys, cnts, offset, len));
            }
        }
        return batches;
    }

    /**
     * Deep-copies a two-BigInt-column batch into a fresh root under {@code alloc}. Values are
     * assumed dense (no nulls) — grouped-aggregate output never has null keys/counts here.
     * This mirrors what the real coordinator must do with Flight-arrived batches it wants to
     * retain past the stream lifecycle.
     */
    private static VectorSchemaRoot copyPartialBatch(BufferAllocator alloc, VectorSchemaRoot src) {
        int rows = src.getRowCount();
        VectorSchemaRoot dst = VectorSchemaRoot.create(PARTIAL_SCHEMA, alloc);
        for (int c = 0; c < 2; c++) {
            BigIntVector from = (BigIntVector) src.getVector(c);
            BigIntVector to = (BigIntVector) dst.getVector(c);
            to.allocateNew(rows);
            if (rows > 0) {
                long byteLen = (long) rows * Long.BYTES;
                MemorySegment s = MemorySegment.ofAddress(from.getDataBuffer().memoryAddress()).reinterpret(byteLen);
                MemorySegment d = MemorySegment.ofAddress(to.getDataBuffer().memoryAddress()).reinterpret(byteLen);
                d.copyFrom(s);
                to.getValidityBuffer().setOne(0, (rows + 7) / 8);
            }
            to.setValueCount(rows);
        }
        dst.setRowCount(rows);
        return dst;
    }

    private static long median(long[] values) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return sorted[sorted.length / 2];
    }

    // ---------------------------------------------------------------
    // Downstream sinks
    // ---------------------------------------------------------------

    /** Retains a deep copy of every drained partial batch (stage A downstream). */
    private static final class RetainingSink implements ExchangeSink {
        private final BufferAllocator alloc;
        private final List<VectorSchemaRoot> retained;

        RetainingSink(BufferAllocator alloc, List<VectorSchemaRoot> retained) {
            this.alloc = alloc;
            this.retained = retained;
        }

        @Override
        public synchronized void feed(VectorSchemaRoot batch) {
            try {
                if (batch.getRowCount() > 0) {
                    retained.add(copyPartialBatch(alloc, batch));
                }
            } finally {
                batch.close();
            }
        }

        @Override
        public void close() {}
    }

    /** Captures (key -> total) pairs — used where the map itself is the assertion target. */
    private static final class CountingMapSink implements ExchangeSink {
        final Map<Long, Long> counts = new HashMap<>();

        @Override
        public synchronized void feed(VectorSchemaRoot batch) {
            try {
                BigIntVector keyCol = (BigIntVector) batch.getVector(0);
                BigIntVector cntCol = (BigIntVector) batch.getVector(1);
                for (int i = 0; i < batch.getRowCount(); i++) {
                    counts.merge(keyCol.get(i), cntCol.get(i), Long::sum);
                }
            } finally {
                batch.close();
            }
        }

        @Override
        public void close() {}
    }

    /**
     * O(1)-per-row downstream for the benchmark: tallies row count and value sum straight off
     * the data buffers so drain timing is not polluted by a Java hash merge.
     */
    private static final class SummingSink implements ExchangeSink {
        long rows;
        long sum;

        @Override
        public synchronized void feed(VectorSchemaRoot batch) {
            try {
                int n = batch.getRowCount();
                var cntBuf = ((BigIntVector) batch.getVector(1)).getDataBuffer();
                for (int i = 0; i < n; i++) {
                    sum += cntBuf.getLong((long) i * Long.BYTES);
                }
                rows += n;
            } finally {
                batch.close();
            }
        }

        @Override
        public void close() {}
    }
}
