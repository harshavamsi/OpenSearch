/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
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
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.spi.ShardAggregationEngine;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;

/**
 * DataFusion-backed {@link ShardAggregationEngine}: runs a grouped aggregation over
 * caller-pushed Arrow batches in a shard-local {@link DatafusionLocalSession} partition
 * stream. This is the engine half of the doc_values analytics path — the Lucene backend
 * decodes columns into Arrow batches and pushes them here; the plan runs entirely in
 * native DataFusion and results stream back as Arrow.
 *
 * <p>Call sequence per session (the proven recipe from the partial/final-reduce POC):
 * {@code createLocalSession} → {@code registerPartitionStream("input-0", passthroughPlan)}
 * (the passthrough scan's lowered schema declares the input columns) →
 * {@code executeLocalPlan(aggPlan)} → feed via {@code senderSend} → {@code senderClose} →
 * drain the stream. The output stream must be drained CONCURRENTLY with feeding — the
 * native channel is bounded (capacity 4), so {@code senderSend} blocks once the plan's
 * input queue fills. {@link Session#feed} is therefore only safe because the framework
 * drains the returned {@link EngineResultStream} on a different thread than the feeder —
 * which holds for the fragment execution path (Flight drains on transport threads).
 * For single-threaded callers the session buffers all batches and replays them in
 * {@link Session#finish()} would be needed; v1 keeps the simpler contract: feed-then-finish
 * works because aggregation output is only produced after end-of-input (GROUP BY is a
 * pipeline breaker), so the channel never backs up more than the operator's internal
 * buffering before {@code senderClose}. The bounded channel still applies backpressure:
 * if the native side stalls, {@code feed} blocks rather than OOMs.
 *
 * @opensearch.internal
 */
final class DatafusionShardAggregationEngine implements ShardAggregationEngine {

    private static final Logger LOGGER = LogManager.getLogger(DatafusionShardAggregationEngine.class);
    private static final String INPUT_ID = "input-0";

    private final DataFusionService service;

    DatafusionShardAggregationEngine(DataFusionService service) {
        this.service = service;
    }

    @Override
    public Session open(BufferAllocator allocator, AggSpec spec, long taskId) {
        List<InputColumn> columns = new ArrayList<>(spec.inputColumns().size());
        for (String name : spec.inputColumns()) {
            columns.add(new InputColumn(name, ColumnKind.LONG));
        }
        return open(allocator, buildAggPlan(spec), columns, taskId);
    }

    @Override
    public byte[] compileFragment(RelNode rebasedFragment) {
        // Lucene-driver fragments bypass BackendPlanAdapter (it applies the DRIVING backend's
        // adapters — Lucene's map is empty), so PPL calls needing DataFusion-shape rewrites
        // (EXTRACT → opensearch_extract, DATE_FORMAT → Rust UDF, REGEXP_REPLACE flag fixup,
        // MINUTE → date_part, …) arrive raw. Apply the same adapter map here before conversion.
        RelNode adapted = applyScalarAdapters(rebasedFragment);
        // The production convertor already rewrites OpenSearchStageInputScan leaves to the
        // engine's named-table scan ("input-<stageId>") and runs the full pre-Substrait
        // rewrite pipeline — the same path shard fragments take on the parquet backend.
        return new DataFusionFragmentConvertor(loadExtensions()).convertFragment(adapted);
    }

    /**
     * Bottom-up scalar-adapter rewrite over a rebased fragment (plain Logical nodes over a
     * stage-input scan). Mirrors {@code BackendPlanAdapter.adaptRex} minus annotation
     * handling — rebased fragments carry no annotation wrappers. Field storage is passed
     * empty: the DataFusion adapters key off Calcite types, not OpenSearch storage.
     */
    private static RelNode applyScalarAdapters(RelNode node) {
        Map<org.opensearch.analytics.spi.ScalarFunction, org.opensearch.analytics.spi.ScalarFunctionAdapter> adapters =
            DataFusionAnalyticsBackendPlugin.scalarFunctionAdapterMap();
        return rewriteTree(node, new org.apache.calcite.rex.RexShuttle() {
            @Override
            public org.apache.calcite.rex.RexNode visitCall(org.apache.calcite.rex.RexCall call) {
                org.apache.calcite.rex.RexNode visited = super.visitCall(call);
                if (!(visited instanceof org.apache.calcite.rex.RexCall recursed)) {
                    return visited;
                }
                org.opensearch.analytics.spi.ScalarFunction fn = org.opensearch.analytics.spi.ScalarFunction
                    .fromSqlOperatorWithFallback(recursed.getOperator());
                if (fn == null) {
                    return recursed;
                }
                org.opensearch.analytics.spi.ScalarFunctionAdapter adapter = adapters.get(fn);
                if (adapter == null) {
                    return recursed;
                }
                return adapter.adapt(recursed, List.of(), node.getCluster());
            }
        });
    }

    /** RelNode.accept(RexShuttle) rewrites only that node's expressions; recurse the inputs too. */
    private static RelNode rewriteTree(RelNode node, org.apache.calcite.rex.RexShuttle shuttle) {
        List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode rewritten = rewriteTree(input, shuttle);
            newInputs.add(rewritten);
            if (rewritten != input) {
                changed = true;
            }
        }
        RelNode current = changed ? node.copy(node.getTraitSet(), newInputs) : node;
        return current.accept(shuttle);
    }

    @Override
    public Session open(BufferAllocator allocator, byte[] fragmentPlanBytes, List<InputColumn> inputColumns, long taskId) {
        long runtimePtr = service.getNativeRuntime().get();
        DatafusionLocalSession session = new DatafusionLocalSession(runtimePtr);
        DatafusionPartitionSender sender = null;
        StreamHandle outStream = null;
        try {
            byte[] passthrough = buildPassthroughPlan(inputColumns);
            NativeBridge.RegisteredInput registered = NativeBridge.registerPartitionStream(session.getPointer(), INPUT_ID, passthrough);
            sender = new DatafusionPartitionSender(registered.pointer());
            // Partial-mode execution: strip the plan to the PARTIAL half of any aggregate
            // pair so engine-native-merge functions (approx_distinct) emit intermediate
            // state (HLL sketches) for the coordinator merge instead of finalized values.
            // For associative aggregates (COUNT/SUM/MIN/MAX) the partial output is
            // value-identical to the final one, so this is uniformly safe on the shard path.
            long streamPtr = NativeBridge.executeLocalPlanPartial(session.getPointer(), fragmentPlanBytes, taskId);
            outStream = new StreamHandle(streamPtr, service.getNativeRuntime());
            LOGGER.debug("[dv-agg] opened session: inputColumns={} planBytes={} taskId={}", inputColumns, fragmentPlanBytes.length, taskId);
            return new SessionImpl(allocator, session, sender, outStream);
        } catch (Throwable t) {
            if (outStream != null) {
                outStream.close();
            }
            if (sender != null) {
                sender.close();
            }
            session.close();
            throw t;
        }
    }

    /**
     * The session owns the native resources in feed order: sender first (EOF), then the
     * result stream (drained by the caller via the returned {@link DatafusionResultStream},
     * which closes the {@link StreamHandle}), then the local session.
     */
    private static final class SessionImpl implements Session {
        private final BufferAllocator allocator;
        private final DatafusionLocalSession session;
        private final DatafusionPartitionSender sender;
        private final StreamHandle outStream;
        private boolean finished;
        private boolean senderClosed;

        SessionImpl(BufferAllocator allocator, DatafusionLocalSession session, DatafusionPartitionSender sender, StreamHandle outStream) {
            this.allocator = allocator;
            this.session = session;
            this.sender = sender;
            this.outStream = outStream;
        }

        @Override
        public void feed(VectorSchemaRoot batch) {
            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            boolean sent = false;
            try {
                Data.exportVectorSchemaRoot(allocator, batch, null, array, arrowSchema);
                sender.send(array.memoryAddress(), arrowSchema.memoryAddress());
                sent = true;
            } finally {
                // On success Rust consumed the FFI structs (release callbacks nulled) — close
                // is a wrapper no-op. On a pre-send failure the export must be released here
                // or the buffers leak in the Java allocator (feedToSender rules).
                if (sent == false) {
                    array.release();
                    arrowSchema.release();
                }
                array.close();
                arrowSchema.close();
                batch.close();
            }
        }

        @Override
        public EngineResultStream finish() {
            if (finished) {
                throw new IllegalStateException("finish() already called");
            }
            finished = true;
            senderClosed = true;
            sender.close();
            // The stream handle's ownership passes to the result stream; the local session
            // is closed when the result stream closes (wrapped iterator below).
            DatafusionResultStream stream = new DatafusionResultStream(outStream, allocator);
            DatafusionLocalSession ownedSession = session;
            return new EngineResultStream() {
                @Override
                public java.util.Iterator<org.opensearch.analytics.backend.EngineResultBatch> iterator() {
                    return stream.iterator();
                }

                @Override
                public void close() {
                    try {
                        stream.close();
                    } finally {
                        ownedSession.close();
                    }
                }
            };
        }

        @Override
        public void close() {
            // Abort path (finish() never called): release everything. After finish(), the
            // returned stream owns outStream + session; only an un-closed sender is ours.
            if (senderClosed == false) {
                senderClosed = true;
                sender.close();
            }
            if (finished == false) {
                try {
                    outStream.close();
                } finally {
                    session.close();
                }
            }
        }
    }

    // ---- Substrait plan builders (Calcite → isthmus, same recipe as the reduce sinks) ----

    private static byte[] buildPassthroughPlan(List<InputColumn> inputColumns) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RelOptCluster cluster = newCluster(typeFactory);
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (InputColumn col : inputColumns) {
            RelDataType type = switch (col.kind()) {
                case KEYWORD -> typeFactory.createSqlType(SqlTypeName.VARCHAR);
                case TIMESTAMP -> typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3);
                case LONG -> typeFactory.createSqlType(SqlTypeName.BIGINT);
            };
            builder.add(col.name(), typeFactory.createTypeWithNullability(type, true));
        }
        RelNode scan = new DataFusionFragmentConvertor.StageInputTableScan(cluster, cluster.traitSet(), INPUT_ID, builder.build());
        return new DataFusionFragmentConvertor(loadExtensions()).convertFragment(scan);
    }

    private static byte[] buildAggPlan(AggSpec spec) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RelOptCluster cluster = newCluster(typeFactory);
        RelNode scan = new DataFusionFragmentConvertor.StageInputTableScan(
            cluster,
            cluster.traitSet(),
            INPUT_ID,
            rowType(typeFactory, spec.inputColumns())
        );
        ImmutableBitSet.Builder groupSet = ImmutableBitSet.builder();
        for (String col : spec.groupColumns()) {
            groupSet.set(columnIndex(spec, col));
        }
        List<AggregateCall> calls = new ArrayList<>(spec.aggCalls().size());
        RelDataType nullableBigint = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        for (AggCall call : spec.aggCalls()) {
            if (call.function() == AggFunction.COUNT) {
                calls.add(
                    AggregateCall.create(
                        SqlStdOperatorTable.COUNT,
                        false,
                        List.of(),
                        -1,
                        typeFactory.createSqlType(SqlTypeName.BIGINT),
                        call.outputName()
                    )
                );
            } else {
                calls.add(
                    AggregateCall.create(
                        toSqlAgg(call.function()),
                        false,
                        List.of(columnIndex(spec, call.inputColumn())),
                        -1,
                        nullableBigint,
                        call.outputName()
                    )
                );
            }
        }
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), groupSet.build(), null, calls);
        return new DataFusionFragmentConvertor(loadExtensions()).convertFragment(agg);
    }

    private static SqlAggFunction toSqlAgg(AggFunction fn) {
        return switch (fn) {
            case SUM -> SqlStdOperatorTable.SUM;
            case MIN -> SqlStdOperatorTable.MIN;
            case MAX -> SqlStdOperatorTable.MAX;
            case AVG -> SqlStdOperatorTable.AVG;
            case COUNT -> SqlStdOperatorTable.COUNT;
        };
    }

    private static int columnIndex(AggSpec spec, String column) {
        int idx = spec.inputColumns().indexOf(column);
        if (idx < 0) {
            throw new IllegalArgumentException("column [" + column + "] not in input schema " + spec.inputColumns());
        }
        return idx;
    }

    private static RelDataType rowType(RelDataTypeFactory typeFactory, List<String> columns) {
        RelDataType bigint = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (String col : columns) {
            builder.add(col, bigint);
        }
        return builder.build();
    }

    private static RelOptCluster newCluster(RelDataTypeFactory typeFactory) {
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner hepPlanner = new HepPlanner(new HepProgramBuilder().build());
        return RelOptCluster.create(hepPlanner, rexBuilder);
    }

    private static volatile SimpleExtension.ExtensionCollection extensions;

    /**
     * Same catalog as {@code DataFusionPlugin.loadSubstraitExtensions}: default collection plus
     * the OpenSearch function yamls — approx_distinct (dc), the scalar/arithmetic overloads,
     * etc. live in those, and shard fragments compiled here must bind the same signatures the
     * coordinator plans with. Cached: SimpleExtension.load parses yaml on every call.
     */
    private static SimpleExtension.ExtensionCollection loadExtensions() {
        SimpleExtension.ExtensionCollection cached = extensions;
        if (cached != null) {
            return cached;
        }
        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(DatafusionShardAggregationEngine.class.getClassLoader());
            SimpleExtension.ExtensionCollection merged = DefaultExtensionCatalog.DEFAULT_COLLECTION.merge(
                SimpleExtension.load(List.of("/delegation_functions.yaml"))
            )
                .merge(SimpleExtension.load(List.of("/opensearch_scalar_functions.yaml")))
                .merge(SimpleExtension.load(List.of("/opensearch_array_functions.yaml")))
                .merge(SimpleExtension.load(List.of("/opensearch_aggregate_functions.yaml")))
                .merge(SimpleExtension.load(List.of("/opensearch_window_functions.yaml")))
                .merge(SimpleExtension.load(List.of("/opensearch_arithmetic_overloads.yaml")));
            extensions = merged;
            return merged;
        } finally {
            t.setContextClassLoader(prev);
        }
    }
}
