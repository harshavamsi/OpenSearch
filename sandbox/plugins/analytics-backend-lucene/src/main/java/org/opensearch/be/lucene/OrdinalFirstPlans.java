/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

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
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.ShardAggregationEngine.AggCall;
import org.opensearch.analytics.spi.ShardAggregationEngine.AggFunction;
import org.opensearch.analytics.spi.ShardAggregationEngine.AggSpec;

import java.util.ArrayList;
import java.util.List;

/**
 * Calcite fragment builders for {@link DocValuesAggregationExecutor}'s ordinal-first keyword
 * group-by. Phase 1 runs the spec's aggregates grouped by the per-segment ordinal (Int64);
 * phase 2 re-groups the materialized terms and MERGES the phase-1 partials — COUNT partials
 * merge via SUM, SUM/MIN/MAX via themselves. Only merge-associative functions are legal here;
 * the executor's routing guarantees it.
 *
 * @opensearch.internal
 */
final class OrdinalFirstPlans {

    private OrdinalFirstPlans() {}

    /** {@code SELECT $ord, <spec aggs> FROM <stage-input $ord, metrics...> GROUP BY $ord}. */
    static RelNode phase1(AggSpec spec, List<String> inputColumns) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RelOptCluster cluster = newCluster(typeFactory);
        RelDataType bigint = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        RelDataTypeFactory.Builder rowBuilder = typeFactory.builder();
        for (String col : inputColumns) {
            rowBuilder.add(col, bigint);
        }
        OpenSearchStageInputScan scan = new OpenSearchStageInputScan(
            cluster,
            cluster.traitSet(),
            0,
            rowBuilder.build(),
            List.of(),
            List.of()
        );
        List<AggregateCall> calls = new ArrayList<>(spec.aggCalls().size());
        for (AggCall call : spec.aggCalls()) {
            calls.add(toCalciteCall(typeFactory, call, inputColumns, call.outputName()));
        }
        return LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, calls);
    }

    /**
     * {@code SELECT term, <merge aggs> FROM <stage-input term, partials...> GROUP BY term}.
     * Partial column c+1 carries {@code spec.aggCalls().get(c)}'s per-segment value.
     */
    static RelNode phase2(AggSpec spec, String keyColumn) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RelOptCluster cluster = newCluster(typeFactory);
        RelDataType bigint = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        RelDataType varchar = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RelDataTypeFactory.Builder rowBuilder = typeFactory.builder();
        rowBuilder.add(keyColumn, varchar);
        for (AggCall call : spec.aggCalls()) {
            rowBuilder.add(call.outputName(), bigint);
        }
        OpenSearchStageInputScan scan = new OpenSearchStageInputScan(
            cluster,
            cluster.traitSet(),
            0,
            rowBuilder.build(),
            List.of(),
            List.of()
        );
        List<AggregateCall> calls = new ArrayList<>(spec.aggCalls().size());
        for (int c = 0; c < spec.aggCalls().size(); c++) {
            AggCall call = spec.aggCalls().get(c);
            AggFunction mergeFn = call.function() == AggFunction.COUNT ? AggFunction.SUM : call.function();
            calls.add(
                AggregateCall.create(
                    switch (mergeFn) {
                        case SUM -> SqlStdOperatorTable.SUM;
                        case MIN -> SqlStdOperatorTable.MIN;
                        case MAX -> SqlStdOperatorTable.MAX;
                        default -> throw new IllegalStateException("non-mergeable " + call.function());
                    },
                    false,
                    List.of(c + 1),
                    -1,
                    bigint,
                    call.outputName()
                )
            );
        }
        return LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, calls);
    }

    private static AggregateCall toCalciteCall(RelDataTypeFactory typeFactory, AggCall call, List<String> inputColumns, String name) {
        RelDataType bigintNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        if (call.function() == AggFunction.COUNT) {
            return AggregateCall.create(SqlStdOperatorTable.COUNT, false, List.of(), -1, typeFactory.createSqlType(SqlTypeName.BIGINT), name);
        }
        int arg = inputColumns.indexOf(call.inputColumn());
        if (arg < 0) {
            throw new IllegalStateException("agg input [" + call.inputColumn() + "] not in " + inputColumns);
        }
        return AggregateCall.create(switch (call.function()) {
            case SUM -> SqlStdOperatorTable.SUM;
            case MIN -> SqlStdOperatorTable.MIN;
            case MAX -> SqlStdOperatorTable.MAX;
            default -> throw new IllegalStateException("non-mergeable " + call.function());
        }, false, List.of(arg), -1, bigintNullable, name);
    }

    private static RelOptCluster newCluster(RelDataTypeFactory typeFactory) {
        return RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), new RexBuilder(typeFactory));
    }
}
