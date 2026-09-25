/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.util.TimestampString;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class OpenSearchTimestampLiteralFolderTests extends OpenSearchTestCase {

    /** Stand-in for the PPL {@code TIMESTAMP(string)} builtin: same name, TIMESTAMP result. */
    private static final SqlFunction PPL_TIMESTAMP = new SqlFunction(
        "TIMESTAMP",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.TIMESTAMP).andThen(SqlTypeTransforms.TO_NULLABLE),
        null,
        OperandTypes.CHARACTER,
        SqlFunctionCategory.TIMEDATE
    );

    private JavaTypeFactoryImpl typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), rexBuilder);
    }

    public void testFoldsPplTimestampCallToLiteral() {
        Filter filter = filterOn("2013-07-01 00:00:00");

        RelNode rewritten = OpenSearchTimestampLiteralFolder.rewrite(filter);

        RexCall condition = (RexCall) ((Filter) rewritten).getCondition();
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, condition.getKind());
        RexNode right = condition.getOperands().get(1);
        assertTrue(right.toString(), right instanceof RexLiteral);
        assertEquals(SqlTypeName.TIMESTAMP, right.getType().getSqlTypeName());
        assertEquals(new TimestampString("2013-07-01 00:00:00"), ((RexLiteral) right).getValueAs(TimestampString.class));
    }

    public void testDateOnlyStringFoldsToMidnight() {
        RexNode right = ((RexCall) ((Filter) OpenSearchTimestampLiteralFolder.rewrite(filterOn("2013-07-31"))).getCondition())
            .getOperands()
            .get(1);
        assertEquals(new TimestampString("2013-07-31 00:00:00"), ((RexLiteral) right).getValueAs(TimestampString.class));
    }

    public void testUnparseableStringIsLeftAlone() {
        Filter filter = filterOn("last week");

        RelNode rewritten = OpenSearchTimestampLiteralFolder.rewrite(filter);

        RexNode right = ((RexCall) ((Filter) rewritten).getCondition()).getOperands().get(1);
        assertTrue(right.toString(), right instanceof RexCall);
        assertEquals("TIMESTAMP", ((RexCall) right).getOperator().getName());
    }

    /**
     * Field-invalid and out-of-i64-ns-range strings stay unfolded so the DataFusion adapter rejects
     * them at plan time with its normal message, instead of the fold producing a literal that fails
     * (or silently succeeds) at execution.
     */
    public void testInvalidFieldsAndOutOfRangeAreLeftAlone() {
        for (String text : List.of("2025-12-01 15:02:61", "2025-13-02", "2025-02-30", "3077-04-12 09:07:00", "0001-01-01")) {
            assertNull(text, OpenSearchTimestampLiteralFolder.parse(text));
        }
        assertEquals(
            new TimestampString("2013-07-01 00:00:00").withNanos(123_000_000),
            OpenSearchTimestampLiteralFolder.parse("2013-07-01T00:00:00.123")
        );
        assertEquals(new TimestampString("2262-04-11 23:47:16"), OpenSearchTimestampLiteralFolder.parse("2262-04-11 23:47:16"));
    }

    private Filter filterOn(String timestampText) {
        RelDataType tsType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
        RelDataType rowType = typeFactory.builder().add("event_date", tsType).build();
        LogicalValues values = LogicalValues.createEmpty(cluster, rowType);
        RexNode call = rexBuilder.makeCall(PPL_TIMESTAMP, List.of(rexBuilder.makeLiteral(timestampText)));
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, rexBuilder.makeInputRef(values, 0), call);
        return LogicalFilter.create(values, condition);
    }
}
