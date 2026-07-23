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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Pins {@link LuceneFragmentConvertor#extractDocValuesAggShape}: eligible iff grouped
 * aggregate whose group keys and agg args all resolve to LONG doc_values-backed physical
 * fields on the Lucene format, with calls restricted to COUNT(*)/SUM/MIN/MAX.
 */
public class LuceneDocValuesAggShapeTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RelOptCluster cluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), new RexBuilder(typeFactory));
    }

    public void testGroupBySumCountOverLongDvColumns_eligible() {
        TableScan scan = dvScan(longDvField("key"), longDvField("metric"));
        RelNode agg = LogicalAggregate.create(
            scan,
            List.of(),
            ImmutableBitSet.of(0),
            null,
            List.of(countStar(scan), sum(scan, 1, "total"))
        );
        LuceneFragmentConvertor.DocValuesAggShape shape = LuceneFragmentConvertor.extractDocValuesAggShape(agg);
        assertNotNull("grouped COUNT+SUM over LONG dv columns must be eligible", shape);
        assertEquals(List.of("key"), shape.groupColumns());
        assertEquals(List.of("key", "metric"), shape.inputColumns());
        assertEquals(2, shape.aggEntries().size());
        assertEquals("COUNT", shape.aggEntries().get(0).function());
        assertNull(shape.aggEntries().get(0).inputColumn());
        assertEquals("SUM", shape.aggEntries().get(1).function());
        assertEquals("metric", shape.aggEntries().get(1).inputColumn());
        assertTrue(LuceneFragmentConvertor.isDocValuesGroupByPath(agg));
    }

    public void testEmptyGroupSet_notEligible() {
        TableScan scan = dvScan(longDvField("key"));
        RelNode agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(countStar(scan)));
        assertNull("global aggregates stay on the count fast path", LuceneFragmentConvertor.extractDocValuesAggShape(agg));
    }

    public void testKeywordGroupKey_notEligible() {
        TableScan scan = dvScan(keywordField("status"), longDvField("metric"));
        RelNode agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(sum(scan, 1, "total")));
        assertNull("keyword group key is out of v1 scope", LuceneFragmentConvertor.extractDocValuesAggShape(agg));
    }

    public void testFieldWithoutLuceneDocValues_notEligible() {
        FieldStorageInfo noDv = new FieldStorageInfo("key", "long", FieldType.LONG, List.of(), List.of(), List.of(), false);
        TableScan scan = dvScan(noDv, longDvField("metric"));
        RelNode agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(countStar(scan)));
        assertNull("no lucene doc_values on the key — ineligible", LuceneFragmentConvertor.extractDocValuesAggShape(agg));
    }

    public void testDistinctCall_notEligible() {
        TableScan scan = dvScan(longDvField("key"), longDvField("metric"));
        AggregateCall distinctSum = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            true,
            List.of(1),
            -1,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true),
            "total"
        );
        RelNode agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(distinctSum));
        assertNull("DISTINCT calls are ineligible", LuceneFragmentConvertor.extractDocValuesAggShape(agg));
    }

    public void testCountField_notEligible() {
        // COUNT(col) needs null-skipping the decode path can't provide (missing decodes as 0).
        TableScan scan = dvScan(longDvField("key"), longDvField("metric"));
        AggregateCall countField = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(1),
            -1,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt_metric"
        );
        RelNode agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(countField));
        assertNull("COUNT(col) is ineligible in v1", LuceneFragmentConvertor.extractDocValuesAggShape(agg));
    }

    // ---- Helpers ----

    private FieldStorageInfo longDvField(String name) {
        return new FieldStorageInfo(
            name,
            "long",
            FieldType.LONG,
            List.of(LuceneDataFormat.LUCENE_FORMAT_NAME),
            List.of(),
            List.of(),
            false
        );
    }

    private FieldStorageInfo keywordField(String name) {
        return new FieldStorageInfo(
            name,
            "keyword",
            FieldType.KEYWORD,
            List.of(LuceneDataFormat.LUCENE_FORMAT_NAME),
            List.of(LuceneDataFormat.LUCENE_FORMAT_NAME),
            List.of(),
            false
        );
    }

    private TableScan dvScan(FieldStorageInfo... fields) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (FieldStorageInfo f : fields) {
            SqlTypeName type = switch (f.getFieldType()) {
                case LONG -> SqlTypeName.BIGINT;
                case KEYWORD -> SqlTypeName.VARCHAR;
                default -> throw new AssertionError(f.getFieldType());
            };
            // Nullable like real mapped fields — SUM's inferred type is then nullable BIGINT,
            // matching the AggregateCall types the tests construct.
            builder.add(f.getFieldName(), typeFactory.createTypeWithNullability(typeFactory.createSqlType(type), true));
        }
        RelDataType rowType = builder.build();
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("test_index"));
        when(table.getRowType()).thenReturn(rowType);
        List<FieldStorageInfo> storage = List.of(fields);
        return new DvScan(cluster, table, storage);
    }

    /** TableScan that is also an {@link OpenSearchRelNode} carrying explicit field storage. */
    private static final class DvScan extends TableScan implements OpenSearchRelNode {
        private final List<FieldStorageInfo> storage;

        DvScan(RelOptCluster cluster, RelOptTable table, List<FieldStorageInfo> storage) {
            super(cluster, cluster.traitSet(), List.of(), table);
            this.storage = storage;
        }

        @Override
        public List<FieldStorageInfo> getOutputFieldStorage() {
            return storage;
        }

        @Override
        public List<String> getViableBackends() {
            return List.of(LuceneDataFormat.LUCENE_FORMAT_NAME);
        }

        @Override
        public RelNode stripAnnotations(List<RelNode> strippedChildren) {
            return this;
        }

        @Override
        public RelNode copyResolved(
            String backend,
            List<RelNode> children,
            List<org.opensearch.analytics.planner.rel.OperatorAnnotation> resolvedAnnotations
        ) {
            return this;
        }
    }

    private AggregateCall countStar(TableScan scan) {
        return AggregateCall.create(SqlStdOperatorTable.COUNT, false, List.of(), -1, typeFactory.createSqlType(SqlTypeName.BIGINT), "cnt");
    }

    private AggregateCall sum(TableScan scan, int arg, String name) {
        return AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(arg),
            -1,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true),
            name
        );
    }
}
