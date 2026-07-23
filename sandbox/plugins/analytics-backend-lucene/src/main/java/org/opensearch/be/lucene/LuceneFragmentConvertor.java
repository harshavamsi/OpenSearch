/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ShardAggregationEngineHolder;
import org.opensearch.analytics.spi.WireFormat;
import org.opensearch.be.lucene.serializers.AbstractQuerySerializer;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.Type;

/**
 * Lucene-as-driver {@link FragmentConvertor}. Walks the resolved fragment, finds the
 * {@link OpenSearchFilter}, and serializes its condition as a {@link BoolQueryBuilder}'s
 * NamedWriteable bytes. Empty bytes when the fragment has no filter ({@code count(*)} over
 * MatchAllDocs at the data node).
 *
 * <p>Reuses the same leaf-serializer registry as {@link LuceneSubtreeConvertor} via
 * {@link QuerySerializerRegistry} — keyword equality, MATCH, MATCH_PHRASE, etc. all
 * round-trip through the same {@link DelegatedPredicateSerializer} → {@link QueryBuilder}
 * path. The data-node Lucene driver deserializes the bytes via NamedWriteable and runs
 * {@code IndexSearcher.count} on the resulting {@link QueryBuilder#toQuery(QueryShardContext)}.
 *
 * <p>Multi-stage / non-shard-scan fragments aren't supported: Lucene drives shard-local
 * count fragments only. Reduce or coordinator stages still run on DataFusion, so this
 * convertor is invoked only when the planner picked Lucene as the StagePlan's backend —
 * which happens exclusively for count-fast-path-eligible shards today.
 *
 * @opensearch.internal
 */
final class LuceneFragmentConvertor implements FragmentConvertor {

    private static final Logger LOGGER = LogManager.getLogger(LuceneFragmentConvertor.class);

    private final Map<ScalarFunction, DelegatedPredicateSerializer> leafSerializers;

    LuceneFragmentConvertor(Map<ScalarFunction, DelegatedPredicateSerializer> leafSerializers) {
        this.leafSerializers = leafSerializers;
    }

    /**
     * True iff the top is an {@link Aggregate} with empty group-set whose every call is
     * {@link SqlKind#COUNT} — what {@code IndexSearcher.count} can answer from the term
     * dictionary. Read by {@link LuceneShardPreference} to score this fragment.
     *
     * <p>Defense-in-depth: PlanForker's chain-agreement filter already narrows aggregate
     * alternatives to declared capabilities (prod Lucene declares only COUNT), so this
     * guards against capability-declaration drift.
     */
    static boolean isCountFastPath(RelNode fragment) {
        if (fragment instanceof Aggregate == false) return false;
        Aggregate agg = (Aggregate) fragment;
        if (agg.getGroupSet().isEmpty() == false) return false;
        for (AggregateCall call : agg.getAggCallList()) {
            if (call.getAggregation().getKind() != SqlKind.COUNT) return false;
        }
        return true;
    }

    /**
     * Aggregate kinds the doc_values group-by path can push into the shard-local engine.
     * AVG is deliberately absent: DataFusion types AVG(Int64) as Float64, breaking the
     * all-Int64 batch contract — and the planner decomposes AVG into SUM+COUNT before
     * shard fragments form, so it shouldn't reach here anyway.
     */
    private static final java.util.Set<SqlKind> DV_AGG_KINDS = java.util.Set.of(
        SqlKind.COUNT,
        SqlKind.SUM,
        SqlKind.SUM0,
        SqlKind.MIN,
        SqlKind.MAX
    );

    /**
     * Field types the decode path handles in v1: LONG only. The runtime emits Int64 for
     * every column, and the coordinator's schema-only Read stub is built from the Calcite
     * row type — an INTEGER/SHORT/DATE key would declare I32/I16/timestamp there while
     * Int64 batches arrive, which is exactly the silent-stall mismatch documented on
     * {@link #convertSchemaOnlyRead}. Widening needs per-type decode-side casts or stub-side
     * Int64 coercion; deferred.
     */
    private static final java.util.Set<org.opensearch.analytics.spi.FieldType> DV_LONG_TYPES = java.util.Set.of(
        org.opensearch.analytics.spi.FieldType.LONG,
        org.opensearch.analytics.spi.FieldType.INTEGER,
        org.opensearch.analytics.spi.FieldType.SHORT,
        org.opensearch.analytics.spi.FieldType.BYTE,
        org.opensearch.analytics.spi.FieldType.UNSIGNED_LONG,
        org.opensearch.analytics.spi.FieldType.DATE,
        org.opensearch.analytics.spi.FieldType.DATE_NANOS,
        org.opensearch.analytics.spi.FieldType.BOOLEAN
    );

    /**
     * True iff the fragment is a grouped aggregate the doc_values path can drive: non-empty
     * group set, every call in {@link #DV_AGG_KINDS} (non-distinct, at most one argument),
     * and every referenced input column (group keys + agg args) resolves to a long-typed
     * doc_values-backed physical field on the Lucene format. Read by
     * {@link LuceneShardPreference}; mirrored by the convert/exec path.
     */
    static boolean isDocValuesGroupByPath(RelNode fragment) {
        return extractDocValuesAggShape(fragment) != null;
    }

    /**
     * Decomposed doc_values group-by shape: the aggregate node plus resolved input column
     * names. {@code null} when the fragment doesn't match (routes to count/objects paths).
     */
    record DocValuesAggShape(Aggregate aggregate, List<String> groupColumns, List<AggSpecEntry> aggEntries, List<String> inputColumns) {
    }

    /** One serializable aggregate call: function kind name + input column (null for COUNT). */
    record AggSpecEntry(String function, String inputColumn, String outputName) {
    }

    static DocValuesAggShape extractDocValuesAggShape(RelNode fragment) {
        if (fragment instanceof Aggregate == false) {
            return null;
        }
        Aggregate agg = (Aggregate) fragment;
        if (agg.getGroupSet().isEmpty()) {
            return null; // global aggregates take the count fast path or stay on DataFusion
        }
        // Resolve the aggregate INPUT's storage info — group/arg ordinals index into it.
        List<FieldStorageInfo> storage = findInputFieldStorage(agg);
        if (storage == null) {
            return null;
        }
        List<String> groupColumns = new ArrayList<>();
        java.util.LinkedHashSet<String> inputColumns = new java.util.LinkedHashSet<>();
        for (int ord : agg.getGroupSet()) {
            String col = docValuesLongColumn(storage, ord);
            if (col == null) {
                return null;
            }
            groupColumns.add(col);
            inputColumns.add(col);
        }
        List<AggSpecEntry> entries = new ArrayList<>(agg.getAggCallList().size());
        for (AggregateCall call : agg.getAggCallList()) {
            if (DV_AGG_KINDS.contains(call.getAggregation().getKind()) == false || call.isDistinct()) {
                return null;
            }
            if (call.getAggregation().getKind() == SqlKind.COUNT && call.getArgList().isEmpty()) {
                entries.add(new AggSpecEntry("COUNT", null, call.getName()));
                continue;
            }
            if (call.getArgList().size() != 1) {
                return null;
            }
            String col = docValuesLongColumn(storage, call.getArgList().get(0));
            if (col == null) {
                return null;
            }
            // COUNT(col) needs null-skipping semantics; v1 decodes missing as 0 and cannot
            // distinguish, so only COUNT(*) is eligible. SUM0 maps to SUM (all-long inputs).
            String fn = switch (call.getAggregation().getKind()) {
                case SUM, SUM0 -> "SUM";
                case MIN -> "MIN";
                case MAX -> "MAX";
                default -> null;
            };
            if (fn == null) {
                return null;
            }
            entries.add(new AggSpecEntry(fn, col, call.getName()));
            inputColumns.add(col);
        }
        return new DocValuesAggShape(agg, groupColumns, entries, List.copyOf(inputColumns));
    }

    /**
     * Resolves ordinal {@code ord} of the aggregate's input to a physical, long-typed,
     * doc_values-backed field name on the Lucene format; {@code null} if ineligible.
     */
    private static String docValuesLongColumn(List<FieldStorageInfo> storage, int ord) {
        if (ord < 0 || ord >= storage.size()) {
            return null;
        }
        FieldStorageInfo info = storage.get(ord);
        if (info.isDerived()) {
            return null;
        }
        if (DV_LONG_TYPES.contains(info.getFieldType()) == false) {
            return null;
        }
        List<String> dvFormats = info.getDocValueFormats();
        if (dvFormats == null || dvFormats.contains(LuceneDataFormat.LUCENE_FORMAT_NAME) == false) {
            return null;
        }
        return info.getFieldName();
    }

    /**
     * Kind-aware resolver for the engine-plan path: LONG for numeric/date types, KEYWORD for
     * keyword fields (single-valued sorted doc_values, materialized to Utf8 at decode);
     * {@code null} when the ordinal has no eligible doc_values backing.
     */
    private static org.opensearch.analytics.spi.ShardAggregationEngine.InputColumn docValuesColumn(
        List<FieldStorageInfo> storage,
        int ord
    ) {
        if (ord < 0 || ord >= storage.size()) {
            return null;
        }
        FieldStorageInfo info = storage.get(ord);
        // The QTF/late-materialization rewriter declares __row_id__ as a derived column, but
        // on lucene-primary segments it is a real singleton SortedNumericDocValues field (the
        // index sort key) — decodable through the LONG path (openColumn unwraps the singleton).
        if (org.opensearch.index.engine.dataformat.DocumentInput.ROW_ID_FIELD.equals(info.getFieldName())) {
            return new org.opensearch.analytics.spi.ShardAggregationEngine.InputColumn(
                info.getFieldName(),
                org.opensearch.analytics.spi.ShardAggregationEngine.ColumnKind.LONG
            );
        }
        if (info.isDerived()) {
            return null;
        }
        List<String> dvFormats = info.getDocValueFormats();
        if (dvFormats == null || dvFormats.contains(LuceneDataFormat.LUCENE_FORMAT_NAME) == false) {
            return null;
        }
        if (info.getFieldType() == org.opensearch.analytics.spi.FieldType.DATE
            || info.getFieldType() == org.opensearch.analytics.spi.FieldType.DATE_NANOS) {
            return new org.opensearch.analytics.spi.ShardAggregationEngine.InputColumn(
                info.getFieldName(),
                org.opensearch.analytics.spi.ShardAggregationEngine.ColumnKind.TIMESTAMP
            );
        }
        if (DV_LONG_TYPES.contains(info.getFieldType())) {
            return new org.opensearch.analytics.spi.ShardAggregationEngine.InputColumn(
                info.getFieldName(),
                org.opensearch.analytics.spi.ShardAggregationEngine.ColumnKind.LONG
            );
        }
        if (info.getFieldType() == org.opensearch.analytics.spi.FieldType.KEYWORD) {
            return new org.opensearch.analytics.spi.ShardAggregationEngine.InputColumn(
                info.getFieldName(),
                org.opensearch.analytics.spi.ShardAggregationEngine.ColumnKind.KEYWORD
            );
        }
        return null;
    }

    /**
     * General doc_values shape: {@code Aggregate [→ Project] [→ Filter] → scan} where every
     * scan column REFERENCED by the engine-executed part (project expressions or aggregate
     * keys/args — the filter is extracted and runs Lucene-side) is a LONG doc_values field.
     * Unlike {@link #extractDocValuesAggShape}, the aggregate calls and project expressions are
     * NOT interpreted here — the whole subtree is rebased onto a stage-input scan and compiled
     * by the engine ({@code ShardAggregationEngine.compileFragment}), so DISTINCT, expressions,
     * and any engine-supported aggregate come along for free. Group set may be empty (global
     * aggregates like {@code count(distinct x)}).
     */
    record GeneralDvShape(RelNode rebasedFragment, List<org.opensearch.analytics.spi.ShardAggregationEngine.InputColumn> inputColumns,
        Filter filter, List<String> outputNames) {
    }

    static GeneralDvShape extractGeneralDvShape(RelNode fragment) {
        // A pure column-permutation Project on top (planner output reorder, e.g. PPL's
        // `stats count() by key` emits Project[count(), key] over the aggregate) is
        // transparent: extract the shape from the aggregate below and re-apply the
        // permutation to the rebased plan.
        Project outputPermutation = null;
        if (fragment instanceof Project topProject
            && topProject.getInput() instanceof Aggregate
            && topProject.getProjects().stream().allMatch(e -> e instanceof RexInputRef)) {
            outputPermutation = topProject;
            fragment = topProject.getInput();
        }
        if (fragment instanceof Aggregate == false) {
            return extractRowDvShape(fragment);
        }
        Aggregate agg = (Aggregate) fragment;
        RelNode below = agg.getInput();
        Project project = null;
        if (below instanceof Project p) {
            project = p;
            below = p.getInput();
        }
        Filter filter = null;
        if (below instanceof Filter f) {
            filter = f;
            below = f.getInput();
        }
        if (below instanceof OpenSearchRelNode == false || below.getInputs().isEmpty() == false) {
            return null;
        }
        List<FieldStorageInfo> storage = ((OpenSearchRelNode) below).getOutputFieldStorage();
        if (storage == null) {
            return null;
        }

        // Ordinals of the scan row referenced by the engine-executed subtree.
        java.util.TreeSet<Integer> referenced = new java.util.TreeSet<>();
        if (project != null) {
            RexShuttle collector = new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef ref) {
                    referenced.add(ref.getIndex());
                    return ref;
                }
            };
            for (RexNode expr : project.getProjects()) {
                expr.accept(collector);
            }
        } else {
            for (int ord : agg.getGroupSet()) {
                referenced.add(ord);
            }
            for (AggregateCall call : agg.getAggCallList()) {
                referenced.addAll(call.getArgList());
                if (call.filterArg >= 0) {
                    return null; // FILTER (WHERE ...) aggregate args not supported
                }
            }
        }
        if (referenced.isEmpty()) {
            // e.g. bare COUNT(*) — the count fast path owns that shape.
            return null;
        }

        List<org.opensearch.analytics.spi.ShardAggregationEngine.InputColumn> inputColumns = new ArrayList<>(referenced.size());
        int[] oldToNew = new int[storage.size()];
        java.util.Arrays.fill(oldToNew, -1);
        for (int ord : referenced) {
            org.opensearch.analytics.spi.ShardAggregationEngine.InputColumn col = docValuesColumn(storage, ord);
            if (col == null) {
                return null;
            }
            oldToNew[ord] = inputColumns.size();
            inputColumns.add(col);
        }

        // Rebase: stage-input scan over just the referenced columns (childStageId 0 → the
        // engine session's "input-0"), refs remapped in the direct consumer only — nodes
        // above the consumer see identical row types.
        RelDataTypeFactory typeFactory = agg.getCluster().getTypeFactory();
        RelDataTypeFactory.Builder rowBuilder = typeFactory.builder();
        List<FieldStorageInfo> newStorage = new ArrayList<>(inputColumns.size());
        RelDataType scanRow = (project != null ? project.getInput() : (filter != null ? filter.getInput() : agg.getInput())).getRowType();
        for (int ord : referenced) {
            rowBuilder.add(scanRow.getFieldList().get(ord).getName(), scanRow.getFieldList().get(ord).getType());
            newStorage.add(storage.get(ord));
        }
        OpenSearchStageInputScan newScan = new OpenSearchStageInputScan(
            agg.getCluster(),
            agg.getTraitSet(),
            0,
            rowBuilder.build(),
            List.of(),
            newStorage
        );

        RexShuttle remap = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                int mapped = oldToNew[ref.getIndex()];
                if (mapped < 0) {
                    throw new IllegalStateException("unreferenced ordinal " + ref.getIndex() + " survived collection");
                }
                return new RexInputRef(mapped, ref.getType());
            }
        };
        RelNode rebasedInput;
        Aggregate rebasedAgg;
        if (project != null) {
            List<RexNode> remapped = new ArrayList<>(project.getProjects().size());
            for (RexNode expr : project.getProjects()) {
                remapped.add(expr.accept(remap));
            }
            rebasedInput = LogicalProject.create(newScan, project.getHints(), remapped, project.getRowType().getFieldNames());
            // Aggregate refs index the project's output row, which is unchanged. copy() (not
            // LogicalAggregate.create) preserves the node type — an OpenSearchAggregate keeps
            // its PARTIAL/FINAL mode, which the engine convertor lowers to partial aggregation
            // (APPROX_COUNT_DISTINCT must emit HLL state for the coordinator merge, not a count).
            rebasedAgg = (Aggregate) agg.copy(agg.getTraitSet(), rebasedInput, agg.getGroupSet(), agg.getGroupSets(), agg.getAggCallList());
        } else {
            ImmutableBitSet.Builder newGroupSet = ImmutableBitSet.builder();
            for (int ord : agg.getGroupSet()) {
                newGroupSet.set(oldToNew[ord]);
            }
            List<AggregateCall> newCalls = new ArrayList<>(agg.getAggCallList().size());
            for (AggregateCall call : agg.getAggCallList()) {
                List<Integer> newArgs = new ArrayList<>(call.getArgList().size());
                for (int a : call.getArgList()) {
                    newArgs.add(oldToNew[a]);
                }
                newCalls.add(call.copy(newArgs, -1, call.collation));
            }
            rebasedAgg = (Aggregate) agg.copy(agg.getTraitSet(), newScan, newGroupSet.build(), null, newCalls);
        }
        RelNode rebased = rebasedAgg;
        if (outputPermutation != null) {
            // Refs index the aggregate's output row, which the rebase preserves.
            rebased = LogicalProject.create(
                rebasedAgg,
                outputPermutation.getHints(),
                outputPermutation.getProjects(),
                outputPermutation.getRowType().getFieldNames()
            );
        }
        List<String> outputNames = outputPermutation != null
            ? outputPermutation.getRowType().getFieldNames()
            : fragment.getRowType().getFieldNames();
        return new GeneralDvShape(rebased, inputColumns, filter, outputNames);
    }

    /**
     * Row-returning doc_values shape: {@code [Project] → [Sort] → [Project] → [Filter] → scan}
     * with no Aggregate — PPL {@code where … | fields … | sort … | head N} compiles to this at
     * the shard stage. The engine executes the Sort/Project part (DataFusion TopK per shard,
     * coordinator reduce merges); the Filter is extracted and runs Lucene-side, exactly like
     * the aggregate path. Requires every scan column consumed by the engine part to be
     * dv-decodable; synthetic/derived columns (late-materialization {@code __row_id__} /
     * {@code ___ugsi}) fail the storage check and fall back.
     *
     * <p><b>Pipeline-breaker contract:</b> the shard session is feed-then-finish
     * (single-threaded), which only avoids deadlock when the plan buffers output until
     * end-of-input. Sort+fetch (TopK) is a pipeline breaker; a fetch-less Project→Filter
     * shape streams output DURING feed and can fill the bounded native channels when the
     * filter matches many rows. Accepted anyway because PPL's high-selectivity point
     * lookups need it (q20 shape); a general fix is the memtable registration path or a
     * concurrent drain.
     */
    static GeneralDvShape extractRowDvShape(RelNode fragment) {
        Project topProject = null;
        RelNode node = fragment;
        if (node instanceof Project p) {
            topProject = p;
            node = p.getInput();
        }
        org.apache.calcite.rel.core.Sort sort = null;
        if (node instanceof org.apache.calcite.rel.core.Sort s) {
            // A shard-level offset would drop rows the coordinator merge still needs; the
            // planner keeps offsets coordinator-side today — bail if one ever shows up here.
            if (s.offset != null) {
                return null;
            }
            sort = s;
            node = s.getInput();
        }
        Project midProject = null;
        if (node instanceof Project p) {
            midProject = p;
            node = p.getInput();
        }
        Filter filter = null;
        if (node instanceof Filter f) {
            filter = f;
            node = f.getInput();
        }
        if (node instanceof OpenSearchRelNode == false || node.getInputs().isEmpty() == false) {
            return null;
        }
        // Bare Filter→Scan / Scan is the count fast path's input subtree (the partial-agg
        // split converts it separately) — claiming it here would corrupt count queries.
        if (topProject == null && sort == null && midProject == null) {
            return null;
        }
        List<FieldStorageInfo> storage = ((OpenSearchRelNode) node).getOutputFieldStorage();
        if (storage == null) {
            return null;
        }

        // Scan ordinals consumed by the engine part. With a mid Project, everything above it
        // references its output — only its expressions touch the scan row. Without one, the
        // top Project's expressions and the sort keys index the scan row directly; and if
        // there is no project at all, the fragment's output IS the scan row, so every column
        // is consumed.
        java.util.TreeSet<Integer> referenced = new java.util.TreeSet<>();
        RexShuttle collector = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                referenced.add(ref.getIndex());
                return ref;
            }
        };
        if (midProject != null) {
            for (RexNode expr : midProject.getProjects()) {
                expr.accept(collector);
            }
        } else if (topProject != null) {
            for (RexNode expr : topProject.getProjects()) {
                expr.accept(collector);
            }
            if (sort != null) {
                for (org.apache.calcite.rel.RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
                    referenced.add(fc.getFieldIndex());
                }
            }
        } else {
            for (int i = 0; i < storage.size(); i++) {
                referenced.add(i);
            }
        }
        // Constant-only projection (e.g. `where UserID = <k> | fields UserID` folds the ref to
        // the literal): feed one real column anyway so the engine sees a row per match. Prefer
        // a column the filter references; fall back to the first dv-eligible one.
        if (referenced.isEmpty()) {
            java.util.TreeSet<Integer> filterRefs = new java.util.TreeSet<>();
            if (filter != null) {
                filter.getCondition().accept(new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef ref) {
                        filterRefs.add(ref.getIndex());
                        return ref;
                    }
                });
            }
            for (int ord : filterRefs) {
                if (docValuesColumn(storage, ord) != null) {
                    referenced.add(ord);
                    break;
                }
            }
            if (referenced.isEmpty()) {
                for (int i = 0; i < storage.size(); i++) {
                    if (docValuesColumn(storage, i) != null) {
                        referenced.add(i);
                        break;
                    }
                }
            }
            if (referenced.isEmpty()) {
                return null;
            }
        }

        List<org.opensearch.analytics.spi.ShardAggregationEngine.InputColumn> inputColumns = new ArrayList<>(referenced.size());
        int[] oldToNew = new int[storage.size()];
        java.util.Arrays.fill(oldToNew, -1);
        for (int ord : referenced) {
            org.opensearch.analytics.spi.ShardAggregationEngine.InputColumn col = docValuesColumn(storage, ord);
            if (col == null) {
                return null;
            }
            oldToNew[ord] = inputColumns.size();
            inputColumns.add(col);
        }

        RelDataTypeFactory typeFactory = fragment.getCluster().getTypeFactory();
        RelDataTypeFactory.Builder rowBuilder = typeFactory.builder();
        List<FieldStorageInfo> newStorage = new ArrayList<>(inputColumns.size());
        RelDataType scanRow = node.getRowType();
        for (int ord : referenced) {
            rowBuilder.add(scanRow.getFieldList().get(ord).getName(), scanRow.getFieldList().get(ord).getType());
            newStorage.add(storage.get(ord));
        }
        OpenSearchStageInputScan newScan = new OpenSearchStageInputScan(
            fragment.getCluster(),
            fragment.getTraitSet(),
            0,
            rowBuilder.build(),
            List.of(),
            newStorage
        );

        RexShuttle remap = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                int mapped = oldToNew[ref.getIndex()];
                if (mapped < 0) {
                    throw new IllegalStateException("unreferenced ordinal " + ref.getIndex() + " survived collection");
                }
                return new RexInputRef(mapped, ref.getType());
            }
        };

        RelNode rebased = newScan;
        if (midProject != null) {
            List<RexNode> remapped = new ArrayList<>(midProject.getProjects().size());
            for (RexNode expr : midProject.getProjects()) {
                remapped.add(expr.accept(remap));
            }
            rebased = LogicalProject.create(newScan, midProject.getHints(), remapped, midProject.getRowType().getFieldNames());
        }
        if (sort != null) {
            org.apache.calcite.rel.RelCollation collation = sort.getCollation();
            if (midProject == null) {
                List<org.apache.calcite.rel.RelFieldCollation> remappedFcs = new ArrayList<>();
                for (org.apache.calcite.rel.RelFieldCollation fc : collation.getFieldCollations()) {
                    remappedFcs.add(fc.withFieldIndex(oldToNew[fc.getFieldIndex()]));
                }
                collation = org.apache.calcite.rel.RelCollations.of(remappedFcs);
            }
            rebased = org.apache.calcite.rel.logical.LogicalSort.create(rebased, collation, null, sort.fetch);
        }
        if (topProject != null) {
            List<RexNode> exprs;
            if (midProject == null) {
                exprs = new ArrayList<>(topProject.getProjects().size());
                for (RexNode expr : topProject.getProjects()) {
                    exprs.add(expr.accept(remap));
                }
            } else {
                exprs = new ArrayList<>(topProject.getProjects());
            }
            rebased = LogicalProject.create(rebased, topProject.getHints(), exprs, topProject.getRowType().getFieldNames());
        }
        return new GeneralDvShape(rebased, inputColumns, filter, fragment.getRowType().getFieldNames());
    }

    /**
     * Field storage of the aggregate's input row: from the nearest {@link OpenSearchRelNode}
     * below the aggregate (Filter layers are transparent — a plain LogicalFilter preserves
     * its input's row shape). {@code null} when no OpenSearch ancestor carries storage info.
     */
    private static List<FieldStorageInfo> findInputFieldStorage(Aggregate agg) {
        RelNode current = agg.getInput();
        while (current != null) {
            if (current instanceof OpenSearchRelNode osNode) {
                return osNode.getOutputFieldStorage();
            }
            if (current.getInputs().isEmpty()) {
                return null;
            }
            current = current.getInputs().getFirst();
        }
        return null;
    }

    /**
     * Sentinel first entry in the columnNames collection marking wire format v2 (doc_values
     * group-by). v1 (count fast path) bytes never contain it — column names come from
     * aggregate-call names which Calcite never generates with this prefix.
     */
    static final String DV_AGG_MARKER = " dv-agg ";

    /**
     * Wire v3 marker: engine-compiled fragment plan. columnNames layout:
     * {@code [MARKER, base64(planBytes), nInput, inputCol..., nOut, outName...]} + the
     * standard {@code [hasFilter][QueryBuilder?]} tail. Preferred over v2 when the engine
     * is installed; v2 remains the no-engine fallback.
     */
    static final String DV_PLAN_MARKER = " dv-plan ";

    @Override
    public byte[] convertFragment(RelNode fragment) {
        // Doc_values engine-plan path (wire v3): rebase the fragment onto a stage-input scan
        // over the referenced LONG dv columns and let the engine compile it — any plan shape
        // the engine can run (DISTINCT, expressions), not just the v2 spec.
        if (ShardAggregationEngineHolder.isAvailable()) {
            GeneralDvShape general = extractGeneralDvShape(fragment);
            if (general != null) {
                return convertDvPlanFragment(general);
            }
            LOGGER.debug("[lucene-dv] general shape extraction returned null for fragment:\n{}", org.apache.calcite.plan.RelOptUtil.toString(fragment));
        } else {
            LOGGER.debug("[lucene-dv] shard aggregation engine not available");
        }
        // Doc_values group-by (wire v2): [DV_AGG_MARKER + spec strings as columnNames]
        // [hasFilter boolean] [QueryBuilder NamedWriteable]? — reuses the v1 layout with a
        // marker so the data-node decoder branches without a format-version field.
        DocValuesAggShape dvShape = extractDocValuesAggShape(fragment);
        if (dvShape != null) {
            return convertDocValuesAggFragment(fragment, dvShape);
        }
        // Lucene-driver wire format: [columnNames StringCollection] [hasFilter boolean]
        // [QueryBuilder NamedWriteable]?. Both ends are controlled (this convertor on the
        // coordinator, LuceneScanInstructionHandler on the data node), so a tiny custom
        // format is fine — beats threading column names through the InstructionNode.
        // columnNames may be empty when the convertor runs against a non-count Lucene
        // alternative kept around for delegation (e.g. DF drives, Lucene is the peer); the
        // bytes are produced but the data node never invokes them — selector or runtime
        // alternative-selection drops this plan before dispatch.
        List<String> columnNames = extractAggCallNames(fragment);
        QueryBuilder filterQuery = null;
        Filter filter = findFilter(fragment);
        if (filter != null) {
            // strip() in FragmentConversionDriver replaces OpenSearchFilter with a plain
            // LogicalFilter, so the field-storage info lives on the OpenSearch ancestor
            // below (the TableScan). Walk down past LogicalFilter to find the nearest
            // OpenSearchRelNode and use its output field storage. The condition itself was
            // already resolved (annotation placeholders unwrapped) by the resolver in strip().
            List<FieldStorageInfo> fieldStorage = findFieldStorage(filter);
            filterQuery = toQueryBuilder(filter.getCondition(), fieldStorage);
        }
        byte[] bytes;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeStringCollection(columnNames);
            if (filterQuery == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeNamedWriteable(filterQuery);
            }
            bytes = BytesReference.toBytes(out.bytes());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize Lucene-driver fragment", e);
        }
        LOGGER.debug("[lucene-count] convertFragment columnNames={} filterQuery={} bytes={}", columnNames, filterQuery, bytes.length);
        return bytes;
    }

    /** Wire v3 encode: engine-compiled plan + input columns + output names + filter tail. */
    private byte[] convertDvPlanFragment(GeneralDvShape shape) {
        byte[] planBytes = ShardAggregationEngineHolder.get().compileFragment(shape.rebasedFragment());
        List<String> encoded = new ArrayList<>();
        encoded.add(DV_PLAN_MARKER);
        encoded.add(java.util.Base64.getEncoder().encodeToString(planBytes));
        encoded.add(Integer.toString(shape.inputColumns().size()));
        for (org.opensearch.analytics.spi.ShardAggregationEngine.InputColumn col : shape.inputColumns()) {
            encoded.add(col.name());
            encoded.add(col.kind().name());
        }
        encoded.add(Integer.toString(shape.outputNames().size()));
        encoded.addAll(shape.outputNames());

        QueryBuilder filterQuery = null;
        if (shape.filter() != null) {
            List<FieldStorageInfo> fieldStorage = findFieldStorage(shape.filter());
            filterQuery = toQueryBuilder(shape.filter().getCondition(), fieldStorage);
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeStringCollection(encoded);
            if (filterQuery == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeNamedWriteable(filterQuery);
            }
            byte[] bytes = BytesReference.toBytes(out.bytes());
            LOGGER.debug(
                "[lucene-dv-plan] convertFragment inputColumns={} outputNames={} plan={}B filter={} bytes={}",
                shape.inputColumns(),
                shape.outputNames(),
                planBytes.length,
                filterQuery,
                bytes.length
            );
            return bytes;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize Lucene dv-plan fragment", e);
        }
    }

    /**
     * Wire v2 encode: the spec rides the columnNames StringCollection as
     * {@code [MARKER, nInput, in..., nGroup, grp..., nAgg, (fn, col-or-"", out)...]}, followed
     * by the standard filter tail. The data-node decoder ({@code LuceneScanInstructionHandler})
     * rebuilds a {@code ShardAggregationEngine.AggSpec} from it; column order here defines the
     * batch schema fed to the engine.
     */
    private byte[] convertDocValuesAggFragment(RelNode fragment, DocValuesAggShape shape) {
        List<String> encoded = new ArrayList<>();
        encoded.add(DV_AGG_MARKER);
        encoded.add(Integer.toString(shape.inputColumns().size()));
        encoded.addAll(shape.inputColumns());
        encoded.add(Integer.toString(shape.groupColumns().size()));
        encoded.addAll(shape.groupColumns());
        encoded.add(Integer.toString(shape.aggEntries().size()));
        for (AggSpecEntry entry : shape.aggEntries()) {
            encoded.add(entry.function());
            encoded.add(entry.inputColumn() == null ? "" : entry.inputColumn());
            encoded.add(entry.outputName());
        }

        QueryBuilder filterQuery = null;
        Filter filter = findFilter(fragment);
        if (filter != null) {
            List<FieldStorageInfo> fieldStorage = findFieldStorage(filter);
            filterQuery = toQueryBuilder(filter.getCondition(), fieldStorage);
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeStringCollection(encoded);
            if (filterQuery == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeNamedWriteable(filterQuery);
            }
            byte[] bytes = BytesReference.toBytes(out.bytes());
            LOGGER.debug("[lucene-dv-agg] convertFragment spec={} filterQuery={} bytes={}", encoded, filterQuery, bytes.length);
            return bytes;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize Lucene doc_values agg fragment", e);
        }
    }

    /**
     * Walks down to find an Aggregate (Calcite {@link Aggregate} or {@code OpenSearchAggregate})
     * and extracts the user-facing call names. These become the Arrow output column names so
     * the coordinator's reduce sink sees the schema it expects.
     */
    private static List<String> extractAggCallNames(RelNode root) {
        RelNode current = root;
        while (current != null) {
            if (current instanceof Aggregate agg) {
                List<String> names = new ArrayList<>(agg.getAggCallList().size());
                for (AggregateCall call : agg.getAggCallList()) {
                    names.add(call.getName());
                }
                return names;
            }
            if (current.getInputs().isEmpty()) break;
            current = current.getInputs().getFirst();
        }
        return List.of();
    }

    @Override
    public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
        // Lucene-as-driver count fragments DO go through the partial-agg split — the driver's
        // FragmentConversionDriver invokes convertFragment on the input subtree (the
        // TableScan / Filter, no Aggregate above), then attachPartialAggOnTop on the
        // OpenSearchAggregate node. Without this rewrite, innerBytes carries an empty
        // columnNames list (extractAggCallNames found no Aggregate in the input) and the
        // data-node Lucene exec engine emits a 0-column Arrow batch — the coordinator
        // reduce sink then stalls waiting for the count column.
        //
        // Strategy: re-decode innerBytes' columnNames length-prefix (always present, possibly
        // empty), then preserve the remaining tail (hasFilter + optional QueryBuilder)
        // verbatim. Re-emit with the partialAggFragment's aggregate-call names as the new
        // columnNames. Avoids needing a NamedWriteableRegistry at coordinator-side conversion.
        if (!(partialAggFragment instanceof Aggregate agg)) {
            throw new IllegalStateException(
                "Lucene attachPartialAggOnTop expected an Aggregate fragment, got " + partialAggFragment.getClass().getSimpleName()
            );
        }
        // Engine-plan path first: engine-native-merge aggregates (approx_distinct) MUST run
        // through the compiled fragment so the shard emits intermediate state (HLL sketches)
        // — the count wire format below would emit finalized Int64 and break the reduce
        // schema contract. The shard engine strips the compiled plan to its PARTIAL half.
        if (ShardAggregationEngineHolder.isAvailable()) {
            GeneralDvShape general = extractGeneralDvShape(agg);
            if (general != null) {
                return convertDvPlanFragment(general);
            }
        }
        // Doc_values group-by: re-encode the whole prefix as the wire-v2 spec, preserving the
        // filter tail from innerBytes (same tail-splice as the count path below).
        DocValuesAggShape dvShape = extractDocValuesAggShape(agg);
        if (dvShape != null) {
            List<String> encoded = new ArrayList<>();
            encoded.add(DV_AGG_MARKER);
            encoded.add(Integer.toString(dvShape.inputColumns().size()));
            encoded.addAll(dvShape.inputColumns());
            encoded.add(Integer.toString(dvShape.groupColumns().size()));
            encoded.addAll(dvShape.groupColumns());
            encoded.add(Integer.toString(dvShape.aggEntries().size()));
            for (AggSpecEntry entry : dvShape.aggEntries()) {
                encoded.add(entry.function());
                encoded.add(entry.inputColumn() == null ? "" : entry.inputColumn());
                encoded.add(entry.outputName());
            }
            return spliceColumnNames(encoded, innerBytes, "[lucene-dv-agg] attachPartialAggOnTop");
        }
        List<String> columnNames = new ArrayList<>(agg.getAggCallList().size());
        for (AggregateCall call : agg.getAggCallList()) {
            columnNames.add(call.getName());
        }

        return spliceColumnNames(columnNames, innerBytes, "[lucene-count] attachPartialAggOnTop");
    }

    /**
     * Replaces innerBytes' leading columnNames StringCollection with {@code columnNames},
     * preserving the {@code [hasFilter][QueryBuilder?]} tail verbatim. Avoids needing a
     * NamedWriteableRegistry at coordinator-side conversion.
     */
    private static byte[] spliceColumnNames(List<String> columnNames, byte[] innerBytes, String logTag) {
        int tailOffset;
        try (StreamInput in = StreamInput.wrap(innerBytes)) {
            in.readStringList(); // discard inner columnNames; we'll write the new prefix instead
            tailOffset = innerBytes.length - in.available();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to decode Lucene innerBytes during partial-agg attach", e);
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeStringCollection(columnNames);
            out.writeBytes(innerBytes, tailOffset, innerBytes.length - tailOffset);
            byte[] bytes = BytesReference.toBytes(out.bytes());
            LOGGER.debug("{} columnNames={} bytes={}", logTag, columnNames, bytes.length);
            return bytes;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize Lucene-driver partial-agg bytes", e);
        }
    }

    @Override
    public WireFormat wireFormat() {
        // convertFragment emits a custom NamedWriteable wire format ([columnNames][hasFilter]
        // [BoolQueryBuilder]?), not self-describing. The orchestrator queries this so it
        // knows to emit a separate schema-only stub via convertSchemaOnlyRead for the
        // coordinator's reduce-sink partition registration.
        return WireFormat.OPAQUE;
    }

    /**
     * Substrait stub describing the count fragment's output partition: one
     * {@code Plan{Read{named_table; base_schema}}} carrying the partition's named-table id
     * and column types. Mirrors {@code DataFusionFragmentConvertor.convertSchemaOnlyRead} —
     * same proto shape, decoded by the same Rust {@code derive_schema_from_partial_plan} on
     * the coordinator.
     *
     * <p>In production (selector with default {@code prefer_metadata_driver=true}) the only
     * Lucene plans reaching this method are the Aggregate-rooted count fast path, where the
     * stub describes a single {@code I64 NOT NULL} column per aggregate call. Tests that pin
     * {@code prefer=false} keep both alternatives — the Lucene plan there can be Filter-rooted
     * over the upstream scan rowType, which is why {@link #toSubstraitType} maps a few extra
     * primitives. Those bytes are never dispatched (the data node picks the peer alternative);
     * the mapping exists so the test path doesn't blow up at conversion.
     */
    @Override
    public byte[] convertSchemaOnlyRead(int childStageId, RelDataType rowType) {
        // Struct-level nullability stays REQUIRED (the row itself is always present); per-field
        // nullability is encoded inside each Type via toSubstraitType. Declared per-field
        // nullability MUST match what LuceneSearchExecEngine.buildSchema produces — Lucene's
        // count emission uses nullable Int64, so the stub's columns must say NULLABLE too. A
        // mismatch here used to silently hang at the partition stream (Rust registers a
        // NOT-NULL partition, runtime batches arrive nullable, drain stalls).
        Type.Struct.Builder structBuilder = Type.Struct.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED);
        NamedStruct.Builder namedStructBuilder = NamedStruct.newBuilder();
        for (RelDataTypeField field : rowType.getFieldList()) {
            namedStructBuilder.addNames(field.getName());
            structBuilder.addTypes(toSubstraitType(field.getType()));
        }
        namedStructBuilder.setStruct(structBuilder.build());

        ReadRel readRel = ReadRel.newBuilder()
            .setNamedTable(ReadRel.NamedTable.newBuilder().addNames("input-" + childStageId).build())
            .setBaseSchema(namedStructBuilder.build())
            .build();
        Rel inputRel = Rel.newBuilder().setRead(readRel).build();
        PlanRel planRel = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder().setInput(inputRel).addAllNames(rowType.getFieldNames()).build())
            .build();

        byte[] bytes = Plan.newBuilder().addRelations(planRel).build().toByteArray();
        LOGGER.debug(
            "[lucene-count] convertSchemaOnlyRead stage={} fields={} bytes={}",
            childStageId,
            rowType.getFieldNames(),
            bytes.length
        );
        return bytes;
    }

    /**
     * Minimal Calcite→Substrait type mapper for the schema-only Read. Covers the count
     * fast path (BIGINT) plus the few primitives a non-driver Lucene plan's row type can
     * carry (text/keyword → string, numerics, boolean). The result is only used for
     * coordinator-side partition registration; the bytes never round-trip back to a
     * Calcite type.
     *
     * <p><b>Nullability:</b> Calcite's COUNT aggregate types as BIGINT NOT NULL, but Lucene's
     * runtime emits a nullable Int64 column ({@code LuceneSearchExecEngine.buildSchema}
     * builds {@code FieldType(true, Int(64,true), null)} — the leading {@code true} is
     * nullable). The Substrait stub MUST reflect the producer's actual runtime schema, not
     * the Calcite logical type, otherwise the Rust-side partition stream registers as
     * NOT-NULL and silently stalls when nullable batches arrive. Force nullable for now;
     * when the driver supports more shapes, this will need a per-column source-of-truth.
     *
     * <p>TODO: when Lucene-driver shapes beyond COUNT land (group-by-count keys), wire in a
     * proper Calcite→Substrait converter so the stub describes real producer schemas.
     */
    private static Type toSubstraitType(RelDataType type) {
        // Always nullable to match LuceneSearchExecEngine.buildSchema's output. See class doc.
        Type.Nullability n = Type.Nullability.NULLABILITY_NULLABLE;
        return switch (type.getSqlTypeName()) {
            case BIGINT -> Type.newBuilder().setI64(Type.I64.newBuilder().setNullability(n)).build();
            case INTEGER -> Type.newBuilder().setI32(Type.I32.newBuilder().setNullability(n)).build();
            case SMALLINT -> Type.newBuilder().setI16(Type.I16.newBuilder().setNullability(n)).build();
            case TINYINT -> Type.newBuilder().setI8(Type.I8.newBuilder().setNullability(n)).build();
            case BOOLEAN -> Type.newBuilder().setBool(Type.Boolean.newBuilder().setNullability(n)).build();
            case DOUBLE -> Type.newBuilder().setFp64(Type.FP64.newBuilder().setNullability(n)).build();
            case FLOAT, REAL -> Type.newBuilder().setFp32(Type.FP32.newBuilder().setNullability(n)).build();
            case VARCHAR, CHAR -> Type.newBuilder().setString(Type.String.newBuilder().setNullability(n)).build();
            // Date/timestamp columns ride the wire as Timestamp(ms) (ColumnKind.TIMESTAMP in
            // the dv scan). PrecisionTimestamp(3) = milliseconds — matches the Calcite
            // TIMESTAMP(3) the engine passthrough declares; the deprecated unparameterized
            // Timestamp is fixed at µs and would mis-declare the partition.
            case DATE, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> Type.newBuilder()
                .setPrecisionTimestamp(Type.PrecisionTimestamp.newBuilder().setPrecision(3).setNullability(n))
                .build();
            case VARBINARY, BINARY -> Type.newBuilder().setBinary(Type.Binary.newBuilder().setNullability(n)).build();
            default -> throw new IllegalStateException(
                "Lucene convertSchemaOnlyRead: unmapped Calcite type " + type.getSqlTypeName() + " for field of type " + type
            );
        };
    }

    /**
     * Walks the linear input chain looking for any Calcite {@link Filter} (covers both
     * {@link OpenSearchFilter} and the plain {@code LogicalFilter} that
     * {@code FragmentConversionDriver.strip} produces once annotation resolution unwraps the
     * filter's condition into native predicate calls).
     */
    private static Filter findFilter(RelNode node) {
        RelNode current = node;
        while (current != null) {
            if (current instanceof Filter filter) return filter;
            if (current.getInputs().isEmpty()) return null;
            current = current.getInputs().getFirst();
        }
        return null;
    }

    /**
     * Returns the field-storage info for a filter's child operator. When the filter is a
     * native {@link OpenSearchFilter} this is just its own {@code getOutputFieldStorage()};
     * for a plain {@code LogicalFilter} produced by {@code strip()}, walk the input chain to
     * the nearest {@link OpenSearchRelNode} (the TableScan) and use its storage. Per-leaf
     * serializers consult this list to resolve column references back to their backing fields.
     */
    private static List<FieldStorageInfo> findFieldStorage(Filter filter) {
        if (filter instanceof OpenSearchFilter osf) {
            return osf.getOutputFieldStorage();
        }
        RelNode current = filter.getInput();
        while (current != null) {
            if (current instanceof OpenSearchRelNode osNode) {
                return osNode.getOutputFieldStorage();
            }
            if (current.getInputs().isEmpty()) break;
            current = current.getInputs().getFirst();
        }
        // Every Lucene-driver fragment has an OpenSearchTableScan ancestor by construction
        // (the table-scan rule wraps it before forking). If we got here, FragmentConversionDriver
        // produced an unexpected shape — fail loud so the planner bug is visible at conversion
        // time, not later when a serializer NPEs on missing field storage.
        throw new IllegalStateException("Lucene-driver filter has no OpenSearchRelNode ancestor: " + filter);
    }

    /**
     * Recursively converts a filter condition RexNode to a {@link QueryBuilder}. Mirrors
     * {@link LuceneSubtreeConvertor#toQueryBuilder} — same boolean structure handling
     * (AND→MUST, OR→SHOULD, NOT→MUST_NOT), same per-leaf serializer lookup. The duplication
     * is intentional: the delegation flow operates on a {@code DelegatedSubtreeConvertor}
     * SPI typed for serialized-bytes output, while the driver flow operates on
     * {@link FragmentConvertor} typed for whole-fragment serialization. Sharing the leaf
     * logic via a shared helper would be a follow-up cleanup.
     */
    private QueryBuilder toQueryBuilder(RexNode node, List<FieldStorageInfo> fieldStorage) {
        if (node instanceof AnnotatedPredicate ap) {
            node = ap.unwrap();
        }
        if (node instanceof RexCall call) {
            switch (call.getKind()) {
                case AND: {
                    BoolQueryBuilder b = new BoolQueryBuilder();
                    for (RexNode child : call.getOperands()) {
                        b.must(toQueryBuilder(child, fieldStorage));
                    }
                    return b;
                }
                case OR: {
                    BoolQueryBuilder b = new BoolQueryBuilder();
                    for (RexNode child : call.getOperands()) {
                        b.should(toQueryBuilder(child, fieldStorage));
                    }
                    return b;
                }
                case NOT: {
                    BoolQueryBuilder b = new BoolQueryBuilder();
                    b.mustNot(toQueryBuilder(call.getOperands().get(0), fieldStorage));
                    return b;
                }
                default:
                    return leafToQueryBuilder(call, fieldStorage);
            }
        }
        throw new IllegalStateException("Unexpected RexNode in Lucene-driver filter condition: " + node);
    }

    private QueryBuilder leafToQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        ScalarFunction fn = ScalarFunction.fromSqlOperatorWithFallback(call.getOperator());
        if (fn == null) {
            throw new IllegalStateException("Unrecognized operator in Lucene-driver filter: " + call.getOperator());
        }
        DelegatedPredicateSerializer serializer = leafSerializers.get(fn);
        if (serializer == null) {
            throw new IllegalStateException("No Lucene serializer for [" + fn + "] in driver-mode filter");
        }
        return ((AbstractQuerySerializer) serializer).buildQueryBuilder(call, fieldStorage);
    }
}
