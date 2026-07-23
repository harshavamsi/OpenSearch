/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.ShardAggregationEngine;
import org.opensearch.analytics.spi.ShardAggregationEngine.AggCall;
import org.opensearch.analytics.spi.ShardAggregationEngine.AggFunction;
import org.opensearch.analytics.spi.ShardAggregationEngine.AggSpec;
import org.opensearch.analytics.spi.ShardAggregationEngine.ColumnKind;
import org.opensearch.analytics.spi.ShardAggregationEngine.InputColumn;
import org.opensearch.analytics.spi.ShardAggregationEngineHolder;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.ReorganizingLongHash;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * ClickBench query definitions for the doc_values legs of the full three-way benchmark.
 * Maps each of the 43 standard queries to (a) a dv+arrow execution — engine plan bytes or
 * ordinal-first AggSpec — where the shard-fragment machinery can express it, (b) a dv-java
 * reference where the classic tier can (single numeric key / global aggregates), and (c) an
 * honest ineligibility reason otherwise.
 *
 * <p>Filters on numeric columns compile to {@link LongPoint} range queries (points are
 * indexed for AdvEngineID/CounterID/... in the shared index); keyword not-equals-empty filters use
 * the term-vs-empty exclusion. LIKE/regexp/CASE/HAVING/row-returning shapes are out of dv
 * scope and report why.
 *
 * @opensearch.internal
 */
final class ClickBenchDvQueries {

    private ClickBenchDvQueries() {}

    /** Numeric dv columns present in the shared index (built from .bin extracts). */
    static final List<String> NUM_COLS = List.of(
        "AdvEngineID",
        "UserID",
        "WatchID",
        "ClientIP",
        "IsRefresh",
        "RegionID",
        "CounterID",
        "ResolutionWidth",
        "EventDate",
        "SearchEngineID",
        "MobilePhone",
        "EventTime",
        "DontCountHits",
        "IsLink",
        "IsDownload",
        "URLHash",
        "WindowClientWidth",
        "WindowClientHeight",
        "TraficSourceID"
    );
    /** Keyword dv columns (from .strbin extracts). */
    static final List<String> STR_COLS = List.of("SearchPhrase", "MobilePhoneModel", "URL", "Title", "Referer");
    /** Numeric columns that also get LongPoint index structures (used in WHERE clauses). */
    static final List<String> POINT_COLS = List.of("AdvEngineID", "CounterID", "EventDate", "IsRefresh", "DontCountHits", "IsLink", "UserID");

    // ---------------------------------------------------------------------
    // dv+arrow: one entry per expressible query
    // ---------------------------------------------------------------------

    interface DvQuery {
        EngineResultStream run(org.opensearch.be.lucene.DocValuesAggregationExecutor executor, IndexSearcher searcher, BufferAllocator alloc)
            throws IOException;

        /** True when output row count is directly comparable to the parquet SQL's. */
        default boolean comparableToParquet() {
            return true;
        }
    }

    /** Plan-bytes query with an optional Lucene filter. */
    private record PlanQuery(Function<RelDataTypeFactory, RelNode> fragment, List<InputColumn> columns, Query filter, boolean comparable)
        implements
            DvQuery {
        @Override
        public EngineResultStream run(
            org.opensearch.be.lucene.DocValuesAggregationExecutor executor,
            IndexSearcher searcher,
            BufferAllocator alloc
        ) throws IOException {
            ShardAggregationEngine engine = ShardAggregationEngineHolder.get();
            byte[] plan = engine.compileFragment(fragment.apply(new JavaTypeFactoryImpl()));
            return executor.execute(searcher, filter, plan, columns, alloc, 0L);
        }

        @Override
        public boolean comparableToParquet() {
            return comparable;
        }
    }

    /** Ordinal-first keyword group-by via the v2 spec entry. */
    private record SpecQuery(AggSpec spec, Query filter, boolean comparable) implements DvQuery {
        @Override
        public EngineResultStream run(
            org.opensearch.be.lucene.DocValuesAggregationExecutor executor,
            IndexSearcher searcher,
            BufferAllocator alloc
        ) throws IOException {
            return executor.execute(searcher, filter, spec, alloc, 0L);
        }

        @Override
        public boolean comparableToParquet() {
            return comparable;
        }
    }

    /**
     * Returns the dv+arrow execution for 1-based query {@code n}, or null when ineligible.
     * LIMIT/ORDER BY tails are dropped (full grouped result returned — the coordinator would
     * topN); row counts stay comparable because ClickBench LIMITs cut presentation, not
     * grouping... except the LIMIT 10 top-N queries, where comparability is marked false and
     * only success/latency is reported.
     */
    static DvQuery forQuery(int n) {
        return switch (n) {
            // q1 count(*) — count fast path territory but run through the engine for timing.
            case 1 -> plan(tf -> globalAgg(tf, List.of(count("c")), "AdvEngineID"), longCols("AdvEngineID"), new MatchAllDocsQuery(), true);
            case 2 -> plan(
                tf -> globalAgg(tf, List.of(count("c")), "AdvEngineID"),
                longCols("AdvEngineID"),
                LongPoint.newRangeQuery("AdvEngineID", 1, Long.MAX_VALUE),
                true
            );
            case 3 -> plan(
                tf -> globalAgg(tf, List.of(sum(tf, 0, "s"), count("c"), avg(tf, 1, "a")), "AdvEngineID", "ResolutionWidth"),
                longCols("AdvEngineID", "ResolutionWidth"),
                new MatchAllDocsQuery(),
                true
            );
            case 4 -> plan(tf -> globalAgg(tf, List.of(avg(tf, 0, "a")), "UserID"), longCols("UserID"), new MatchAllDocsQuery(), true);
            case 5 -> plan(
                tf -> globalAgg(tf, List.of(distinctCount(tf, 0, "u")), "UserID"),
                longCols("UserID"),
                new MatchAllDocsQuery(),
                true
            );
            case 6 -> plan(
                tf -> globalAggTyped(tf, List.of(distinctCount(tf, 0, "u")), col("SearchPhrase", SqlTypeName.VARCHAR)),
                List.of(new InputColumn("SearchPhrase", ColumnKind.KEYWORD)),
                new MatchAllDocsQuery(),
                true
            );
            case 7 -> plan(
                tf -> globalAgg(tf, List.of(min(tf, 0, "mn"), max(tf, 0, "mx")), "EventDate"),
                longCols("EventDate"),
                new MatchAllDocsQuery(),
                true
            );
            case 8 -> plan(
                tf -> groupByAgg(tf, 1, List.of(count("c")), col("AdvEngineID", SqlTypeName.BIGINT)),
                longCols("AdvEngineID"),
                LongPoint.newRangeQuery("AdvEngineID", 1, Long.MAX_VALUE),
                true
            );
            case 9 -> plan(
                tf -> groupByAgg(tf, 1, List.of(distinctCount(tf, 1, "u")), col("RegionID", SqlTypeName.BIGINT), col("UserID", SqlTypeName.BIGINT)),
                longCols("RegionID", "UserID"),
                new MatchAllDocsQuery(),
                false // LIMIT 10 in SQL
            );
            case 10 -> plan(
                tf -> groupByAgg(
                    tf,
                    1,
                    List.of(sum(tf, 1, "s"), count("c"), avg(tf, 2, "a"), distinctCount(tf, 3, "u")),
                    col("RegionID", SqlTypeName.BIGINT),
                    col("AdvEngineID", SqlTypeName.BIGINT),
                    col("ResolutionWidth", SqlTypeName.BIGINT),
                    col("UserID", SqlTypeName.BIGINT)
                ),
                longCols("RegionID", "AdvEngineID", "ResolutionWidth", "UserID"),
                new MatchAllDocsQuery(),
                false
            );
            case 11 -> plan(
                tf -> groupByAgg(tf, 1, List.of(distinctCount(tf, 1, "u")), col("MobilePhoneModel", SqlTypeName.VARCHAR), col("UserID", SqlTypeName.BIGINT)),
                List.of(new InputColumn("MobilePhoneModel", ColumnKind.KEYWORD), new InputColumn("UserID", ColumnKind.LONG)),
                nonEmpty("MobilePhoneModel"),
                false
            );
            case 12 -> plan(
                tf -> groupByAgg(
                    tf,
                    2,
                    List.of(distinctCount(tf, 2, "u")),
                    col("MobilePhone", SqlTypeName.BIGINT),
                    col("MobilePhoneModel", SqlTypeName.VARCHAR),
                    col("UserID", SqlTypeName.BIGINT)
                ),
                List.of(
                    new InputColumn("MobilePhone", ColumnKind.LONG),
                    new InputColumn("MobilePhoneModel", ColumnKind.KEYWORD),
                    new InputColumn("UserID", ColumnKind.LONG)
                ),
                nonEmpty("MobilePhoneModel"),
                false
            );
            // q13/q14: single keyword key — ordinal-first spec path.
            case 13 -> spec(
                new AggSpec(List.of("SearchPhrase"), List.of("SearchPhrase"), List.of(new AggCall(AggFunction.COUNT, null, "c"))),
                nonEmpty("SearchPhrase"),
                false
            );
            case 14 -> plan(
                tf -> groupByAgg(tf, 1, List.of(distinctCount(tf, 1, "u")), col("SearchPhrase", SqlTypeName.VARCHAR), col("UserID", SqlTypeName.BIGINT)),
                List.of(new InputColumn("SearchPhrase", ColumnKind.KEYWORD), new InputColumn("UserID", ColumnKind.LONG)),
                nonEmpty("SearchPhrase"),
                false
            );
            case 15 -> plan(
                tf -> groupByAgg(tf, 2, List.of(count("c")), col("SearchEngineID", SqlTypeName.BIGINT), col("SearchPhrase", SqlTypeName.VARCHAR)),
                List.of(new InputColumn("SearchEngineID", ColumnKind.LONG), new InputColumn("SearchPhrase", ColumnKind.KEYWORD)),
                nonEmpty("SearchPhrase"),
                false
            );
            case 16 -> plan(
                tf -> groupByAgg(tf, 1, List.of(count("c")), col("UserID", SqlTypeName.BIGINT)),
                longCols("UserID"),
                new MatchAllDocsQuery(),
                false
            );
            case 17, 18 -> plan(
                tf -> groupByAgg(tf, 2, List.of(count("c")), col("UserID", SqlTypeName.BIGINT), col("SearchPhrase", SqlTypeName.VARCHAR)),
                List.of(new InputColumn("UserID", ColumnKind.LONG), new InputColumn("SearchPhrase", ColumnKind.KEYWORD)),
                new MatchAllDocsQuery(),
                false
            );
            // q19: extract(minute) — EventTime is epoch seconds; minute = (t/60)%60 as a
            // Project expression. Engine-evaluated.
            case 19 -> plan(tf -> q19Fragment(tf), longCols("UserID", "EventTime"), new MatchAllDocsQuery(), false);
            case 30 -> plan(tf -> q30Fragment(tf, 90), longCols("ResolutionWidth"), new MatchAllDocsQuery(), true);
            case 31 -> plan(
                tf -> groupByAgg(
                    tf,
                    2,
                    List.of(count("c"), sum(tf, 2, "s"), avg(tf, 3, "a")),
                    col("SearchEngineID", SqlTypeName.BIGINT),
                    col("ClientIP", SqlTypeName.BIGINT),
                    col("IsRefresh", SqlTypeName.BIGINT),
                    col("ResolutionWidth", SqlTypeName.BIGINT)
                ),
                longCols("SearchEngineID", "ClientIP", "IsRefresh", "ResolutionWidth"),
                nonEmpty("SearchPhrase"),
                false
            );
            case 32, 33 -> plan(
                tf -> groupByAgg(
                    tf,
                    2,
                    List.of(count("c"), sum(tf, 2, "s"), avg(tf, 3, "a")),
                    col("WatchID", SqlTypeName.BIGINT),
                    col("ClientIP", SqlTypeName.BIGINT),
                    col("IsRefresh", SqlTypeName.BIGINT),
                    col("ResolutionWidth", SqlTypeName.BIGINT)
                ),
                longCols("WatchID", "ClientIP", "IsRefresh", "ResolutionWidth"),
                n == 32 ? nonEmpty("SearchPhrase") : new MatchAllDocsQuery(),
                false
            );
            case 34, 35 -> spec(
                new AggSpec(List.of("URL"), List.of("URL"), List.of(new AggCall(AggFunction.COUNT, null, "c"))),
                new MatchAllDocsQuery(),
                false
            );
            case 36 -> plan(tf -> q36Fragment(tf), longCols("ClientIP"), new MatchAllDocsQuery(), false);
            case 37 -> spec(
                new AggSpec(List.of("URL"), List.of("URL"), List.of(new AggCall(AggFunction.COUNT, null, "c"))),
                q37Filter(),
                false
            );
            case 38 -> spec(
                new AggSpec(List.of("Title"), List.of("Title"), List.of(new AggCall(AggFunction.COUNT, null, "c"))),
                q38Filter(),
                false
            );
            case 41 -> plan(
                tf -> groupByAgg(tf, 2, List.of(count("c")), col("URLHash", SqlTypeName.BIGINT), col("EventDate", SqlTypeName.BIGINT)),
                longCols("URLHash", "EventDate"),
                q41Filter(),
                false
            );
            case 42 -> plan(
                tf -> groupByAgg(
                    tf,
                    2,
                    List.of(count("c")),
                    col("WindowClientWidth", SqlTypeName.BIGINT),
                    col("WindowClientHeight", SqlTypeName.BIGINT)
                ),
                longCols("WindowClientWidth", "WindowClientHeight"),
                q37Filter(),
                false
            );
            // ---- coverage-first additions (perf later) ----
            // q20: row-returning point lookup — decode UserID for matching docs, engine passthrough.
            case 20 -> plan(
                tf -> passthrough(tf, col("UserID", SqlTypeName.BIGINT)),
                longCols("UserID"),
                LongPoint.newExactQuery("UserID", 435090932899640449L),
                true
            );
            // q21: COUNT(*) WHERE URL LIKE '%google%' — Lucene wildcard filter.
            case 21 -> plan(
                tf -> globalAgg(tf, List.of(count("c")), "AdvEngineID"),
                longCols("AdvEngineID"),
                like("URL", "*google*"),
                true
            );
            // q22 (reduced): SearchPhrase group + COUNT, WHERE URL LIKE '%google%' AND phrase<>''.
            // MIN(VARCHAR) has no isthmus substrait binding; the filter+group dominates anyway.
            case 22 -> plan(
                tf -> groupByAgg(tf, 1, List.of(count("c")), col("SearchPhrase", SqlTypeName.VARCHAR)),
                List.of(new InputColumn("SearchPhrase", ColumnKind.KEYWORD)),
                and(like("URL", "*google*"), nonEmpty("SearchPhrase")),
                false
            );
            // q23 (reduced): Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND phrase<>'';
            // MIN(URL)/MIN(Title) dropped for the same isthmus binding gap.
            case 23 -> plan(
                tf -> groupByAgg(
                    tf,
                    1,
                    List.of(count("c"), distinctCount(tf, 1, "u")),
                    col("SearchPhrase", SqlTypeName.VARCHAR),
                    col("UserID", SqlTypeName.BIGINT)
                ),
                List.of(new InputColumn("SearchPhrase", ColumnKind.KEYWORD), new InputColumn("UserID", ColumnKind.LONG)),
                and(like("Title", "*Google*"), notLike("URL", "*.google.*"), nonEmpty("SearchPhrase")),
                false
            );
            // q24: SELECT * ... ORDER BY EventTime LIMIT 10 — decode a representative column set
            // (full 24-col SELECT * would work but bloats decode; noted in the cell).
            case 24 -> plan(
                tf -> sortLimit(
                    tf,
                    passthrough(
                        tf,
                        col("WatchID", SqlTypeName.BIGINT),
                        col("EventTime", SqlTypeName.BIGINT),
                        col("URL", SqlTypeName.VARCHAR),
                        col("Title", SqlTypeName.VARCHAR)
                    ),
                    1,
                    10
                ),
                List.of(
                    new InputColumn("WatchID", ColumnKind.LONG),
                    new InputColumn("EventTime", ColumnKind.LONG),
                    new InputColumn("URL", ColumnKind.KEYWORD),
                    new InputColumn("Title", ColumnKind.KEYWORD)
                ),
                like("URL", "*google*"),
                false
            );
            // q25-q27: SearchPhrase row-returning sorts.
            case 25 -> plan(
                tf -> sortLimit(tf, passthrough(tf, col("SearchPhrase", SqlTypeName.VARCHAR), col("EventTime", SqlTypeName.BIGINT)), 1, 10),
                List.of(new InputColumn("SearchPhrase", ColumnKind.KEYWORD), new InputColumn("EventTime", ColumnKind.LONG)),
                nonEmpty("SearchPhrase"),
                false
            );
            case 26 -> plan(
                tf -> sortLimit(tf, passthrough(tf, col("SearchPhrase", SqlTypeName.VARCHAR)), 0, 10),
                List.of(new InputColumn("SearchPhrase", ColumnKind.KEYWORD)),
                nonEmpty("SearchPhrase"),
                false
            );
            case 27 -> plan(
                tf -> sortLimit2(tf, passthrough(tf, col("SearchPhrase", SqlTypeName.VARCHAR), col("EventTime", SqlTypeName.BIGINT)), 1, 0, 10),
                List.of(new InputColumn("SearchPhrase", ColumnKind.KEYWORD), new InputColumn("EventTime", ColumnKind.LONG)),
                nonEmpty("SearchPhrase"),
                false
            );
            // q28: CounterID group + AVG(length(URL)) + COUNT HAVING COUNT > 100000.
            case 28 -> plan(tf -> q28Fragment(tf), List.of(
                new InputColumn("CounterID", ColumnKind.LONG),
                new InputColumn("URL", ColumnKind.KEYWORD)
            ), nonEmpty("URL"), false);
            // q39: URL group under compound filter incl. IsLink/IsDownload.
            case 39 -> spec(
                new AggSpec(List.of("URL"), List.of("URL"), List.of(new AggCall(AggFunction.COUNT, null, "c"))),
                q39Filter(),
                false
            );
            // q40: CASE WHEN (SearchEngineID=0 AND AdvEngineID=0) THEN Referer ELSE '' as a group key.
            case 40 -> plan(tf -> q40Fragment(tf), List.of(
                new InputColumn("TraficSourceID", ColumnKind.LONG),
                new InputColumn("SearchEngineID", ColumnKind.LONG),
                new InputColumn("AdvEngineID", ColumnKind.LONG),
                new InputColumn("Referer", ColumnKind.KEYWORD),
                new InputColumn("URL", ColumnKind.KEYWORD)
            ), q39Filter(), false);
            // q43: minute bucketing over EventTime under the q37-style filter window.
            case 43 -> plan(tf -> q43Fragment(tf), longCols("EventTime"), q43Filter(), false);
            default -> null;
        };
    }

    static String ineligibleReason(int n) {
        return switch (n) {
            case 29 -> "REGEXP_REPLACE in GROUP BY key (isthmus mapping unverified; excluded in mustang correctness too)";
            default -> "";
        };
    }

    // ---------------------------------------------------------------------
    // dv-java: classic-tier shapes only
    // ---------------------------------------------------------------------

    interface JavaQuery {
        void run(DirectoryReader reader, IndexSearcher searcher) throws IOException;
    }

    static JavaQuery javaForQuery(int n) {
        return switch (n) {
            case 1 -> (reader, searcher) -> searcher.count(new MatchAllDocsQuery());
            case 2 -> (reader, searcher) -> searcher.count(LongPoint.newRangeQuery("AdvEngineID", 1, Long.MAX_VALUE));
            case 3 -> (reader, searcher) -> javaGlobalSum(reader, searcher, new MatchAllDocsQuery(), "AdvEngineID", "ResolutionWidth");
            case 4 -> (reader, searcher) -> javaGlobalSum(reader, searcher, new MatchAllDocsQuery(), "UserID");
            case 7 -> (reader, searcher) -> javaGlobalSum(reader, searcher, new MatchAllDocsQuery(), "EventDate");
            case 8 -> (reader, searcher) -> javaGroupCount(reader, searcher, LongPoint.newRangeQuery("AdvEngineID", 1, Long.MAX_VALUE), "AdvEngineID");
            case 16 -> (reader, searcher) -> javaGroupCount(reader, searcher, new MatchAllDocsQuery(), "UserID");
            default -> null;
        };
    }

    // ---------------------------------------------------------------------
    // Filters
    // ---------------------------------------------------------------------

    private static Query nonEmpty(String keywordField) {
        // term != '' — a BooleanQuery MUST_NOT over the empty term with a match-all base.
        org.apache.lucene.search.BooleanQuery.Builder b = new org.apache.lucene.search.BooleanQuery.Builder();
        b.add(new MatchAllDocsQuery(), org.apache.lucene.search.BooleanClause.Occur.MUST);
        b.add(
            new org.apache.lucene.search.TermQuery(new org.apache.lucene.index.Term(keywordField, new BytesRef(""))),
            org.apache.lucene.search.BooleanClause.Occur.MUST_NOT
        );
        return new org.apache.lucene.search.ConstantScoreQuery(b.build());
    }

    /** CounterID=62 AND EventDate in [15887,15917] AND DontCountHits=0 AND IsRefresh=0. */
    private static Query q37Filter() {
        org.apache.lucene.search.BooleanQuery.Builder b = new org.apache.lucene.search.BooleanQuery.Builder();
        b.add(LongPoint.newExactQuery("CounterID", 62), org.apache.lucene.search.BooleanClause.Occur.MUST);
        b.add(LongPoint.newRangeQuery("EventDate", 15887, 15917), org.apache.lucene.search.BooleanClause.Occur.MUST);
        b.add(LongPoint.newExactQuery("DontCountHits", 0), org.apache.lucene.search.BooleanClause.Occur.MUST);
        b.add(LongPoint.newExactQuery("IsRefresh", 0), org.apache.lucene.search.BooleanClause.Occur.MUST);
        return new org.apache.lucene.search.ConstantScoreQuery(b.build());
    }

    private static Query q38Filter() {
        return q37Filter();
    }

    /** CounterID=62 AND EventDate in range AND IsRefresh=0 (q41 drops DontCountHits). */
    private static Query q41Filter() {
        org.apache.lucene.search.BooleanQuery.Builder b = new org.apache.lucene.search.BooleanQuery.Builder();
        b.add(LongPoint.newExactQuery("CounterID", 62), org.apache.lucene.search.BooleanClause.Occur.MUST);
        b.add(LongPoint.newRangeQuery("EventDate", 15887, 15917), org.apache.lucene.search.BooleanClause.Occur.MUST);
        b.add(LongPoint.newExactQuery("IsRefresh", 0), org.apache.lucene.search.BooleanClause.Occur.MUST);
        return new org.apache.lucene.search.ConstantScoreQuery(b.build());
    }

    // ---------------------------------------------------------------------
    // Fragment builders
    // ---------------------------------------------------------------------

    record Col(String name, SqlTypeName type) {}

    private static Col col(String name, SqlTypeName type) {
        return new Col(name, type);
    }

    private static List<InputColumn> longCols(String... names) {
        List<InputColumn> cols = new ArrayList<>(names.length);
        for (String n : names) {
            cols.add(new InputColumn(n, ColumnKind.LONG));
        }
        return cols;
    }

    private static DvQuery plan(Function<RelDataTypeFactory, RelNode> fragment, List<InputColumn> columns, Query filter, boolean comparable) {
        return new PlanQuery(fragment, columns, filter, comparable);
    }

    private static DvQuery spec(AggSpec s, Query filter, boolean comparable) {
        return new SpecQuery(s, filter, comparable);
    }

    private static OpenSearchStageInputScan scan(RelDataTypeFactory tf, Col... cols) {
        RelOptCluster cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), new RexBuilder(tf));
        RelDataTypeFactory.Builder b = tf.builder();
        for (Col c : cols) {
            b.add(c.name(), tf.createTypeWithNullability(tf.createSqlType(c.type()), true));
        }
        return new OpenSearchStageInputScan(cluster, cluster.traitSet(), 0, b.build(), List.of(), List.of());
    }

    /** Global aggregate (empty group set) over long columns. */
    private static RelNode globalAgg(RelDataTypeFactory tf, List<AggregateCall> calls, String... longCols) {
        Col[] cols = new Col[longCols.length];
        for (int i = 0; i < longCols.length; i++) {
            cols[i] = col(longCols[i], SqlTypeName.BIGINT);
        }
        return LogicalAggregate.create(scan(tf, cols), List.of(), ImmutableBitSet.of(), null, calls);
    }

    private static RelNode globalAggTyped(RelDataTypeFactory tf, List<AggregateCall> calls, Col... cols) {
        return LogicalAggregate.create(scan(tf, cols), List.of(), ImmutableBitSet.of(), null, calls);
    }

    /** GROUP BY the first {@code groupArity} columns with the given calls. */
    private static RelNode groupByAgg(RelDataTypeFactory tf, int groupArity, List<AggregateCall> calls, Col... cols) {
        ImmutableBitSet.Builder groups = ImmutableBitSet.builder();
        for (int i = 0; i < groupArity; i++) {
            groups.set(i);
        }
        return LogicalAggregate.create(scan(tf, cols), List.of(), groups.build(), null, calls);
    }

    /** q19: GROUP BY UserID, (EventTime/60)%60, then COUNT — minute bucket as a Project expr. */
    private static RelNode q19Fragment(RelDataTypeFactory tf) {
        OpenSearchStageInputScan s = scan(tf, col("UserID", SqlTypeName.BIGINT), col("EventTime", SqlTypeName.BIGINT));
        RexBuilder rex = s.getCluster().getRexBuilder();
        RexNode userId = rex.makeInputRef(s, 0);
        RexNode eventTime = rex.makeInputRef(s, 1);
        RexNode sixty = rex.makeExactLiteral(java.math.BigDecimal.valueOf(60));
        // DIVIDE (not DIVIDE_INTEGER): isthmus has no substrait mapping for "/INT", and
        // i64/i64 division is already truncating in DataFusion.
        RexNode minute = rex.makeCall(
            SqlStdOperatorTable.MOD,
            rex.makeCall(SqlStdOperatorTable.DIVIDE, eventTime, sixty),
            sixty
        );
        LogicalProject project = LogicalProject.create(s, List.of(), List.of(userId, minute), List.of("UserID", "m"));
        return LogicalAggregate.create(
            project,
            List.of(),
            ImmutableBitSet.of(0, 1),
            null,
            List.of(count("c"))
        );
    }

    /** q30: SUM(ResolutionWidth + k) for k in [0, n) — Project of n expressions then n SUMs. */
    private static RelNode q30Fragment(RelDataTypeFactory tf, int n) {
        OpenSearchStageInputScan s = scan(tf, col("ResolutionWidth", SqlTypeName.BIGINT));
        RexBuilder rex = s.getCluster().getRexBuilder();
        RexNode w = rex.makeInputRef(s, 0);
        List<RexNode> exprs = new ArrayList<>(n);
        List<String> names = new ArrayList<>(n);
        for (int k = 0; k < n; k++) {
            exprs.add(k == 0 ? w : rex.makeCall(SqlStdOperatorTable.PLUS, w, rex.makeExactLiteral(java.math.BigDecimal.valueOf(k))));
            names.add("w" + k);
        }
        LogicalProject project = LogicalProject.create(s, List.of(), exprs, names);
        RelDataType bigintNullable = tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.BIGINT), true);
        List<AggregateCall> sums = new ArrayList<>(n);
        for (int k = 0; k < n; k++) {
            sums.add(AggregateCall.create(SqlStdOperatorTable.SUM, false, List.of(k), -1, bigintNullable, "s" + k));
        }
        return LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(), null, sums);
    }

    /** Bare projection of the scan columns (row-returning shapes; engine passthrough). */
    private static RelNode passthrough(RelDataTypeFactory tf, Col... cols) {
        return scan(tf, cols);
    }

    /** ORDER BY column {@code sortCol} ASC LIMIT {@code fetch}. */
    private static RelNode sortLimit(RelDataTypeFactory tf, RelNode input, int sortCol, int fetch) {
        RexBuilder rex = input.getCluster().getRexBuilder();
        return LogicalSort.create(
            input,
            RelCollations.of(new RelFieldCollation(sortCol)),
            null,
            rex.makeExactLiteral(java.math.BigDecimal.valueOf(fetch))
        );
    }

    /** ORDER BY sortCol1, sortCol2 LIMIT fetch. */
    private static RelNode sortLimit2(RelDataTypeFactory tf, RelNode input, int sortCol1, int sortCol2, int fetch) {
        RexBuilder rex = input.getCluster().getRexBuilder();
        return LogicalSort.create(
            input,
            RelCollations.of(new RelFieldCollation(sortCol1), new RelFieldCollation(sortCol2)),
            null,
            rex.makeExactLiteral(java.math.BigDecimal.valueOf(fetch))
        );
    }

    private static Query like(String field, String wildcardPattern) {
        return new org.apache.lucene.search.WildcardQuery(new org.apache.lucene.index.Term(field, wildcardPattern));
    }

    private static Query notLike(String field, String wildcardPattern) {
        org.apache.lucene.search.BooleanQuery.Builder b = new org.apache.lucene.search.BooleanQuery.Builder();
        b.add(new MatchAllDocsQuery(), org.apache.lucene.search.BooleanClause.Occur.MUST);
        b.add(like(field, wildcardPattern), org.apache.lucene.search.BooleanClause.Occur.MUST_NOT);
        return new org.apache.lucene.search.ConstantScoreQuery(b.build());
    }

    private static Query and(Query... queries) {
        org.apache.lucene.search.BooleanQuery.Builder b = new org.apache.lucene.search.BooleanQuery.Builder();
        for (Query q : queries) {
            b.add(q, org.apache.lucene.search.BooleanClause.Occur.MUST);
        }
        return new org.apache.lucene.search.ConstantScoreQuery(b.build());
    }

    /** MIN over a VARCHAR column — unused: isthmus has no substrait binding for MIN(VARCHAR). */
    @SuppressWarnings("unused")
    private static AggregateCall minStr(RelDataTypeFactory tf, int arg, String name) {
        return AggregateCall.create(
            SqlStdOperatorTable.MIN,
            false,
            List.of(arg),
            -1,
            tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.VARCHAR), true),
            name
        );
    }

    /** q28: GROUP BY CounterID + AVG(length(URL)) + COUNT(*) HAVING COUNT(*) > 100000. */
    private static RelNode q28Fragment(RelDataTypeFactory tf) {
        OpenSearchStageInputScan s = scan(tf, col("CounterID", SqlTypeName.BIGINT), col("URL", SqlTypeName.VARCHAR));
        RexBuilder rex = s.getCluster().getRexBuilder();
        RexNode counter = rex.makeInputRef(s, 0);
        RexNode urlLen = rex.makeCast(
            tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.BIGINT), true),
            rex.makeCall(SqlStdOperatorTable.CHAR_LENGTH, rex.makeInputRef(s, 1))
        );
        LogicalProject project = LogicalProject.create(s, List.of(), List.of(counter, urlLen), List.of("CounterID", "l"));
        RelNode agg = LogicalAggregate.create(
            project,
            List.of(),
            ImmutableBitSet.of(0),
            null,
            List.of(avg(tf, 1, "l"), count("c"))
        );
        RexBuilder rex2 = agg.getCluster().getRexBuilder();
        // HAVING c > 100000 — filter over the aggregate output (col 2 = c).
        RexNode having = rex2.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rex2.makeInputRef(agg, 2),
            rex2.makeExactLiteral(java.math.BigDecimal.valueOf(100_000))
        );
        return LogicalFilter.create(agg, having);
    }

    /** q40 (reduced): GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, CASE-src, URL + COUNT. */
    private static RelNode q40Fragment(RelDataTypeFactory tf) {
        OpenSearchStageInputScan s = scan(
            tf,
            col("TraficSourceID", SqlTypeName.BIGINT),
            col("SearchEngineID", SqlTypeName.BIGINT),
            col("AdvEngineID", SqlTypeName.BIGINT),
            col("Referer", SqlTypeName.VARCHAR),
            col("URL", SqlTypeName.VARCHAR)
        );
        RexBuilder rex = s.getCluster().getRexBuilder();
        RexNode cond = rex.makeCall(
            SqlStdOperatorTable.AND,
            rex.makeCall(SqlStdOperatorTable.EQUALS, rex.makeInputRef(s, 1), rex.makeExactLiteral(java.math.BigDecimal.ZERO)),
            rex.makeCall(SqlStdOperatorTable.EQUALS, rex.makeInputRef(s, 2), rex.makeExactLiteral(java.math.BigDecimal.ZERO))
        );
        RexNode src = rex.makeCall(
            SqlStdOperatorTable.CASE,
            cond,
            rex.makeInputRef(s, 3),
            rex.makeLiteral("")
        );
        LogicalProject project = LogicalProject.create(
            s,
            List.of(),
            List.of(rex.makeInputRef(s, 0), rex.makeInputRef(s, 1), rex.makeInputRef(s, 2), src, rex.makeInputRef(s, 4)),
            List.of("TraficSourceID", "SearchEngineID", "AdvEngineID", "Src", "Dst")
        );
        return LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(0, 1, 2, 3, 4), null, List.of(count("c")));
    }

    /** q43: GROUP BY (EventTime/60)*60 minute bucket + COUNT (DATE_FORMAT reduced to bucketing). */
    private static RelNode q43Fragment(RelDataTypeFactory tf) {
        OpenSearchStageInputScan s = scan(tf, col("EventTime", SqlTypeName.BIGINT));
        RexBuilder rex = s.getCluster().getRexBuilder();
        RexNode t = rex.makeInputRef(s, 0);
        RexNode sixty = rex.makeExactLiteral(java.math.BigDecimal.valueOf(60));
        RexNode bucket = rex.makeCall(
            SqlStdOperatorTable.MULTIPLY,
            rex.makeCall(SqlStdOperatorTable.DIVIDE, t, sixty),
            sixty
        );
        LogicalProject project = LogicalProject.create(s, List.of(), List.of(bucket), List.of("M"));
        return LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(0), null, List.of(count("c")));
    }

    /** q39: CounterID=62 AND date window AND IsRefresh=0 AND IsLink!=0 AND IsDownload=0. */
    private static Query q39Filter() {
        org.apache.lucene.search.BooleanQuery.Builder b = new org.apache.lucene.search.BooleanQuery.Builder();
        b.add(LongPoint.newExactQuery("CounterID", 62), org.apache.lucene.search.BooleanClause.Occur.MUST);
        b.add(LongPoint.newRangeQuery("EventDate", 15887, 15917), org.apache.lucene.search.BooleanClause.Occur.MUST);
        b.add(LongPoint.newExactQuery("IsRefresh", 0), org.apache.lucene.search.BooleanClause.Occur.MUST);
        b.add(LongPoint.newRangeQuery("IsLink", 1, Long.MAX_VALUE), org.apache.lucene.search.BooleanClause.Occur.MUST);
        b.add(LongPoint.newExactQuery("IsDownload", 0), org.apache.lucene.search.BooleanClause.Occur.MUST);
        return new org.apache.lucene.search.ConstantScoreQuery(b.build());
    }

    /** q43: CounterID=62 AND narrow date window AND IsRefresh=0 AND DontCountHits=0. */
    private static Query q43Filter() {
        org.apache.lucene.search.BooleanQuery.Builder b = new org.apache.lucene.search.BooleanQuery.Builder();
        b.add(LongPoint.newExactQuery("CounterID", 62), org.apache.lucene.search.BooleanClause.Occur.MUST);
        b.add(LongPoint.newRangeQuery("EventDate", 15900, 15901), org.apache.lucene.search.BooleanClause.Occur.MUST);
        b.add(LongPoint.newExactQuery("IsRefresh", 0), org.apache.lucene.search.BooleanClause.Occur.MUST);
        b.add(LongPoint.newExactQuery("DontCountHits", 0), org.apache.lucene.search.BooleanClause.Occur.MUST);
        return new org.apache.lucene.search.ConstantScoreQuery(b.build());
    }

    /** q36: GROUP BY ClientIP, ClientIP-1, ClientIP-2, ClientIP-3 + COUNT. */
    private static RelNode q36Fragment(RelDataTypeFactory tf) {
        OpenSearchStageInputScan s = scan(tf, col("ClientIP", SqlTypeName.BIGINT));
        RexBuilder rex = s.getCluster().getRexBuilder();
        RexNode ip = rex.makeInputRef(s, 0);
        List<RexNode> exprs = new ArrayList<>(4);
        List<String> names = new ArrayList<>(4);
        for (int k = 0; k < 4; k++) {
            exprs.add(k == 0 ? ip : rex.makeCall(SqlStdOperatorTable.MINUS, ip, rex.makeExactLiteral(java.math.BigDecimal.valueOf(k))));
            names.add("ip" + k);
        }
        LogicalProject project = LogicalProject.create(s, List.of(), exprs, names);
        return LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(0, 1, 2, 3), null, List.of(count("c")));
    }

    private static AggregateCall count(String name) {
        return AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            new JavaTypeFactoryImpl().createSqlType(SqlTypeName.BIGINT),
            name
        );
    }

    private static AggregateCall sum(RelDataTypeFactory tf, int arg, String name) {
        return AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(arg),
            -1,
            tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.BIGINT), true),
            name
        );
    }

    private static AggregateCall min(RelDataTypeFactory tf, int arg, String name) {
        return AggregateCall.create(
            SqlStdOperatorTable.MIN,
            false,
            List.of(arg),
            -1,
            tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.BIGINT), true),
            name
        );
    }

    private static AggregateCall max(RelDataTypeFactory tf, int arg, String name) {
        return AggregateCall.create(
            SqlStdOperatorTable.MAX,
            false,
            List.of(arg),
            -1,
            tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.BIGINT), true),
            name
        );
    }

    private static AggregateCall avg(RelDataTypeFactory tf, int arg, String name) {
        // Calcite's AVG return-type inference is ARG0 (BIGINT for BIGINT args); typing the
        // call DOUBLE trips the LogicalAggregate type assert. DataFusion still computes f64.
        return AggregateCall.create(
            SqlStdOperatorTable.AVG,
            false,
            List.of(arg),
            -1,
            tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.BIGINT), true),
            name
        );
    }

    private static AggregateCall distinctCount(RelDataTypeFactory tf, int arg, String name) {
        return AggregateCall.create(SqlStdOperatorTable.COUNT, true, List.of(arg), -1, tf.createSqlType(SqlTypeName.BIGINT), name);
    }

    // ---------------------------------------------------------------------
    // dv-java reference impls (classic-tier shapes)
    // ---------------------------------------------------------------------

    private static void javaGlobalSum(DirectoryReader reader, IndexSearcher searcher, Query query, String... columns) throws IOException {
        int batch = org.opensearch.be.lucene.DocValuesAggregationExecutor.BATCH_SIZE;
        int[] docs = new int[batch];
        long[] vals = new long[batch];
        var weight = searcher.createWeight(searcher.rewrite(query), org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES, 1f);
        long blackhole = 0;
        for (var leaf : reader.leaves()) {
            var scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            var dvs = new org.apache.lucene.index.NumericDocValues[columns.length];
            for (int c = 0; c < columns.length; c++) {
                dvs[c] = leaf.reader().getNumericDocValues(columns[c]);
            }
            var it = scorer.iterator();
            int size = 0;
            for (int doc = it.nextDoc(); doc != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                docs[size++] = doc;
                if (size == batch) {
                    for (var dv : dvs) {
                        dv.longValues(size, docs, 0, vals, 0, 0L);
                        for (int i = 0; i < size; i++) {
                            blackhole += vals[i];
                        }
                    }
                    size = 0;
                }
            }
            if (size > 0) {
                for (var dv : dvs) {
                    dv.longValues(size, docs, 0, vals, 0, 0L);
                    for (int i = 0; i < size; i++) {
                        blackhole += vals[i];
                    }
                }
            }
        }
        if (blackhole == Long.MIN_VALUE) {
            throw new AssertionError();
        }
    }

    private static void javaGroupCount(DirectoryReader reader, IndexSearcher searcher, Query query, String keyColumn) throws IOException {
        int batch = org.opensearch.be.lucene.DocValuesAggregationExecutor.BATCH_SIZE;
        int[] docs = new int[batch];
        long[] keys = new long[batch];
        var weight = searcher.createWeight(searcher.rewrite(query), org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES, 1f);
        try (ReorganizingLongHash hash = new ReorganizingLongHash(BigArrays.NON_RECYCLING_INSTANCE)) {
            long[] counts = new long[1 << 20];
            for (var leaf : reader.leaves()) {
                var scorer = weight.scorer(leaf);
                if (scorer == null) {
                    continue;
                }
                var keyDv = leaf.reader().getNumericDocValues(keyColumn);
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
                counts = java.util.Arrays.copyOf(counts, Integer.highestOneBit(o) << 1);
            }
            counts[o]++;
        }
        return counts;
    }

    // ---------------------------------------------------------------------
    // Shared fixture: index build + engine install
    // ---------------------------------------------------------------------

    static void installEngine(NativeRuntimeHandle runtimeHandle) throws Exception {
        DataFusionService service = org.mockito.Mockito.mock(DataFusionService.class);
        org.mockito.Mockito.when(service.getNativeRuntime()).thenReturn(runtimeHandle);
        java.lang.reflect.Constructor<?> ctor = Class.forName("org.opensearch.be.datafusion.DatafusionShardAggregationEngine")
            .getDeclaredConstructor(DataFusionService.class);
        ctor.setAccessible(true);
        ShardAggregationEngineHolder.install((ShardAggregationEngine) ctor.newInstance(service));
    }

    /** Builds the shared full-column Lucene index at {@code lucene-index-full/} once; reused. */
    static void buildIndexIfMissing(Path dataDir, int rows) throws IOException {
        Path indexDir = dataDir.resolve("lucene-index-full");
        if (Files.isDirectory(indexDir) && Files.list(indexDir).findAny().isPresent()) {
            System.out.println("[setup] reusing full Lucene index at " + indexDir);
            return;
        }
        for (String col : NUM_COLS) {
            if (Files.exists(dataDir.resolve(col + ".bin")) == false) {
                throw new IllegalStateException("missing fixture " + col + ".bin");
            }
        }
        for (String col : STR_COLS) {
            if (Files.exists(dataDir.resolve(col + ".strbin")) == false) {
                throw new IllegalStateException("missing fixture " + col + ".strbin");
            }
        }
        System.out.println("[setup] building FULL Lucene index (100M docs, " + (NUM_COLS.size() + STR_COLS.size()) + " columns) — one-time");
        Files.createDirectories(indexDir);
        long t0 = System.nanoTime();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment[] num = new MemorySegment[NUM_COLS.size()];
            for (int c = 0; c < NUM_COLS.size(); c++) {
                try (FileChannel ch = FileChannel.open(dataDir.resolve(NUM_COLS.get(c) + ".bin"), StandardOpenOption.READ)) {
                    num[c] = ch.map(FileChannel.MapMode.READ_ONLY, 0, (long) rows * Long.BYTES, arena);
                }
            }
            MemorySegment[] str = new MemorySegment[STR_COLS.size()];
            long[] strPos = new long[STR_COLS.size()];
            for (int c = 0; c < STR_COLS.size(); c++) {
                try (FileChannel ch = FileChannel.open(dataDir.resolve(STR_COLS.get(c) + ".strbin"), StandardOpenOption.READ)) {
                    str[c] = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size(), arena);
                }
            }
            try (MMapDirectory dir = new MMapDirectory(indexDir)) {
                IndexWriterConfig iwc = new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec()).setRAMBufferSizeMB(2048);
                try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                    Document doc = new Document();
                    NumericDocValuesField[] numFields = new NumericDocValuesField[NUM_COLS.size()];
                    for (int c = 0; c < NUM_COLS.size(); c++) {
                        numFields[c] = new NumericDocValuesField(NUM_COLS.get(c), 0);
                        doc.add(numFields[c]);
                    }
                    // Keyword semantics = doc_values + untokenized inverted index (OpenSearch
                    // keyword mapping); term/wildcard filters need the postings.
                    SortedDocValuesField[] strFields = new SortedDocValuesField[STR_COLS.size()];
                    StringField[] strTerms = new StringField[STR_COLS.size()];
                    for (int c = 0; c < STR_COLS.size(); c++) {
                        strFields[c] = new SortedDocValuesField(STR_COLS.get(c), new BytesRef(""));
                        doc.add(strFields[c]);
                        strTerms[c] = new StringField(STR_COLS.get(c), new BytesRef(""), org.apache.lucene.document.Field.Store.NO);
                        doc.add(strTerms[c]);
                    }
                    LongPoint[] points = new LongPoint[POINT_COLS.size()];
                    int[] pointNumIdx = new int[POINT_COLS.size()];
                    for (int c = 0; c < POINT_COLS.size(); c++) {
                        points[c] = new LongPoint(POINT_COLS.get(c), 0);
                        doc.add(points[c]);
                        pointNumIdx[c] = NUM_COLS.indexOf(POINT_COLS.get(c));
                    }
                    byte[] scratch = new byte[1 << 16];
                    for (int i = 0; i < rows; i++) {
                        for (int c = 0; c < NUM_COLS.size(); c++) {
                            numFields[c].setLongValue(num[c].getAtIndex(ValueLayout.JAVA_LONG, i));
                        }
                        for (int c = 0; c < STR_COLS.size(); c++) {
                            int len = str[c].get(ValueLayout.JAVA_INT_UNALIGNED, strPos[c]);
                            strPos[c] += 4;
                            if (len > scratch.length) {
                                scratch = new byte[Integer.highestOneBit(len) << 1];
                            }
                            MemorySegment.copy(str[c], strPos[c], MemorySegment.ofArray(scratch), 0, len);
                            strPos[c] += len;
                            BytesRef term = new BytesRef(scratch, 0, len);
                            strFields[c].setBytesValue(term);
                            strTerms[c].setBytesValue(term);
                        }
                        for (int c = 0; c < POINT_COLS.size(); c++) {
                            points[c].setLongValue(num[pointNumIdx[c]].getAtIndex(ValueLayout.JAVA_LONG, i));
                        }
                        writer.addDocument(doc);
                        if (i % 10_000_000 == 9_999_999) {
                            System.out.printf(Locale.ROOT, "[setup] indexed %dM docs%n", (i + 1) / 1_000_000);
                        }
                    }
                    writer.forceMerge(1);
                }
            }
        }
        System.out.printf(Locale.ROOT, "[setup] full index built in %ds%n", TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - t0));
    }
}
