/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.BackendShardPreference;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.DelegatedSubtreeConvertor;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.EngineCapability;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.FilterDelegationHandle;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.FragmentInstructionHandlerFactory;
import org.opensearch.analytics.spi.ProjectCapability;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ScanCapability;
import org.opensearch.analytics.spi.SearchExecEngineProvider;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 * Analytics SPI extension for the Lucene backend. Declares filter capabilities
 * for full-text and standard predicates, and provides {@link DelegatedPredicateSerializer}
 * implementations for serializing delegated queries into {@link QueryBuilder} bytes.
 *
 * <p>At the data node, the serialized bytes are deserialized back into a {@link QueryBuilder},
 * which uses the field name encoded within it to look up the appropriate
 * {@link org.opensearch.index.mapper.MappedFieldType} and create the Lucene query.
 *
 * @opensearch.internal
 */
public class LuceneAnalyticsBackendPlugin implements AnalyticsSearchBackendPlugin {

    private static final String LUCENE_FORMAT = LuceneDataFormat.LUCENE_FORMAT_NAME;
    private static final Set<String> LUCENE_FORMATS = Set.of(LUCENE_FORMAT);

    // Lucene's STANDARD filter capabilities must stay in lockstep with the serializers
    // registered in QuerySerializerRegistry — declaring a capability without a matching
    // DelegatedPredicateSerializer makes the marking layer pick Lucene as viable for
    // operators it can't actually translate, and the failure surfaces at convert time as
    // an IllegalStateException ("No Lucene serializer for [..]"). Today only EQUALS has
    // a serializer; range ops, NOT_EQUALS, IS_NULL, IS_NOT_NULL, IN, LIKE are deferred
    // until their serializers land.
    // TODO: have CapabilityRegistry intersect declared FilterCapability against the
    // backend's serializer keyset at startup so this list can't drift again. The TODO in
    // OpenSearchFilterRule.resolveViableBackends references the same constraint.
    private static final Set<ScalarFunction> STANDARD_OPS = Set.of(
        ScalarFunction.EQUALS,
        ScalarFunction.NOT_EQUALS,
        ScalarFunction.IS_NULL,
        ScalarFunction.IS_NOT_NULL,
        ScalarFunction.LIKE,
        ScalarFunction.GREATER_THAN,
        ScalarFunction.GREATER_THAN_OR_EQUAL,
        ScalarFunction.LESS_THAN,
        ScalarFunction.LESS_THAN_OR_EQUAL,
        ScalarFunction.SARG_PREDICATE
    );

    private static final Set<ScalarFunction> FULL_TEXT_OPS = Set.of(
        ScalarFunction.MATCH,
        ScalarFunction.MATCH_PHRASE,
        ScalarFunction.MATCH_BOOL_PREFIX,
        ScalarFunction.MATCH_PHRASE_PREFIX,
        ScalarFunction.MULTI_MATCH,
        ScalarFunction.QUERY_STRING,
        ScalarFunction.SIMPLE_QUERY_STRING,
        ScalarFunction.FUZZY,
        ScalarFunction.WILDCARD,
        ScalarFunction.REGEXP,
        ScalarFunction.WILDCARD_QUERY,
        ScalarFunction.QUERY,
        ScalarFunction.MATCHALL
    );

    // Field types Lucene's data format indexes (see LuceneFieldFactoryRegistry): text family
    // plus the numeric/date/boolean family (LongPoint/DoublePoint + doc values as of the
    // lucene-primary doc_values path).
    // TODO: derive this list from LuceneFieldFactoryRegistry instead of hardcoding.
    private static final Set<FieldType> STANDARD_TYPES = new HashSet<>();
    static {
        STANDARD_TYPES.add(FieldType.KEYWORD);
        STANDARD_TYPES.add(FieldType.TEXT);
        STANDARD_TYPES.add(FieldType.MATCH_ONLY_TEXT);
        STANDARD_TYPES.addAll(FieldType.numeric());
        STANDARD_TYPES.addAll(FieldType.date());
        STANDARD_TYPES.add(FieldType.BOOLEAN);
    }

    private static final Set<FieldType> FULL_TEXT_TYPES = new HashSet<>();
    static {
        FULL_TEXT_TYPES.addAll(FieldType.keyword());
        FULL_TEXT_TYPES.addAll(FieldType.text());
    }

    private static final Set<FieldType> KEYWORD_ONLY = Set.of(FieldType.KEYWORD);

    private static final Set<FilterCapability> FILTER_CAPS;
    static {
        Set<FilterCapability> caps = new HashSet<>();
        for (ScalarFunction op : STANDARD_OPS) {
            if (op == ScalarFunction.LIKE) {
                caps.add(new FilterCapability.Standard(op, KEYWORD_ONLY, LUCENE_FORMATS));
            } else {
                caps.add(new FilterCapability.Standard(op, STANDARD_TYPES, LUCENE_FORMATS));
            }
        }
        for (ScalarFunction op : FULL_TEXT_OPS) {
            for (FieldType type : FULL_TEXT_TYPES) {
                caps.add(new FilterCapability.FullText(op, type, LUCENE_FORMATS, Set.of()));
            }
        }
        FILTER_CAPS = caps;
    }

    /**
     * Long-typed field types the doc_values group-by path decodes (v1: LONG only — the
     * runtime emits Int64 for every column and the coordinator's schema stub must match;
     * see {@code LuceneFragmentConvertor.DV_LONG_TYPES}).
     */
    private static final Set<FieldType> DV_AGG_TYPES = new HashSet<>();
    static {
        // Every integer-family type indexes as a long doc_values column (LuceneFieldFactoryRegistry
        // LONG_FACTORY); the dv scan decodes them all through the same Int64 path.
        DV_AGG_TYPES.add(FieldType.LONG);
        DV_AGG_TYPES.add(FieldType.INTEGER);
        DV_AGG_TYPES.add(FieldType.SHORT);
        DV_AGG_TYPES.add(FieldType.BYTE);
        DV_AGG_TYPES.add(FieldType.UNSIGNED_LONG);
        DV_AGG_TYPES.add(FieldType.DATE);
        DV_AGG_TYPES.add(FieldType.DATE_NANOS);
        DV_AGG_TYPES.add(FieldType.BOOLEAN);
    }

    /** Group-key types the engine-plan (wire v3) path accepts: long-family numerics + KEYWORD terms. */
    private static final Set<FieldType> DV_KEY_TYPES = new HashSet<>();
    static {
        DV_KEY_TYPES.addAll(DV_AGG_TYPES);
        DV_KEY_TYPES.add(FieldType.KEYWORD);
    }

    /**
     * Scalar expressions the engine-plan (wire v3) path evaluates inside the compiled shard
     * fragment (the whole Aggregate[->Project] subtree is handed to the shard engine): integer
     * arithmetic over long-family dv columns, and CHAR_LENGTH over keyword terms. Declared so
     * the Project planning rule keeps Lucene viable for expression group-bys (q19/q30/q36/q43
     * shapes); execution correctness is the engine's (DataFusion via compileFragment).
     */
    private static final Set<ProjectCapability> PROJECT_CAPS;
    static {
        Set<ProjectCapability> caps = new HashSet<>();
        // Capability lookups key on the call's RETURN type, so the arithmetic set must
        // include DOUBLE/FLOAT: avg() plans as Project[sum/count] whose DIVIDE returns double.
        Set<FieldType> arithTypes = new HashSet<>(DV_AGG_TYPES);
        arithTypes.add(FieldType.DOUBLE);
        arithTypes.add(FieldType.FLOAT);
        // CAST in date-range predicates returns DATE (PPL compiles timestamp literals as
        // CAST(varchar AS timestamp)); the filter rule intersects nested-scalar viability,
        // so lucene must claim CAST over date types or every date-range filter dies.
        arithTypes.addAll(FieldType.date());
        arithTypes.add(FieldType.KEYWORD);
        for (ScalarFunction op : List.of(
            ScalarFunction.PLUS,
            ScalarFunction.MINUS,
            ScalarFunction.TIMES,
            ScalarFunction.DIVIDE,
            ScalarFunction.MOD,
            ScalarFunction.CAST,
            // PPL compiles date/timestamp literals in predicates as TIMESTAMP('...') /
            // DATE('...') constructor calls; the filter rule requires every nested scalar
            // to be viable on a candidate backend. These fold to constants before the
            // Lucene range query is built, so claiming them costs nothing at runtime.
            ScalarFunction.TIMESTAMP,
            ScalarFunction.DATE,
            // Engine-evaluated project expressions (the compiled dv-plan fragment hands the
            // whole Aggregate[->Project] subtree to DataFusion, which executes these natively —
            // same conversion path the parquet-primary backend uses): date extraction/formatting,
            // regex rewrite, and CASE with its nested boolean/comparison operands.
            ScalarFunction.EXTRACT,
            ScalarFunction.DATE_FORMAT,
            ScalarFunction.REGEXP_REPLACE,
            ScalarFunction.CASE,
            ScalarFunction.AND,
            ScalarFunction.OR,
            ScalarFunction.NOT,
            ScalarFunction.EQUALS,
            ScalarFunction.NOT_EQUALS,
            ScalarFunction.GREATER_THAN,
            ScalarFunction.GREATER_THAN_OR_EQUAL,
            ScalarFunction.LESS_THAN,
            ScalarFunction.LESS_THAN_OR_EQUAL
        )) {
            caps.add(new ProjectCapability.Scalar(op, arithTypes, LUCENE_FORMATS, true));
        }
        caps.add(new ProjectCapability.Scalar(ScalarFunction.CHAR_LENGTH, DV_KEY_TYPES, LUCENE_FORMATS, true));
        PROJECT_CAPS = Set.copyOf(caps);
    }

    /**
     * Lucene-secondary indexes the term dictionary (inverted index) for the same field
     * types it accepts filters on — keyword / text / match_only_text. The Index
     * scan capability lets the planner mark Lucene viable as a driver for metadata-only
     * operations (count today, group-by-count and top-K terms in future) over scans whose
     * fields are listed here. It does NOT imply Lucene can deliver row values; consumers
     * needing values (Project, Sort) consult value-producing scan capabilities separately
     * and self-restrict, which the chain-agreement filter at PlanForker enforces — the
     * DocValues capability below is the value-producing declaration for the group-by path.
     */
    private static final Set<ScanCapability> SCAN_CAPS = Set.of(
        new ScanCapability.Index(LUCENE_FORMATS, STANDARD_TYPES),
        // Value-producing scan over long-typed doc_values columns — the group-by path
        // decodes them into Arrow batches, so the planner may treat Lucene as able to
        // deliver row values for these types (unlike the metadata-only Index capability).
        new ScanCapability.DocValues(LUCENE_FORMATS, DV_KEY_TYPES)
    );

    /**
     * Lucene drives count(*) over fields it indexes (metadata fast path), and grouped
     * COUNT/SUM/MIN/MAX over long-typed doc_values columns (decoded into Arrow and
     * aggregated by the shard-local engine via {@code DocValuesAggregationExecutor}).
     * Coupled with the scan capabilities above, this lets PlanForker emit a Lucene-driver
     * StagePlan alternative for those fragments without bypassing the existing engine path.
     */
    private static final Set<AggregateCapability> AGGREGATE_CAPS;
    static {
        Set<AggregateCapability> caps = new HashSet<>();
        caps.add(AggregateCapability.simple(AggregateFunction.COUNT, STANDARD_TYPES, LUCENE_FORMATS));
        for (AggregateFunction fn : List.of(
            AggregateFunction.COUNT,
            AggregateFunction.SUM,
            AggregateFunction.SUM0,
            AggregateFunction.MIN,
            AggregateFunction.MAX,
            AggregateFunction.AVG
        )) {
            caps.add(AggregateCapability.simple(fn, DV_AGG_TYPES, LUCENE_FORMATS));
        }
        // Keyword group keys ride the engine-plan path; COUNT is the aggregate PlanForker
        // checks against the KEY type for terms-shaped fragments.
        caps.add(AggregateCapability.simple(AggregateFunction.COUNT, DV_KEY_TYPES, LUCENE_FORMATS));
        // MIN/MAX over keyword ride the same engine-plan path — DataFusion's min/max(utf8)
        // executes them inside the compiled shard fragment (q22/q23/q29 shapes).
        caps.add(AggregateCapability.simple(AggregateFunction.MIN, DV_KEY_TYPES, LUCENE_FORMATS));
        caps.add(AggregateCapability.simple(AggregateFunction.MAX, DV_KEY_TYPES, LUCENE_FORMATS));
        AGGREGATE_CAPS = Set.copyOf(caps);
    }

    private final LucenePlugin plugin;

    public LuceneAnalyticsBackendPlugin(LucenePlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public String name() {
        return LuceneDataFormat.LUCENE_FORMAT_NAME;
    }

    @Override
    public BackendCapabilityProvider getCapabilityProvider() {
        return new BackendCapabilityProvider() {
            @Override
            public Set<EngineCapability> supportedEngineCapabilities() {
                return Set.of();
            }

            @Override
            public Set<FilterCapability> filterCapabilities() {
                return FILTER_CAPS;
            }

            @Override
            public Set<ScanCapability> scanCapabilities() {
                return SCAN_CAPS;
            }

            @Override
            public Set<AggregateCapability> aggregateCapabilities() {
                return AGGREGATE_CAPS;
            }

            @Override
            public Set<ProjectCapability> projectCapabilities() {
                return PROJECT_CAPS;
            }

            @Override
            public Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                return QuerySerializerRegistry.getSerializers();
            }

            @Override
            public BackendShardPreference shardPreference() {
                return SHARD_PREFERENCE;
            }
        };
    }

    private static final BackendShardPreference SHARD_PREFERENCE = new LuceneShardPreference();

    private static final Logger LOGGER = LogManager.getLogger(LuceneAnalyticsBackendPlugin.class);

    @Override
    public FilterDelegationHandle getFilterDelegationHandle(List<DelegatedExpression> expressions, CommonExecutionContext ctx) {
        ShardScanExecutionContext shardCtx = (ShardScanExecutionContext) ctx;
        IndexReaderProvider.Reader reader = shardCtx.getReader();
        LuceneReader luceneReader = reader.getReader(plugin.getDataFormat(), LuceneReader.class);
        // Shared per-reader searcher (see LuceneReader#searcher) — a fresh one here crashes the node
        // on self-union delegated scans.
        IndexSearcher searcher = luceneReader.searcher(shardCtx.getQueryCache(), shardCtx.getQueryCachingPolicy());
        QueryShardContext queryShardContext = buildMinimalQueryShardContext(shardCtx, searcher);
        BooleanSupplier isCancelled = () -> {
            Task task = shardCtx.getTask();
            return task instanceof CancellableTask ct && ct.isCancelled();
        };
        return new LuceneFilterDelegationHandle(
            expressions,
            queryShardContext,
            luceneReader,
            reader.catalogSnapshot(),
            shardCtx.getNamedWriteableRegistry(),
            isCancelled
        );
    }

    // ── Lucene-as-driver execution path (count fast path) ──

    @Override
    public FragmentConvertor getFragmentConvertor() {
        return new LuceneFragmentConvertor(QuerySerializerRegistry.getSerializers());
    }

    @Override
    public FragmentInstructionHandlerFactory getInstructionHandlerFactory() {
        return new LuceneInstructionHandlerFactory(plugin);
    }

    @Override
    public SearchExecEngineProvider getSearchExecEngineProvider() {
        return (ctx, backendContext) -> {
            if (!(backendContext instanceof LuceneSearcherState state)) {
                throw new IllegalStateException(
                    "Lucene SearchExecEngineProvider expected LuceneSearcherState but got "
                        + (backendContext == null ? "null" : backendContext.getClass().getName())
                );
            }
            LuceneSearchExecEngine engine = new LuceneSearchExecEngine(state);
            engine.prepare(ctx);
            return engine;
        };
    }

    /** Package-private — also reused by {@link LuceneScanInstructionHandler} in driver mode. */
    static QueryShardContext buildMinimalQueryShardContext(ShardScanExecutionContext ctx, IndexSearcher searcher) {
        return new QueryShardContext(
            0,
            ctx.getIndexSettings(),
            null,  // bigArrays
            null,  // bitsetFilterCache
            null,  // indexFieldDataLookup
            ctx.getMapperService(),
            null,  // similarityService
            null,  // scriptService
            null,  // xContentRegistry
            null,  // namedWriteableRegistry
            null,  // client
            searcher,
            System::currentTimeMillis,
            null,  // clusterAlias
            s -> true,  // indexNameMatcher
            () -> true,  // allowExpensiveQueries
            null   // valuesSourceRegistry
        );
    }

    // ---- Serializers ----

    @Override
    public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
        return QuerySerializerRegistry.getSerializers();
    }

    @Override
    public DelegatedSubtreeConvertor getDelegatedSubtreeConvertor() {
        return new LuceneSubtreeConvertor(QuerySerializerRegistry.getSerializers());
    }
}
