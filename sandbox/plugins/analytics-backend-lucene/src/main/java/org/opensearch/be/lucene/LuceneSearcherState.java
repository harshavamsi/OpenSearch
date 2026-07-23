/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.ShardAggregationEngine;

import java.util.List;
import java.util.Objects;

/**
 * Lucene-side {@link BackendExecutionContext}. Built by {@link LuceneScanInstructionHandler}
 * from the wire bytes {@link LuceneFragmentConvertor} produced (filter {@code QueryBuilder} +
 * aggregate-call column names) and consumed by {@link LuceneSearchExecEngine}.
 *
 * <p>Mirrors the role {@code DataFusionSessionState} plays for the DataFusion backend —
 * a small immutable state record threaded from instruction handler to search engine.
 *
 * <p>Holds no native resources; {@link #close()} is a no-op. The {@link IndexSearcher}'s
 * underlying reader is owned by the caller-acquired {@code ReaderContext}, which closes it
 * after the engine stream drains.
 *
 * @opensearch.internal
 */
final class LuceneSearcherState implements BackendExecutionContext {

    private final IndexSearcher searcher;
    /** Never {@code null}; {@code MatchAllDocsQuery} when the fragment had no filter. */
    private final Query filterQuery;
    /** Aggregate-call output names — one Int64 column per name in the result Arrow batch. */
    private final List<String> outputColumnNames;
    /**
     * Doc_values group-by spec, or {@code null} for the count fast path. When present the
     * exec engine routes to {@link DocValuesAggregationExecutor} instead of
     * {@code IndexSearcher.count}.
     */
    private final ShardAggregationEngine.AggSpec aggSpec;
    /** Wire-v3 engine-compiled plan bytes, or {@code null}. Takes precedence over aggSpec. */
    private final byte[] planBytes;
    /** Input columns to decode+feed for the plan-bytes path (wire v3). */
    private final List<ShardAggregationEngine.InputColumn> planInputColumns;

    LuceneSearcherState(IndexSearcher searcher, Query filterQuery, List<String> outputColumnNames) {
        this(searcher, filterQuery, outputColumnNames, null);
    }

    LuceneSearcherState(
        IndexSearcher searcher,
        Query filterQuery,
        List<String> outputColumnNames,
        ShardAggregationEngine.AggSpec aggSpec,
        byte[] planBytes,
        List<ShardAggregationEngine.InputColumn> planInputColumns
    ) {
        this.searcher = Objects.requireNonNull(searcher, "searcher");
        this.filterQuery = Objects.requireNonNull(filterQuery, "filterQuery (use MatchAllDocsQuery for no-filter fragments)");
        this.outputColumnNames = List.copyOf(Objects.requireNonNull(outputColumnNames, "outputColumnNames"));
        this.aggSpec = aggSpec;
        this.planBytes = planBytes;
        this.planInputColumns = planInputColumns;
    }

    LuceneSearcherState(IndexSearcher searcher, Query filterQuery, List<String> outputColumnNames, ShardAggregationEngine.AggSpec aggSpec) {
        this(searcher, filterQuery, outputColumnNames, aggSpec, null, null);
    }

    IndexSearcher searcher() {
        return searcher;
    }

    Query filterQuery() {
        return filterQuery;
    }

    List<String> outputColumnNames() {
        return outputColumnNames;
    }

    ShardAggregationEngine.AggSpec aggSpec() {
        return aggSpec;
    }

    byte[] planBytes() {
        return planBytes;
    }

    List<ShardAggregationEngine.InputColumn> planInputColumns() {
        return planInputColumns;
    }
}
