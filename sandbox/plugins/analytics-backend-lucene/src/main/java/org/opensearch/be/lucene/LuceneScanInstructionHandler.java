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
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.ShardAggregationEngine;
import org.opensearch.analytics.spi.ShardScanInstructionNode;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;

/**
 * Lucene-side shard-scan instruction handler. Reads a {@link ShardScanInstructionNode}
 * produced for a Lucene {@code StagePlan}, acquires the shard's Lucene reader, deserialises
 * the filter {@link QueryBuilder} from {@code ShardScanExecutionContext.getFragmentBytes()},
 * compiles it to a Lucene {@link Query}, and returns a {@link LuceneSearcherState} for
 * {@link LuceneSearchExecEngine} to execute.
 *
 * <p>Empty {@code fragmentBytes} → {@link MatchAllDocsQuery} (count(*) over the whole shard).
 *
 * @opensearch.internal
 */
final class LuceneScanInstructionHandler implements FragmentInstructionHandler<ShardScanInstructionNode> {

    private static final Logger LOGGER = LogManager.getLogger(LuceneScanInstructionHandler.class);

    private final LucenePlugin plugin;

    LuceneScanInstructionHandler(LucenePlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public BackendExecutionContext apply(
        ShardScanInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        ShardScanExecutionContext shardCtx = (ShardScanExecutionContext) commonContext;
        IndexReaderProvider.Reader reader = shardCtx.getReader();
        LuceneReader luceneReader = reader.getReader(plugin.getDataFormat(), LuceneReader.class);
        if (luceneReader == null) {
            throw new IllegalStateException("Lucene-driver fragment dispatched to a shard with no LuceneReader");
        }
        // Shared per-reader searcher (see LuceneReader#searcher).
        IndexSearcher searcher = luceneReader.searcher(shardCtx.getQueryCache(), shardCtx.getQueryCachingPolicy());
        Decoded decoded = decodeFragmentBytes(shardCtx, searcher);
        LOGGER.debug(
            "[lucene-count] shardId={} filterQuery={} columnNames={} aggSpec={}",
            shardCtx.getShardId(),
            decoded.filterQuery,
            decoded.columnNames,
            decoded.aggSpec
        );
        return new LuceneSearcherState(
            searcher,
            decoded.filterQuery,
            decoded.columnNames,
            decoded.aggSpec,
            decoded.planBytes,
            decoded.planInputColumns
        );
    }

    /**
     * Deserializes the wire format produced by {@link LuceneFragmentConvertor#convertFragment}:
     * {@code [columnNames String[]] [hasFilter boolean] [QueryBuilder NamedWriteable]?}.
     * Empty bytes → no filter, no column names (legacy/defensive fallback that shouldn't
     * happen on the Lucene-driver path but stays safe if the wire shape ever drifts).
     */
    private Decoded decodeFragmentBytes(ShardScanExecutionContext shardCtx, IndexSearcher searcher) {
        byte[] bytes = shardCtx.getFragmentBytes();
        if (bytes == null || bytes.length == 0) {
            return new Decoded(new MatchAllDocsQuery(), java.util.List.of(), null);
        }
        try (StreamInput rawInput = StreamInput.wrap(bytes)) {
            StreamInput input = new NamedWriteableAwareStreamInput(rawInput, shardCtx.getNamedWriteableRegistry());
            java.util.List<String> columnNames = input.readStringList();
            boolean hasFilter = input.readBoolean();
            Query filterQuery;
            if (hasFilter) {
                QueryShardContext qsc = LuceneAnalyticsBackendPlugin.buildMinimalQueryShardContext(shardCtx, searcher);
                QueryBuilder queryBuilder = input.readNamedWriteable(QueryBuilder.class);
                // Rewrite FieldExistsQuery → postings-only equivalent for the doc-values-less
                // lucene-secondary segment (same reason as the filter-delegation path). This covers
                // the Lucene-driver scan path (count + non-count) executed by LuceneSearchExecEngine.
                filterQuery = LuceneQueryConversionUtils.rewriteFieldExists(queryBuilder.toQuery(qsc), searcher.getIndexReader());
            } else {
                filterQuery = new MatchAllDocsQuery();
            }
            // Wire v3 (engine-compiled plan): [MARKER, base64(plan), nInput, cols..., nOut, outs...].
            if (columnNames.isEmpty() == false && LuceneFragmentConvertor.DV_PLAN_MARKER.equals(columnNames.get(0))) {
                int pos = 1;
                byte[] planBytes = java.util.Base64.getDecoder().decode(columnNames.get(pos++));
                int nInput = Integer.parseInt(columnNames.get(pos++));
                java.util.List<ShardAggregationEngine.InputColumn> inputColumns = new java.util.ArrayList<>(nInput);
                for (int i = 0; i < nInput; i++) {
                    String name = columnNames.get(pos++);
                    ShardAggregationEngine.ColumnKind kind = ShardAggregationEngine.ColumnKind.valueOf(columnNames.get(pos++));
                    inputColumns.add(new ShardAggregationEngine.InputColumn(name, kind));
                }
                int nOut = Integer.parseInt(columnNames.get(pos++));
                java.util.List<String> outputNames = new java.util.ArrayList<>(nOut);
                for (int i = 0; i < nOut; i++) {
                    outputNames.add(columnNames.get(pos++));
                }
                return new Decoded(filterQuery, outputNames, null, planBytes, inputColumns);
            }
            // Wire v2 (doc_values group-by): the spec rides the columnNames collection behind
            // a marker entry — see LuceneFragmentConvertor.convertDocValuesAggFragment.
            if (columnNames.isEmpty() == false && LuceneFragmentConvertor.DV_AGG_MARKER.equals(columnNames.get(0))) {
                ShardAggregationEngine.AggSpec spec = decodeAggSpec(columnNames);
                java.util.List<String> outputNames = new java.util.ArrayList<>(spec.groupColumns());
                for (ShardAggregationEngine.AggCall call : spec.aggCalls()) {
                    outputNames.add(call.outputName());
                }
                return new Decoded(filterQuery, outputNames, spec);
            }
            return new Decoded(filterQuery, columnNames, null);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to deserialize Lucene-driver fragment bytes", e);
        }
    }

    /** Decodes the wire-v2 spec strings (marker already verified at index 0). */
    private static ShardAggregationEngine.AggSpec decodeAggSpec(java.util.List<String> encoded) {
        int pos = 1;
        int nInput = Integer.parseInt(encoded.get(pos++));
        java.util.List<String> inputColumns = new java.util.ArrayList<>(nInput);
        for (int i = 0; i < nInput; i++) {
            inputColumns.add(encoded.get(pos++));
        }
        int nGroup = Integer.parseInt(encoded.get(pos++));
        java.util.List<String> groupColumns = new java.util.ArrayList<>(nGroup);
        for (int i = 0; i < nGroup; i++) {
            groupColumns.add(encoded.get(pos++));
        }
        int nAgg = Integer.parseInt(encoded.get(pos++));
        java.util.List<ShardAggregationEngine.AggCall> aggCalls = new java.util.ArrayList<>(nAgg);
        for (int i = 0; i < nAgg; i++) {
            String fn = encoded.get(pos++);
            String col = encoded.get(pos++);
            String out = encoded.get(pos++);
            aggCalls.add(
                new ShardAggregationEngine.AggCall(ShardAggregationEngine.AggFunction.valueOf(fn), col.isEmpty() ? null : col, out)
            );
        }
        return new ShardAggregationEngine.AggSpec(inputColumns, groupColumns, aggCalls);
    }

    private record Decoded(Query filterQuery, java.util.List<String> columnNames, ShardAggregationEngine.AggSpec aggSpec, byte[] planBytes,
        java.util.List<ShardAggregationEngine.InputColumn> planInputColumns) {
        Decoded(Query filterQuery, java.util.List<String> columnNames, ShardAggregationEngine.AggSpec aggSpec) {
            this(filterQuery, columnNames, aggSpec, null, null);
        }
    }
}
