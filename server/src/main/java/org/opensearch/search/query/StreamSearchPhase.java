/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.opensearch.OpenSearchException;
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.arrow.spi.StreamTicket;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.SearchContextSourcePrinter;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.bucket.terms.GlobalOrdinalsStringTermsAggregator;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator;
import org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.opensearch.search.aggregations.support.StreamingAggregator;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.stream.OSTicket;
import org.opensearch.search.stream.StreamSearchResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus.DEFAULT_PRECISION;

/**
 * StreamSearchPhase is the search phase for streaming search.
 */
public class StreamSearchPhase extends QueryPhase {

    private static final Logger LOGGER = LogManager.getLogger(StreamSearchPhase.class);
    public static final QueryPhaseSearcher DEFAULT_QUERY_PHASE_SEARCHER = new DefaultStreamSearchPhaseSearcher();

    public StreamSearchPhase() {
        super(DEFAULT_QUERY_PHASE_SEARCHER);
    }

    @Override
    public void execute(SearchContext searchContext) throws QueryPhaseExecutionException {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{}", new SearchContextSourcePrinter(searchContext));
        }
        final AggregationProcessor aggregationProcessor = this.getQueryPhaseSearcher().aggregationProcessor(searchContext);
        aggregationProcessor.preProcess(searchContext);
        executeInternal(searchContext, this.getQueryPhaseSearcher());

        if (searchContext.getProfilers() != null) {
            ProfileShardResult shardResults = SearchProfileShardResults.buildShardResults(
                searchContext.getProfilers(),
                searchContext.request()
            );
            searchContext.queryResult().profileResults(shardResults);
        }
    }

    /**
     * DefaultStreamSearchPhaseSearcher
     */
    public static class DefaultStreamSearchPhaseSearcher extends DefaultQueryPhaseSearcher {

        @Override
        public boolean searchWith(
            SearchContext searchContext,
            ContextIndexSearcher searcher,
            Query query,
            LinkedList<QueryCollectorContext> collectors,
            boolean hasFilterCollector,
            boolean hasTimeout
        ) throws IOException {
            return searchWithCollector(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
        }

        protected boolean searchWithCollector(
            SearchContext searchContext,
            ContextIndexSearcher searcher,
            Query query,
            LinkedList<QueryCollectorContext> collectors,
            boolean hasFilterCollector,
            boolean hasTimeout
        ) throws IOException {
            return searchWithCollector(searchContext, searcher, query, collectors, hasTimeout);
        }

        private boolean searchWithCollector(
            SearchContext searchContext,
            ContextIndexSearcher searcher,
            Query query,
            LinkedList<QueryCollectorContext> collectors,
            boolean timeoutSet
        ) throws IOException {
            QuerySearchResult queryResult = searchContext.queryResult();
            StreamManager streamManager = searchContext.streamManager();
            if (streamManager == null) {
                throw new RuntimeException("StreamManager not setup");
            }
            final boolean[] isCancelled = { false };
            final Schema[] schema = { null };
            final Optional<VectorSchemaRoot>[] root = new Optional[] { Optional.empty() };
            final Collector queryCollector = QueryCollectorContext.createQueryCollector(collectors);
            StreamTicket ticket = streamManager.registerStream(new StreamProducer() {
                @Override
                public void close() {
                    isCancelled[0] = true;
                    if (root[0].isPresent()) {
                        root[0].get().close();
                    }
                }

                @Override
                public BatchedJob createJob(BufferAllocator allocator) {
                    return new BatchedJob() {
                        @Override
                        public void run(VectorSchemaRoot root, FlushSignal flushSignal) {
                            try {
                                final StreamingAggregator streamingAggregatorCollector = new StreamingAggregator(
                                    (Aggregator) queryCollector,
                                    searchContext,
                                    root,
                                    1_000_000,
                                    flushSignal,
                                    searchContext.shardTarget().getShardId(),
                                    searchContext.bigArrays(),
                                    new HyperLogLogPlusPlus.HyperLogLog(searchContext.bigArrays(), 1, DEFAULT_PRECISION)
                                );
                                try {
                                    searcher.addQueryCancellation(() -> {
                                        if (isCancelled[0] == true) {
                                            throw new OpenSearchException("Stream for query results cancelled.");
                                        }
                                    });
                                    searcher.search(query, streamingAggregatorCollector);
                                } catch (EarlyTerminatingCollector.EarlyTerminationException e) {
                                    // EarlyTerminationException is not caught in ContextIndexSearcher to allow force termination of
                                    // collection. Postcollection
                                    // still needs to be processed for Aggregations when early termination takes place.
                                    searchContext.bucketCollectorProcessor().processPostCollection(streamingAggregatorCollector);
                                    queryResult.terminatedEarly(true);
                                }
                                if (searchContext.isSearchTimedOut()) {
                                    assert timeoutSet : "TimeExceededException thrown even though timeout wasn't set";
                                    if (searchContext.request().allowPartialSearchResults() == false) {
                                        throw new QueryPhaseExecutionException(searchContext.shardTarget(), "Time exceeded");
                                    }
                                    queryResult.searchTimedOut(true);
                                }
                                if (searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER
                                    && queryResult.terminatedEarly() == null) {
                                    queryResult.terminatedEarly(false);
                                }

                                for (QueryCollectorContext ctx : collectors) {
                                    ctx.postProcess(queryResult);
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        @Override
                        public void onCancel() {
                            isCancelled[0] = true;
                        }

                        @Override
                        public boolean isCancelled() {
                            return searchContext.isCancelled() || isCancelled[0];
                        }
                    };
                }

                @Override
                public TimeValue getJobDeadline() {
                    return TimeValue.timeValueMinutes(5);
                }

                @Override
                public VectorSchemaRoot createRoot(BufferAllocator allocator) {
                    List<Field> fields = new ArrayList<>();
                    if (queryCollector instanceof GlobalOrdinalsStringTermsAggregator) {
                        Field countField = new Field("count", FieldType.nullable(new ArrowType.Int(64, false)), null);
                        List<Field> bucketOrdFields = new ArrayList<>();
                        bucketOrdFields.add(new Field("bucketOrd", FieldType.nullable(new ArrowType.Utf8()), null));

                        // Sub-fields for cardinality
                        Aggregator[] subAggregators = ((GlobalOrdinalsStringTermsAggregator) queryCollector).subAggregators();
                        for (Aggregator subAggregator : subAggregators) {
                            if (subAggregator instanceof CardinalityAggregator) {
                                bucketOrdFields.add(new Field("subCount", FieldType.nullable(new ArrowType.Binary()), null));
                            }
                        }

                        Field bucketOrdField = new Field("bucketOrd", FieldType.nullable(new ArrowType.Struct()), bucketOrdFields);

                        fields.add(countField);
                        fields.add(bucketOrdField);
                    }

                    return VectorSchemaRoot.create(new Schema(fields), allocator);

                }

                @Override
                public int estimatedRowCount() {
                    return searcher.getIndexReader().numDocs();
                }

                @Override
                public String getAction() {
                    return searchContext.getTask().getAction();
                }
            }, searchContext.getTask().getParentTaskId());
            StreamSearchResult streamSearchResult = searchContext.streamSearchResult();
            streamSearchResult.flights(List.of(new OSTicket(ticket.toBytes())));
            return false;
        }
    }
}
