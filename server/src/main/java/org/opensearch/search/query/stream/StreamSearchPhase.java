/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.stream;

import org.apache.arrow.flight.Ticket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.opensearch.search.SearchContextSourcePrinter;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.query.ArrowCollector;
import org.opensearch.search.query.EarlyTerminatingCollector;
import org.opensearch.search.query.QueryCollectorContext;
import org.opensearch.search.query.QueryPhase;
import org.opensearch.search.query.QueryPhaseExecutionException;
import org.opensearch.search.query.QueryPhaseSearcher;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.stream.OSTicket;
import org.opensearch.search.stream.StreamSearchResult;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

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
        executeInternal(searchContext, this.getQueryPhaseSearcher());
        if (searchContext.getProfilers() != null) {
            ProfileShardResult shardResults = SearchProfileShardResults.buildShardResults(
                searchContext.getProfilers(),
                searchContext.request()
            );
            searchContext.queryResult().profileResults(shardResults);
        }
    }


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

        @Override
        public AggregationProcessor aggregationProcessor(SearchContext searchContext) {
            return new AggregationProcessor() {
                @Override
                public void preProcess(SearchContext context) {

                }

                @Override
                public void postProcess(SearchContext context) {

                }
            };
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
            final ArrowCollector collector = createQueryCollector(collectors);
            QuerySearchResult queryResult = searchContext.queryResult();
            StreamResultFlightProducer.CollectorCallback collectorCallback = new StreamResultFlightProducer.CollectorCallback() {
                @Override
                public void collect(Collector queryCollector) throws IOException {
                    try {
                        searcher.search(query, queryCollector);
                    } catch (EarlyTerminatingCollector.EarlyTerminationException e) {
                        // EarlyTerminationException is not caught in ContextIndexSearcher to allow force termination of collection. Postcollection
                        // still needs to be processed for Aggregations when early termination takes place.
                        searchContext.bucketCollectorProcessor().processPostCollection(queryCollector);
                        queryResult.terminatedEarly(true);
                    }
                    if (searchContext.isSearchTimedOut()) {
                        assert timeoutSet : "TimeExceededException thrown even though timeout wasn't set";
                        if (searchContext.request().allowPartialSearchResults() == false) {
                            throw new QueryPhaseExecutionException(searchContext.shardTarget(), "Time exceeded");
                        }
                        queryResult.searchTimedOut(true);
                    }
                    if (searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER && queryResult.terminatedEarly() == null) {
                        queryResult.terminatedEarly(false);
                    }
                    for (QueryCollectorContext ctx : collectors) {
                        ctx.postProcess(queryResult);
                    }
                }
            };
            Ticket ticket = searchContext.flightService().getFlightProducer().createStream(collector, collectorCallback);
            StreamSearchResult streamSearchResult = searchContext.streamSearchResult();
            streamSearchResult.flights(List.of(new OSTicket(ticket.getBytes())));
            return false;
        }

        public static ArrowCollector createQueryCollector(List<QueryCollectorContext> collectors) throws IOException {
            Collector collector = QueryCollectorContext.createQueryCollector(collectors);
            return new ArrowCollector(collector, null, 1000);
        }
    }
}
