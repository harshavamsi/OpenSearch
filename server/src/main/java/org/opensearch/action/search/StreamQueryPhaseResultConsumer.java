/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.InternalMultiTerms;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.search.aggregations.bucket.terms.StreamingMultiTermsReducer;
import org.opensearch.search.aggregations.bucket.terms.StreamingTermsReducer;
import org.opensearch.search.query.QuerySearchResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Streaming query phase result consumer.
 *
 * <p>Routes top-level {@link InternalTerms} aggregations through a {@link StreamingTermsReducer}
 * so coordinator memory stays bounded by {@code topN_size} instead of growing with
 * {@code unique_terms_across_shards}. Non-terms aggregations (top-docs, metric aggs at the top
 * level, etc.) keep the existing {@link InternalAggregations#topLevelReduce} path.
 *
 * @opensearch.internal
 */
public class StreamQueryPhaseResultConsumer extends QueryPhaseResultConsumer {

    private static final Logger logger = LogManager.getLogger(StreamQueryPhaseResultConsumer.class);

    /**
     * Per-agg-name reducers. Created lazily on first sighting of an eligible terms agg. Access
     * is serialized via the same lock as {@code partialReduce} (reduce tasks run one at a time).
     */
    private final Map<String, StreamingTermsReducer<?, ?>> termsReducers = new HashMap<>();

    /** Per-agg-name reducers for InternalMultiTerms. Disjoint from {@code termsReducers}. */
    private final Map<String, StreamingMultiTermsReducer> multiTermsReducers = new HashMap<>();

    /** Cap on how large per-agg topN can be before we bail on streaming reduce. */
    private final int maxStreamingTopN;

    public StreamQueryPhaseResultConsumer(
        SearchRequest request,
        Executor executor,
        CircuitBreaker circuitBreaker,
        SearchPhaseController controller,
        SearchProgressListener progressListener,
        NamedWriteableRegistry namedWriteableRegistry,
        int expectedResultSize,
        Consumer<Exception> onPartialMergeFailure
    ) {
        super(
            request,
            executor,
            circuitBreaker,
            controller,
            progressListener,
            namedWriteableRegistry,
            expectedResultSize,
            onPartialMergeFailure
        );
        // Conservative default: 100K survivors per agg, matching the planner threshold. Callers
        // that explicitly set aggs `size` higher than this trigger a fallback to the default
        // topLevelReduce path rather than silently capping.
        this.maxStreamingTopN = 100_000;
    }

    /**
     * For stream search, the minBatchReduceSize is set higher than shard number
     *
     * @param minBatchReduceSize: pass as number of shard
     */
    @Override
    int getBatchReduceSize(int requestBatchedReduceSize, int minBatchReduceSize) {
        return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 10);
    }

    /**
     * Release per-agg reducer state when the consumer is closed. Each reducer holds a survivor
     * map (topN buckets + pending sub-agg lists + HLL sketches); without explicit release, they
     * live until the consumer object becomes GC-reachable-free, which is delayed when the search
     * task is still registered. On Omnissa-shaped high-cardinality queries this was the
     * dominant source of retained heap between queries.
     */
    @Override
    public void close() {
        try {
            for (StreamingTermsReducer<?, ?> r : termsReducers.values()) {
                r.release();
            }
            termsReducers.clear();
            for (StreamingMultiTermsReducer r : multiTermsReducers.values()) {
                r.release();
            }
            multiTermsReducers.clear();
        } finally {
            super.close();
        }
    }

    void consumeStreamResult(SearchPhaseResult result, Runnable next) {
        // For streaming, we skip the ArraySearchPhaseResults.consumeResult() call
        // since it doesn't support multiple results from the same shard.
        QuerySearchResult querySearchResult = result.queryResult();
        pendingReduces.consume(querySearchResult, next);
    }

    /**
     * Streaming-aware fold of partial aggregations.
     *
     * <p>For each agg name in the incoming batch:
     * <ul>
     *   <li>If eligible (top-level {@link InternalTerms} matching the streaming scope and
     *       under {@link #maxStreamingTopN}), fold each entry into the per-name
     *       {@link StreamingTermsReducer}, then emit the reducer's current finalized state.</li>
     *   <li>Otherwise, fall back to {@link InternalAggregations#topLevelReduce}.</li>
     * </ul>
     */
    @Override
    protected InternalAggregations reduceAggsList(List<InternalAggregations> aggsList, InternalAggregation.ReduceContext ctx) {
        if (aggsList.isEmpty()) {
            return super.reduceAggsList(aggsList, ctx);
        }

        // Group incoming aggs by name, matching the shape InternalAggregations.reduce expects.
        Map<String, List<InternalAggregation>> byName = new HashMap<>();
        for (InternalAggregations aggs : aggsList) {
            for (Aggregation agg : aggs.asList()) {
                byName.computeIfAbsent(agg.getName(), k -> new ArrayList<>(aggsList.size())).add((InternalAggregation) agg);
            }
        }

        List<InternalAggregation> out = new ArrayList<>(byName.size());
        List<String> fallbackNames = null;
        for (Map.Entry<String, List<InternalAggregation>> entry : byName.entrySet()) {
            String name = entry.getKey();
            List<InternalAggregation> perName = entry.getValue();
            InternalAggregation first = perName.get(0);
            if (first instanceof InternalMultiTerms multiSample && isMultiTermsStreamingEligible(multiSample)) {
                InternalAggregation folded = foldMultiTerms(name, multiSample, perName, ctx);
                if (folded != null) {
                    out.add(folded);
                    continue;
                }
                if (fallbackNames == null) {
                    fallbackNames = new ArrayList<>();
                }
                fallbackNames.add(name);
            } else if (first instanceof InternalTerms<?, ?> termsSample && isStreamingEligible(termsSample)) {
                InternalAggregation folded = foldTerms(name, termsSample, perName, ctx);
                if (folded != null) {
                    out.add(folded);
                    continue;
                }
                // Reducer rejected this agg (e.g. topN too large); fall back below.
                if (fallbackNames == null) {
                    fallbackNames = new ArrayList<>();
                }
                fallbackNames.add(name);
            } else {
                // Non-streaming agg: defer to the default path on an isolated sub-list so we don't
                // double-reduce terms aggs that we've already folded above. reduce() handles the
                // single-agg case internally (matching InternalAggregations.reduce semantics).
                out.add(first.reduce(perName, ctx));
            }
        }

        if (fallbackNames != null && fallbackNames.isEmpty() == false) {
            // Streaming refused for some aggs — redo those via topLevelReduce and merge the
            // result into our output. We rebuild the agg subset of the original lists so the
            // fallback path sees exactly what it expects.
            List<InternalAggregations> subList = new ArrayList<>(aggsList.size());
            for (InternalAggregations aggs : aggsList) {
                List<InternalAggregation> kept = new ArrayList<>();
                for (Aggregation a : aggs.asList()) {
                    if (fallbackNames.contains(a.getName())) {
                        kept.add((InternalAggregation) a);
                    }
                }
                if (kept.isEmpty() == false) {
                    subList.add(InternalAggregations.from(kept));
                }
            }
            InternalAggregations fallback = super.reduceAggsList(subList, ctx);
            if (fallback != null) {
                for (Aggregation a : fallback.asList()) {
                    out.add((InternalAggregation) a);
                }
            }
        }

        return InternalAggregations.from(out);
    }

    /**
     * Fold a list of per-agg-name entries into the persistent reducer for this name. Emits the
     * reducer's current finalized state as a single merged {@link InternalAggregation}.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private InternalAggregation foldTerms(
        String name,
        InternalTerms<?, ?> sample,
        List<InternalAggregation> entries,
        InternalAggregation.ReduceContext ctx
    ) {
        int requiredSize = sample.getRequiredSize();
        if (requiredSize <= 0 || requiredSize > maxStreamingTopN) {
            logger.debug("streaming reducer: agg={}, reason=topN_out_of_range, requested={}, max={}", name, requiredSize, maxStreamingTopN);
            return null;
        }

        StreamingTermsReducer reducer = termsReducers.get(name);
        if (reducer == null) {
            reducer = new StreamingTermsReducer<>(requiredSize, ctx);
            termsReducers.put(name, reducer);
        }
        for (InternalAggregation agg : entries) {
            reducer.accept((InternalTerms) agg);
        }
        return reducer.finalize(ctx);
    }

    /** Streaming scope gate. v1: top-level {@link InternalTerms} with non-null buckets. */
    private boolean isStreamingEligible(InternalTerms<?, ?> terms) {
        // Additional gating (e.g. order-mode) is handled at the planner via FlushModeResolver;
        // here we accept any InternalTerms that arrives.
        return terms != null && terms.getBuckets() != null;
    }

    /** Eligibility gate for multi_terms — same shape as {@link #isStreamingEligible}. */
    private boolean isMultiTermsStreamingEligible(InternalMultiTerms multi) {
        return multi != null && multi.getBuckets() != null;
    }

    /**
     * Fold a list of per-agg-name {@link InternalMultiTerms} entries into the persistent
     * multi-terms reducer for this name.
     */
    private InternalAggregation foldMultiTerms(
        String name,
        InternalMultiTerms sample,
        List<InternalAggregation> entries,
        InternalAggregation.ReduceContext ctx
    ) {
        int requiredSize = sample.getRequiredSize();
        if (requiredSize <= 0 || requiredSize > maxStreamingTopN) {
            logger.debug(
                "streaming multiterms reducer: agg={}, reason=topN_out_of_range, requested={}, max={}",
                name,
                requiredSize,
                maxStreamingTopN
            );
            return null;
        }
        StreamingMultiTermsReducer reducer = multiTermsReducers.get(name);
        if (reducer == null) {
            reducer = new StreamingMultiTermsReducer(requiredSize, ctx);
            multiTermsReducers.put(name, reducer);
        }
        for (InternalAggregation agg : entries) {
            reducer.accept((InternalMultiTerms) agg);
        }
        return reducer.finalize(ctx);
    }
}
