/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.common.lease.Releasables;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Streaming variant of {@link MultiTermsAggregator}.
 *
 * <p>Reuses the parent's collection + bucket-building machinery unchanged. The only delta:
 * {@link #doReset()} rebuilds {@link #bucketOrds} per batch so each per-segment flush emits
 * buckets for only the terms seen in that segment. The coordinator's
 * {@link StreamingMultiTermsReducer} folds identical keys across batches.
 *
 * <p>Disables sub-agg deferral — streaming flushes each segment as an independent mini-query,
 * and {@code BestBucketsDeferringCollector.prepareSelectedBuckets} is single-use.
 */
public class StreamMultiTermsAggregator extends MultiTermsAggregator {

    private final CardinalityUpperBound cardinality;

    public StreamMultiTermsAggregator(
        String name,
        AggregatorFactories factories,
        boolean showTermDocCountError,
        List<ValuesSource> rawValuesSources,
        List<InternalValuesSource> internalValuesSources,
        List<String> fields,
        List<DocValueFormat> formats,
        BucketOrder order,
        SubAggCollectionMode collectMode,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        SearchContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(
            name,
            factories,
            showTermDocCountError,
            rawValuesSources,
            internalValuesSources,
            fields,
            formats,
            order,
            collectMode,
            bucketCountThresholds,
            context,
            parent,
            cardinality,
            metadata
        );
        this.cardinality = cardinality;
    }

    @Override
    public void doReset() {
        super.doReset();
        // See StreamStringTermsAggregator.doReset for the rationale: without rebuilding between
        // batches, selectTopBuckets re-scans every bucket seen since the shard started, making
        // per-segment work O(segments × unique_keys) instead of O(unique_keys).
        Releasables.close(bucketOrds);
        bucketOrds = BytesKeyedBucketOrds.build(context.bigArrays(), cardinality);
    }

    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        return false;
    }
}
