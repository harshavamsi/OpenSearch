/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.stream;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;
import java.util.List;

/**
 * A result of stream search execution.
 */
@ExperimentalApi
public class StreamSearchResult extends SearchPhaseResult {
    private List<OSTicket> flightTickets;
    private final QuerySearchResult queryResult;
    private final FetchSearchResult fetchResult;

    public StreamSearchResult() {
        super();
        this.queryResult = QuerySearchResult.nullInstance();
        this.fetchResult = new FetchSearchResult();
    }

    public StreamSearchResult(StreamInput in) throws IOException {
        super(in);
        contextId = new ShardSearchContextId(in);
        setShardSearchRequest(in.readOptionalWriteable(ShardSearchRequest::new));
        if (in.readOptionalBoolean()) {
            flightTickets = in.readList(OSTicket::new);
        }
        queryResult = new QuerySearchResult(contextId, getSearchShardTarget(), getShardSearchRequest());
        fetchResult = new FetchSearchResult(contextId, getSearchShardTarget());
        setSearchShardTarget(getSearchShardTarget());
    }

    public StreamSearchResult(ShardSearchContextId id, SearchShardTarget shardTarget, ShardSearchRequest searchRequest) {
        this.contextId = id;
        queryResult = new QuerySearchResult(id, shardTarget, searchRequest);
        fetchResult = new FetchSearchResult(id, shardTarget);
        setSearchShardTarget(shardTarget);
        setShardSearchRequest(searchRequest);
    }

    public void flights(List<OSTicket> flightTickets) {
        this.flightTickets = flightTickets;
    }

    @Override
    public void setSearchShardTarget(SearchShardTarget shardTarget) {
        super.setSearchShardTarget(shardTarget);
        queryResult.setSearchShardTarget(shardTarget);
    }

    @Override
    public void setShardIndex(int shardIndex) {
        super.setShardIndex(shardIndex);
        queryResult.setShardIndex(shardIndex);
    }

    @Override
    public QuerySearchResult queryResult() {
        return queryResult;
    }

    @Override
    public FetchSearchResult fetchResult() {
        return fetchResult;
    }

    public List<OSTicket> getFlightTickets() {
        return flightTickets;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        contextId.writeTo(out);
        out.writeOptionalWriteable(getShardSearchRequest());
        if (flightTickets != null) {
            out.writeOptionalBoolean(true);
            out.writeList(flightTickets);
        }
    }
}
