/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;

@ExperimentalApi
public class StreamTargetResponse implements Writeable {

    private final QuerySearchResult querySearchResult;
    private final SearchShardTarget searchShardTarget;

    public StreamTargetResponse(QuerySearchResult querySearchResult, SearchShardTarget searchShardTarget) {
        this.querySearchResult = querySearchResult;
        this.searchShardTarget = searchShardTarget;
    }

    public StreamTargetResponse(StreamInput in) throws IOException {
        this.querySearchResult = new QuerySearchResult(in);
        this.searchShardTarget = new SearchShardTarget(in);
    }

    public QuerySearchResult getQuerySearchResult() {
        return querySearchResult;
    }

    public SearchShardTarget getSearchShardTarget() {
        return searchShardTarget;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        querySearchResult.writeTo(out);
        searchShardTarget.writeTo(out);
    }

    @Override
    public String toString() {
        return "StreamTargetResponse{" + "querySearchResult=" + querySearchResult + ", searchShardTarget=" + searchShardTarget + '}';
    }
}
