/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.search.SearchShardTarget;

import java.io.IOException;

public class ShardStreamQueryResult implements Writeable {

    public SearchShardTarget getTarget() {
        return target;
    }

    public ShardSearchContextId getId() {
        return id;
    }

    private final SearchShardTarget target;
    private final ShardSearchContextId id;

    public ShardStreamQueryResult(SearchShardTarget searchShardTarget, ShardSearchContextId contextId) {
        this.target = searchShardTarget;
        this.id = contextId;
    }

    public ShardStreamQueryResult(StreamInput in) throws IOException {
        target = new SearchShardTarget(in);
        id = new ShardSearchContextId(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        target.writeTo(out);
        id.writeTo(out);
    }
}
