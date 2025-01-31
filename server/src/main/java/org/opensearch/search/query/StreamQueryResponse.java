/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.SearchPhaseResult;

import java.io.IOException;

public class StreamQueryResponse extends SearchPhaseResult {
    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }
}
