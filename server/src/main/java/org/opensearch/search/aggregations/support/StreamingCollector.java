/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.support;

import org.apache.arrow.vector.FieldVector;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.opensearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;
import java.util.List;

public abstract class StreamingCollector implements Collector {

    public abstract LeafBucketCollector getLeafCollector(LeafReaderContext context, List<FieldVector> fieldVectors) throws IOException;
}
